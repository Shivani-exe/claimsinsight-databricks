# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · MVP2: new batches, no duplicates, rerun-safe (v2)
# MAGIC
# MAGIC **Problem:** new claim files keep arriving. Some rows are repeats, some are corrections, some are broken.
# MAGIC If we just `append`, repeats are double-counted. If we `overwrite`, history is wiped.
# MAGIC
# MAGIC **Solution:** for each batch
# MAGIC 1. **Guard:** if the audit table says this batch already loaded successfully, skip it.
# MAGIC 2. **Bronze:** store the batch exactly as received (+ batch_id).
# MAGIC 3. **Validate:** check every row and give each failure a **reason**. Failed rows → **quarantine**.
# MAGIC 4. **MERGE** valid rows into `silver_claims_current_mvp2` on **CLM_ID + SEGMENT**:
# MAGIC    new key → insert · same key + changed facts → update · same key + same facts → nothing.
# MAGIC 5. **Audit:** log what happened (read, quarantined, inserted, updated, status).
# MAGIC
# MAGIC **v2 adds:** the skip guard, validation reasons with `try_cast`, and batch-level replace for Bronze and
# MAGIC quarantine so a forced rerun never duplicates the logs.
# MAGIC
# MAGIC **Two layers of safety (defense in depth):**
# MAGIC - Layer 1, the guard: don't reprocess a batch that already succeeded.
# MAGIC - Layer 2, the MERGE: even if forced to reprocess, the current table doesn't change (idempotent).
# MAGIC
# MAGIC ⚠️ The CMS files are historical, not a live feed. We **simulate** batches by splitting the data by year
# MAGIC and adding planned repeats, corrections and bad rows. This is documented, not hidden.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CURRENT = t("silver_claims_current_mvp2")
BRONZE_EVENTS = t("bronze_claim_events_mvp2")
QUARANTINE = t("quarantine_claim_events_mvp2")
AUDIT = t("audit_batches_mvp2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Start the current table from the 2008 data (the "day zero" state)

# COMMAND ----------

base = (
    spark.table(t("silver_inpatient_segments"))
    .filter(F.year("CLM_FROM_DT") == 2008)
    .withColumn("batch_id", F.lit("base_2008"))
)
base.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(CURRENT)

# Fresh start for the MVP2 log tables (so this notebook can be re-run from the top)
for tbl in [BRONZE_EVENTS, QUARANTINE, AUDIT]:
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")

print("Day-zero rows in current table:", spark.table(CURRENT).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Simulate two incoming batches (written as CSV files to the landing volume)
# MAGIC | Batch | Contains | Expected result |
# MAGIC |---|---|---|
# MAGIC | `batch_001` | all 2009 rows | all inserted |
# MAGIC | `batch_002` | all 2010 rows + 100 unchanged repeats + 10 corrected rows + 5 rows with no CLM_ID + 3 rows with an unreadable amount | 2010 inserted, 10 updated, 100 ignored, 8 quarantined |

# COMMAND ----------

raw = spark.table(t("bronze_inpatient")).drop("ingested_at", "source_file")
raw_2009 = raw.filter(F.substring("CLM_FROM_DT", 1, 4) == "2009")
raw_2010 = raw.filter(F.substring("CLM_FROM_DT", 1, 4) == "2010")

repeats = raw_2009.orderBy(F.desc("CLM_ID")).limit(100)                     # same facts, sent again
corrected = (raw_2009.orderBy("CLM_ID").limit(10)                           # same key, new amount
             .withColumn("CLM_PMT_AMT",
                         (F.col("CLM_PMT_AMT").cast("decimal(12,2)") + 100).cast("string")))
missing_id = raw_2010.orderBy("CLM_ID").limit(5).withColumn("CLM_ID", F.lit(None).cast("string"))
bad_amount = raw_2010.orderBy(F.desc("CLM_ID")).limit(3).withColumn("CLM_PMT_AMT", F.lit("NOT_A_NUMBER"))

batches = {
    "batch_001": raw_2009,
    "batch_002": (raw_2010.unionByName(repeats).unionByName(corrected)
                  .unionByName(missing_id).unionByName(bad_amount)),
}

for batch_id, df in batches.items():
    path = f"{LANDING_VOLUME}/{batch_id}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path)
    print(f"{batch_id}: {df.count():,} rows written to {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Validation rules (each failure gets a reason)
# MAGIC `try_cast` = "try to convert; if it can't, give NULL instead of crashing".
# MAGIC *Analogy: a cashier who sets aside a torn banknote instead of shutting the whole till.*
# MAGIC
# MAGIC `F.when(condition, reason)` gives the reason only when the condition is true (otherwise NULL).
# MAGIC `concat_ws("; ", ...)` joins the non-NULL reasons. Empty text = the row passed every rule.

# COMMAND ----------

def add_rejection_reason(raw_df):
    seg = F.expr("try_cast(SEGMENT AS INT)")
    from_dt = F.expr("try_to_timestamp(CLM_FROM_DT, 'yyyyMMdd')")
    amount_text = F.trim(F.col("CLM_PMT_AMT"))
    amount = F.expr("try_cast(CLM_PMT_AMT AS DECIMAL(12,2))")

    return raw_df.withColumn("rejection_reason", F.concat_ws("; ",
        F.when(F.col("CLM_ID").isNull() | (F.trim("CLM_ID") == ""), F.lit("MISSING_CLAIM_ID")),
        F.when(seg.isNull() | ~seg.isin(1, 2), F.lit("INVALID_SEGMENT")),
        F.when(from_dt.isNull(), F.lit("INVALID_FROM_DATE")),
        F.when(amount_text.isNull() | (amount_text == ""), F.lit("MISSING_PAYMENT")),
        F.when((amount_text != "") & amount.isNull(), F.lit("INVALID_PAYMENT")),
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · The batch processor (the heart of MVP2)

# COMMAND ----------

def already_loaded(batch_id):
    """Layer 1: has this batch already been processed successfully?"""
    if not spark.catalog.tableExists(AUDIT):
        return False
    done = spark.table(AUDIT).filter((F.col("batch_id") == batch_id) & (F.col("status") == "SUCCESS"))
    return done.count() > 0


def remove_batch_rows(table, batch_id):
    """Batch-level replace: delete this batch's old rows before writing them again."""
    if spark.catalog.tableExists(table):
        spark.sql(f"DELETE FROM {table} WHERE batch_id = '{batch_id}'")


def write_audit(batch_id, status, read=0, quarantined=0, inserted=0, updated=0):
    row = spark.createDataFrame(
        [(batch_id, status, read, quarantined, inserted, updated)],
        "batch_id string, status string, rows_read long, rows_quarantined long, "
        "rows_inserted long, rows_updated long",
    ).withColumn("run_at", F.current_timestamp())
    row.write.format("delta").mode("append").saveAsTable(AUDIT)


def process_batch(batch_id, force=False):
    # 0) GUARD: skip batches that already succeeded (unless we deliberately force a rerun)
    if already_loaded(batch_id) and not force:
        write_audit(batch_id, "SKIPPED_ALREADY_LOADED")
        print(f"{batch_id}: already loaded → skipped")
        return

    path = f"{LANDING_VOLUME}/{batch_id}"

    # 1) BRONZE: read exactly as received, stamp it, replace this batch's rows (never other batches)
    raw_batch = (
        spark.read.option("header", "true").option("inferSchema", "false").csv(path)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit(path))
        .withColumn("batch_id", F.lit(batch_id))
    )
    remove_batch_rows(BRONZE_EVENTS, batch_id)
    raw_batch.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(BRONZE_EVENTS)
    rows_read = raw_batch.count()

    # 2) VALIDATE: every row gets a reason ("" = passed)
    checked = add_rejection_reason(raw_batch)

    bad = checked.filter(F.col("rejection_reason") != "").withColumn("quarantined_at", F.current_timestamp())
    remove_batch_rows(QUARANTINE, batch_id)
    bad.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(QUARANTINE)
    rows_quarantined = bad.count()

    # 3) CLEAN the good rows with the SAME function as Silver, then dedupe inside the batch
    good = checked.filter(F.col("rejection_reason") == "").drop("rejection_reason")
    typed = to_silver_segments(good).withColumn("batch_id", F.lit(batch_id)).dropDuplicates(KEY_COLS)
    typed.createOrReplaceTempView("batch_src")

    # 4) MERGE on the key. Update only when the facts really changed (row_hash differs).
    result = spark.sql(f"""
        MERGE INTO {CURRENT} AS tgt
        USING batch_src AS src
          ON  tgt.CLM_ID  = src.CLM_ID
          AND tgt.SEGMENT = src.SEGMENT
        WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).collect()[0]
    inserted = int(result["num_inserted_rows"])
    updated = int(result["num_updated_rows"])

    # 5) AUDIT
    write_audit(batch_id, "SUCCESS", rows_read, rows_quarantined, inserted, updated)
    print(f"{batch_id}: read={rows_read:,} quarantined={rows_quarantined} "
          f"inserted={inserted:,} updated={updated:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Run the batches. PREDICT each line before you run it.

# COMMAND ----------

process_batch("batch_001")   # predict: inserted = 2009 rows, updated 0, quarantined 0

# COMMAND ----------

process_batch("batch_002")   # predict: inserted 13,572, updated 10, quarantined 8 (5 missing ID + 3 bad amount)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 · THE PROOF: two kinds of rerun

# COMMAND ----------

process_batch("batch_002")              # Layer 1 → predict: "already loaded → skipped"

# COMMAND ----------

process_batch("batch_002", force=True)  # Layer 2 → predict: inserted 0, updated 0, quarantined 8 (not 16!)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 · Final checks + evidence for the call

# COMMAND ----------

current = spark.table(CURRENT)
bronze_events = spark.table(BRONZE_EVENTS)
quarantine = spark.table(QUARANTINE)

print("Current table rows :", current.count())
print("Key duplicates     :", count_key_duplicates(current, KEY_COLS))     # must be 0
print("Bronze event rows  :", bronze_events.count())                       # batch_001 + batch_002, once each
print("Quarantined rows   :", quarantine.count())                          # 8, even after the forced rerun

assert count_key_duplicates(current, KEY_COLS) == 0
assert quarantine.count() == 8, "Quarantine should hold each bad row once"

display(quarantine.groupBy("rejection_reason").count())    # why rows were rejected
display(spark.table(AUDIT).orderBy("run_at"))               # screenshot this for the call

# COMMAND ----------

# MAGIC %md
# MAGIC ### Known limits (say these honestly; they're the next steps)
# MAGIC - Batches are simulated from historical files, not a live feed.
# MAGIC - CMS rows have no version number, so a late-arriving *older* copy would overwrite a newer one.
# MAGIC   A real feed would carry a version or timestamp, and the MERGE would only update when it's newer.
# MAGIC - Batches are processed by calling `process_batch` by hand.
# MAGIC   Next: **Auto Loader** (pick up new files automatically) and **Lakeflow Jobs** (schedule).
