# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver (clean, typed, checked)
# MAGIC
# MAGIC **Problem:** Bronze is all text. You can't add up `"26000.00"` or compare dates stored as `"20090412"`.
# MAGIC And nobody has checked the key yet.
# MAGIC
# MAGIC **Silver = the sorting room.** Give each column its real type, keep only the columns we need,
# MAGIC and run *quality gates* (checks that stop the pipeline if something is wrong).
# MAGIC
# MAGIC We build three Silver tables, at three different **grains**:
# MAGIC | Table | One row = | Key |
# MAGIC |---|---|---|
# MAGIC | `silver_inpatient_segments` | one segment of one claim | CLM_ID + SEGMENT |
# MAGIC | `silver_inpatient_claims_2008` | one 2008 claim that has only 1 segment | CLM_ID |
# MAGIC | `silver_beneficiaries_2008` | one patient | DESYNPUF_ID |

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Segments table (same grain as the source)

# COMMAND ----------

bronze_inpatient = spark.table(t("bronze_inpatient"))
segments = to_silver_segments(bronze_inpatient)   # the cleaning rules live in 00_config

segments.printSchema()   # READ THIS: dates should now say date, money decimal, SEGMENT integer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Quality gates (the pipeline STOPS if any fails)
# MAGIC `assert condition, message` → if the condition is False, stop with that message.
# MAGIC *Analogy: a factory inspector who halts the line instead of letting bad parts through.*

# COMMAND ----------

bronze_rows = bronze_inpatient.count()
silver_rows = segments.count()
key_dupes = count_key_duplicates(segments, KEY_COLS)
null_keys = segments.filter(F.col("CLM_ID").isNull() | F.col("SEGMENT").isNull()).count()

print("Bronze rows      :", bronze_rows)
print("Silver rows      :", silver_rows)
print("Key duplicates   :", key_dupes)
print("Null keys        :", null_keys)

assert silver_rows == bronze_rows, "Rows were lost or added between Bronze and Silver"
assert key_dupes == 0, "CLM_ID + SEGMENT is not unique: the grain assumption is wrong"
assert null_keys == 0, "Some rows have no key"

# COMMAND ----------

segments.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(t("silver_inpatient_segments"))
print("Wrote", t("silver_inpatient_segments"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Claims table for reporting (2008, single-segment only)
# MAGIC **Why only single-segment?** A 2-segment claim's money is split across 2 rows, and how to combine it
# MAGIC isn't documented. Rather than guess (and risk double-counting), the reporting cohort uses claims with
# MAGIC exactly one segment, and we *record* how many we left out. That's a defensible, documented choice.

# COMMAND ----------

segments_per_claim = segments.groupBy("CLM_ID").agg(F.count("*").alias("n_segments"))

claims_2008 = (
    segments
    .join(segments_per_claim, on="CLM_ID", how="inner")
    .filter((F.col("n_segments") == 1) & (F.year("CLM_FROM_DT") == 2008))
    .drop("n_segments", "SEGMENT")
)

multi_segment_claims = segments_per_claim.filter("n_segments > 1").count()
print("Claims with 2 segments (excluded from reporting):", multi_segment_claims)
print("2008 single-segment claims:", claims_2008.count())

assert count_key_duplicates(claims_2008, ["CLM_ID"]) == 0, "CLM_ID should be unique at claim grain"

claims_2008.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(t("silver_inpatient_claims_2008"))
print("Wrote", t("silver_inpatient_claims_2008"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Beneficiaries (one row per patient)

# COMMAND ----------

beneficiaries = (
    spark.table(t("bronze_beneficiary_2008"))
    .select(
        F.trim("DESYNPUF_ID").alias("DESYNPUF_ID"),
        F.to_date("BENE_BIRTH_DT", "yyyyMMdd").alias("BENE_BIRTH_DT"),
        F.to_date("BENE_DEATH_DT", "yyyyMMdd").alias("BENE_DEATH_DT"),
        F.col("BENE_SEX_IDENT_CD"),
        F.col("SP_STATE_CODE"),      # stays text: "06" must not become 6
        F.col("BENE_COUNTY_CD"),
        "source_file", "ingested_at",
    )
)

assert count_key_duplicates(beneficiaries, ["DESYNPUF_ID"]) == 0, "Patient ID should be unique"

beneficiaries.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(t("silver_beneficiaries_2008"))
print("Wrote", t("silver_beneficiaries_2008"), "rows:", beneficiaries.count())
