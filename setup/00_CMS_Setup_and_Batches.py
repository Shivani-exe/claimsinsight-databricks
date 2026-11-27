# Databricks notebook source

# ============================================================
# MVP2 CMS UPGRADE — STEP 1A: INSPECT SOURCE DATA
# ============================================================
# Purpose:
# Check which historical CMS claims are already available.
# Identify fields needed to create incremental events.
#
# READ ONLY — no tables or files are changed.
# ============================================================

from pyspark.sql import functions as F

# 1. Read the original CMS inpatient Bronze table.

cms = spark.table(
    "workspace.claimsinsight.bronze_inpatient"
)

print("Total CMS records:", cms.count())

# 2. Inspect the columns needed for incremental processing.

required = [
    "CLM_ID",
    "SEGMENT",
    "DESYNPUF_ID",
    "PRVDR_NUM",
    "CLM_FROM_DT",
    "CLM_THRU_DT",
    "CLM_PMT_AMT",
    "ADMTNG_ICD9_DGNS_CD"
]

missing = [c for c in required if c not in cms.columns]

print("Missing expected columns:", missing)

if missing:
    print("Available columns:", cms.columns)
    raise ValueError("Review CMS source schema before continuing.")

# 3. Count inpatient records by claim start year.
# Historical CMS dates are typically stored as YYYYMMDD.

by_year = (
    cms
    .withColumn(
        "claim_year",
        F.substring(
            F.col("CLM_FROM_DT").cast("string"), 1, 4
        )
    )
    .groupBy("claim_year")
    .count()
    .orderBy("claim_year")
)

print("CMS records by year:")
display(by_year)

# 4. Inspect example claim records and field values.

display(
    cms.select(*required).limit(5)
)

print("PASS: CMS source inspection completed.")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a separate Unity Catalog Volume for CMS batches.
# MAGIC -- IF NOT EXISTS makes this safe to run again.
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS
# MAGIC workspace.claimsinsight.mvp2_cms_upgrade;
# MAGIC

# COMMAND ----------


# Create the landing folder inside the new Volume.
# This is where our 2009 and 2010 CMS batches will go.

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"
LANDING = BASE + "/landing"

dbutils.fs.mkdirs(LANDING)

print("PASS: CMS landing location created.")
print("Landing path:", LANDING)


# COMMAND ----------


# ============================================================
# STEP 1B — GENERATE REAL CMS-DERIVED CLAIM EVENT BATCHES
# ============================================================
# Batch 001: 2009 historical CMS claims
# Batch 002: 2010 CMS claims + simulated test scenarios
#
# Write to an isolated landing location.
# Do not touch existing MVP1 or MVP2 landing/checkpoints.
# ============================================================

from pyspark.sql import functions as F

cms = spark.table("workspace.claimsinsight.bronze_inpatient")

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"
LANDING = BASE + "/landing"

dbutils.fs.mkdirs(LANDING)

# BLOCK 1: Convert original CMS columns to the existing
# MVP2 event format. Event versions and timestamps are
# simulated metadata, not attributes supplied by CMS.

def prepare_cms_events(year):

    raw = cms.filter(
        F.substring(F.col("CLM_FROM_DT"), 1, 4) == str(year)
    )

    events = raw.select(
        F.col("CLM_ID").alias("claim_id"),
        F.col("SEGMENT").alias("segment"),
        F.col("CLM_PMT_AMT").alias("paid_amount"),
        F.col("DESYNPUF_ID").alias("beneficiary_id"),
        F.col("PRVDR_NUM").alias("provider_id"),
        F.to_date(
            "CLM_THRU_DT", "yyyyMMdd"
        ).cast("string").alias("claim_through_date"),
        F.col("ADMTNG_ICD9_DGNS_CD").alias("diagnosis_code")
    )

    events = events.filter(
        F.col("claim_id").isNotNull()
        & F.col("segment").isNotNull()
        & F.col("paid_amount").isNotNull()
        & F.col("beneficiary_id").isNotNull()
        & F.col("provider_id").isNotNull()
        & F.col("claim_through_date").isNotNull()
    )

    events = (
        events
        .withColumn("event_version", F.lit(1))
        .withColumn("operation", F.lit("INSERT"))
        .withColumn(
            "source_updated_at",
            F.lit(f"2026-11-{1 if year == 2009 else 2:02d}T00:00:00")
        )
        .withColumn(
            "event_id",
            F.concat(
                F.lit(f"CMS{year}_"),
                F.sha2(
                    F.concat_ws(
                        "|", F.col("claim_id"),
                        F.col("segment")
                    ),
                    256
                )
            )
        )
    )

    return events


# BLOCK 2: Prepare both historical years.

batch_2009 = prepare_cms_events(2009)
batch_2010 = prepare_cms_events(2010)

# Guard against ambiguous business keys.
for year, df in [(2009, batch_2009), (2010, batch_2010)]:
    duplicates = (
        df.groupBy("claim_id", "segment")
        .count().filter("count > 1")
    )
    assert duplicates.limit(1).count() == 0, (
        f"Duplicate claim keys in {year}; review before writing."
    )

print("Valid 2009 records:", batch_2009.count())
print("Valid 2010 records:", batch_2010.count())


# BLOCK 3: Create simulated scenarios for Batch 002.
# Repeats preserve the exact 2009 event payload.

repeats = batch_2009.orderBy("claim_id").limit(100)

# Ten newer corrections to existing 2009 claims.
corrections = (
    batch_2009.orderBy(F.desc("claim_id")).limit(10)
    .withColumn("event_version", F.lit(2))
    .withColumn("operation", F.lit("UPDATE"))
    .withColumn(
        "paid_amount",
        (
            F.col("paid_amount").cast("decimal(18,2)")
            + F.lit(100)
        ).cast("string")
    )
    .withColumn(
        "source_updated_at",
        F.lit("2026-11-03T00:00:00")
    )
    .withColumn(
        "event_id",
        F.concat(
            F.lit("CMS_CORR_"),
            F.sha2(
                F.concat_ws(
                    "|", F.col("claim_id"),
                    F.col("segment")
                ), 256
            )
        )
    )
)

# Five deliberately invalid claim identifiers.
missing_ids = (
    batch_2010.orderBy("claim_id").limit(5)
    .withColumn("claim_id", F.lit(None).cast("string"))
    .withColumn(
        "event_id",
        F.concat(F.lit("CMS_BAD_ID_"), F.col("event_id"))
    )
)

# Three deliberately invalid payment values.
bad_payments = (
    batch_2010.orderBy(F.desc("claim_id")).limit(3)
    .withColumn("paid_amount", F.lit("NOT_A_NUMBER"))
    .withColumn(
        "event_id",
        F.concat(F.lit("CMS_BAD_PAY_"), F.col("event_id"))
    )
)

batch_002 = (
    batch_2010
    .unionByName(repeats)
    .unionByName(corrections)
    .unionByName(missing_ids)
    .unionByName(bad_payments)
)

# BLOCK 4: Write two CSV batches to isolated folders.
# Our existing Auto Loader checkpoint will not be affected.

columns = [
    "claim_id", "segment", "event_id",
    "event_version", "operation", "paid_amount",
    "beneficiary_id", "provider_id",
    "claim_through_date", "diagnosis_code",
    "source_updated_at"
]

for batch_name, df in [
    ("cms_batch_001", batch_2009),
    ("cms_batch_002", batch_002)
]:
    path = f"{LANDING}/{batch_name}"

    (
        df.select(*columns)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(path)
    )

    print(f"PASS: {batch_name} created — {df.count():,} events")

print("PASS: CMS batch generation completed.")
print("Landing location:", LANDING)


# COMMAND ----------


# Create folders required by the CMS Auto Loader pipeline.
# Keep the existing landing files unchanged.

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"

for folder in ["incoming", "schemas", "checkpoints"]:
    dbutils.fs.mkdirs(f"{BASE}/{folder}")

# Display the updated Volume structure.

display(dbutils.fs.ls(BASE))

print("PASS: CMS pipeline folders created.")


# COMMAND ----------


# ============================================================
# STEP 2A — PREPARE ISOLATED CMS PIPELINE ENVIRONMENT
# ============================================================
# 1. Create a separate schema for CMS-scale Delta tables.
# 2. Create an incoming folder for Auto Loader.
# 3. Copy ONLY the 2009 batch into the incoming folder.
#
# This does not run ingestion or modify existing tables.
# ============================================================

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"

# BLOCK 1: Create an isolated schema.
# All CMS-scale Bronze, Silver, queue and audit tables
# will be stored separately from our original MVP2 tables.

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.claimsinsight_cms_mvp2
""")

print("PASS: Isolated CMS schema created.")

# BLOCK 2: Create folders for streaming ingestion.
# Separate checkpoints prevent conflicts with the
# original MVP2 Auto Loader and queue streams.

for folder in [
    "incoming",
    "schemas",
    "checkpoints"
]:
    dbutils.fs.mkdirs(f"{BASE}/{folder}")

print("PASS: CMS streaming folders created.")

# BLOCK 3: Locate the existing 2009 batch CSV.
# Spark wrote it inside the cms_batch_001 directory.

batch_path = BASE + "/landing/cms_batch_001"

csv_files = [
    file.path
    for file in dbutils.fs.ls(batch_path)
    if file.name.endswith(".csv")
]

assert len(csv_files) == 1, (
    "Expected exactly one CSV file in cms_batch_001."
)

# BLOCK 4: Copy the 2009 batch to our active incoming
# folder, without touching the original generated files.

destination = BASE + "/incoming/cms_batch_001.csv"

dbutils.fs.cp(
    csv_files[0],
    destination
)

print("PASS: 2009 CMS batch staged for ingestion.")
print("Incoming file:", destination)

# BLOCK 5: Verify only the intended batch is ready.

incoming_files = [
    file.name
    for file in dbutils.fs.ls(BASE + "/incoming")
    if file.name.endswith(".csv")
]

assert incoming_files == ["cms_batch_001.csv"], (
    "Unexpected files in incoming folder. "
    "Do not start ingestion yet."
)

print("PASS: Only Batch 001 is ready.")


# COMMAND ----------


# ============================================================
# STEP 2B — INITIALIZE ISOLATED CMS PIPELINE TABLES
# ============================================================
# Reuse existing MVP2 table structures, but start empty.
# Do not overwrite tables if they already exist.

OLD = "workspace.claimsinsight"
NEW = "workspace.claimsinsight_cms_mvp2"

# 1. Create isolated schema.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NEW}")

# 2. Create empty tables based on existing structures.
tables = [
    "bronze_claim_events_mvp2",
    "mvp2_processing_queue",
    "silver_claims_current_mvp2",
    "quarantine_claim_events_mvp2",
    "mvp2_event_processing_ledger",
    "mvp2_processing_decisions",
    "mvp2_batch_audit"
]

for table in tables:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {NEW}.{table}
        USING DELTA
        AS SELECT *
        FROM {OLD}.{table}
        WHERE 1 = 0
    """)

    count = spark.table(f"{NEW}.{table}").count()
    print(f"{table}: {count} records")

# 3. Verify initial table state.
assert all(
    spark.table(f"{NEW}.{table}").count() == 0
    for table in tables
), "An existing CMS table contains data; review before proceeding."

print("PASS: Seven empty CMS pipeline tables are ready.")


# COMMAND ----------


# ============================================================
# CMS MVP2 — CHECK SOURCE DATA QUALITY
# ============================================================
# Count missing required fields in our 2009 events.
# This helps us choose appropriate validation rules.
# READ ONLY — no tables or records are modified.
# ============================================================

from pyspark.sql import functions as F

queue = spark.table(
    "workspace.claimsinsight_cms_mvp2.mvp2_processing_queue"
)

fields = [
    "claim_id",
    "segment",
    "event_id",
    "event_version",
    "paid_amount",
    "beneficiary_id",
    "provider_id",
    "claim_through_date",
    "source_updated_at"
]

checks = [
    F.sum(
        F.when(
            F.col(c).isNull()
            | (F.trim(F.col(c).cast("string")) == ""),
            1
        ).otherwise(0)
    ).alias(c)
    for c in fields
]

display(queue.agg(*checks))

print("PASS: CMS data-quality inspection completed.")


# COMMAND ----------


# ============================================================
# CMS MVP2 — STAGE BATCH 002
# ============================================================
# Copy the existing 2010 CMS batch to the Auto Loader input.
# Keep the original generated CSV file unchanged.
# Do not reset checkpoints or overwrite Delta tables.
# ============================================================

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"

batch_path = BASE + "/landing/cms_batch_002"

csv_files = [
    f.path
    for f in dbutils.fs.ls(batch_path)
    if f.name.endswith(".csv")
]

assert len(csv_files) == 1, (
    "Expected exactly one Batch 002 CSV file."
)

destination = BASE + "/incoming/cms_batch_002.csv"

# Avoid overwriting a file that Auto Loader may have
# already processed in a previous execution.

existing = [
    f.name
    for f in dbutils.fs.ls(BASE + "/incoming")
]

if "cms_batch_002.csv" not in existing:
    dbutils.fs.cp(csv_files[0], destination)
    print("PASS: Batch 002 copied into incoming.")
else:
    print("PASS: Batch 002 is already staged.")

print("Incoming file:", destination)
