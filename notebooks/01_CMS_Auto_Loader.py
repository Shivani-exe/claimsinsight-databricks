# Databricks notebook source

# ============================================================
# CMS MVP2 — TASK 1: AUTO LOADER INGESTION
# ============================================================
# Read CMS claim events from the isolated incoming folder.
# Reuse our existing ingestion logic with new paths.
# Do not change the original MVP2 tables or checkpoints.
# ============================================================

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"
SCHEMA = "workspace.claimsinsight_cms_mvp2"

# 1. Read new CMS event files.

incoming = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", BASE + "/schemas/claims")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .load(BASE + "/incoming")
)

# 2. Add ingestion metadata.

bronze = (
    incoming
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path"))
)

# 3. Ingest available files into isolated CMS Bronze.

query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        BASE + "/checkpoints/autoloader_claims"
    )
    .trigger(availableNow=True)
    .toTable(f"{SCHEMA}.bronze_claim_events_mvp2")
)

query.awaitTermination()

if query.exception() is not None:
    raise RuntimeError(str(query.exception()))

# 4. Report the resulting data volume.

count = spark.table(
    f"{SCHEMA}.bronze_claim_events_mvp2"
).count()

print("PASS: CMS Auto Loader ingestion completed.")
print("CMS Bronze records:", count)
