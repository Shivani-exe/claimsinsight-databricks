# Databricks notebook source

# ============================================================
# CMS MVP2 — TASK 2: STAGE EVENTS
# ============================================================
# Read newly ingested CMS Bronze records incrementally.
# Append them to a persistent processing queue.
#
# This task does not validate, MERGE or quarantine claims.
# It does not modify the original MVP2 pipeline.
# ============================================================

SCHEMA = "workspace.claimsinsight_cms_mvp2"

BASE = "/Volumes/workspace/claimsinsight/mvp2_cms_upgrade"

BRONZE = f"{SCHEMA}.bronze_claim_events_mvp2"
QUEUE = f"{SCHEMA}.mvp2_processing_queue"

CHECKPOINT = BASE + "/checkpoints/bronze_processing_queue"

# BLOCK 1: Stream new Bronze records.
# Checkpoint tracks progress across job runs.

bronze_stream = (
    spark.readStream
    .format("delta")
    .table(BRONZE)
)

# BLOCK 2: Append records to the CMS processing queue.
# availableNow processes available records and then stops.

query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(QUEUE)
)

# BLOCK 3: Wait for staging to finish.
# Fail the task if streaming raises an error.

query.awaitTermination()

if query.exception() is not None:
    raise RuntimeError(str(query.exception()))

# BLOCK 4: Display current queue size.
# No hardcoded historical batch assertions.

queue_count = spark.table(QUEUE).count()

print("PASS: CMS event staging completed.")
print("CMS processing queue records:", queue_count)
