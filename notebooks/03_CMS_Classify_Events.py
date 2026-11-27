# Databricks notebook source

# ============================================================
# CMS MVP2 — TASK 3: CLASSIFY EVENTS
# ============================================================
# Read unprocessed CMS deliveries from the processing queue.
# Validate fields, identify repeats and compare versions.
# Persist decisions before modifying Silver.
#
# This notebook writes ONLY to the decisions table.
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

BASE = "workspace.claimsinsight_cms_mvp2"

QUEUE = f"{BASE}.mvp2_processing_queue"
LEDGER = f"{BASE}.mvp2_event_processing_ledger"
SILVER = f"{BASE}.silver_claims_current_mvp2"
DECISIONS = f"{BASE}.mvp2_processing_decisions"

KEYS = ["source_file", "event_id"]

PAYLOAD = [
    "claim_id", "segment", "event_id", "event_version",
    "operation", "paid_amount", "beneficiary_id",
    "provider_id", "claim_through_date",
    "diagnosis_code", "source_updated_at"
]

# BLOCK 1: Discover deliveries not yet registered or staged.
# Existing decisions must remain unchanged during retries.

queue = spark.table(QUEUE)
ledger_keys = spark.table(LEDGER).select(*KEYS).distinct()

saved_keys = (
    spark.table(DECISIONS)
    .select(*KEYS)
    .distinct()
)

pending = (
    queue
    .join(ledger_keys, KEYS, "left_anti")
    .join(saved_keys, KEYS, "left_anti")
)

count = pending.count()
print("New CMS events:", count)

# BLOCK 2: Check for duplicate delivery identifiers.
# Stop if the same delivery appears more than once.

assert pending.select(*KEYS).distinct().count() == count
assert pending.select("event_id").distinct().count() == count

assert pending.filter(
    F.col("source_file").isNull()
    | F.col("event_id").isNull()
    | (F.trim("event_id") == "")
).limit(1).count() == 0

# BLOCK 3: Calculate a consistent event fingerprint.
# This distinguishes identical replays from event-ID reuse.

def fingerprint(df):
    return df.withColumn(
        "_payload_hash",
        F.sha2(
            F.to_json(
                F.struct(*[
                    F.col(c).cast("string").alias(c)
                    for c in PAYLOAD
                ]),
                {"ignoreNullFields": "false"}
            ),
            256
        )
    )

historical = queue.join(
    ledger_keys, KEYS, "inner"
)

previous = (
    fingerprint(historical)
    .groupBy("event_id")
    .agg(
        F.collect_set("_payload_hash")
        .alias("previous_hashes")
    )
)

# BLOCK 4: Convert raw text into typed fields.
# try_cast produces NULL instead of crashing on bad values.

incoming = (
    fingerprint(pending)
    .join(previous, "event_id", "left")
    .withColumn("segment", F.expr("try_cast(segment AS INT)"))
    .withColumn(
        "event_version",
        F.expr("try_cast(event_version AS INT)")
    )
    .withColumn(
        "paid_amount",
        F.expr("try_cast(paid_amount AS DECIMAL(18,2))")
    )
    .withColumn(
        "claim_through_date",
        F.expr("try_cast(claim_through_date AS DATE)")
    )
    .withColumn(
        "source_updated_at",
        F.expr("try_cast(source_updated_at AS TIMESTAMP)")
    )
)

# BLOCK 5: Compare incoming claims with current Silver.
# Rename target columns to avoid ambiguous references.

current = spark.table(SILVER).select(
    F.col("claim_id").alias("s_claim_id"),
    F.col("segment").alias("s_segment"),
    F.col("event_version").alias("s_version"),
    F.col("paid_amount").alias("s_payment")
)

joined = incoming.join(
    current,
    (incoming["claim_id"] == current["s_claim_id"])
    & (incoming["segment"] == current["s_segment"]),
    "left"
)

# BLOCK 6: Define validation and replay rules.

missing_id = (
    F.col("claim_id").isNull()
    | (F.trim("claim_id") == "")
)

invalid_fields = (
    F.col("segment").isNull()
    | (F.col("segment") < 1)
    | F.col("event_version").isNull()
    | (F.col("event_version") < 1)
    | F.col("beneficiary_id").isNull()
    | (F.trim("beneficiary_id") == "")
    | F.col("provider_id").isNull()
    | (F.trim("provider_id") == "")
    | F.col("claim_through_date").isNull()
    | F.col("source_updated_at").isNull()
    | ~F.col("operation").isin("INSERT", "UPDATE")
)

seen = F.col("previous_hashes").isNotNull()

exact_replay = seen & F.coalesce(
    F.array_contains(
        F.col("previous_hashes"),
        F.col("_payload_hash")
    ),
    F.lit(False)
)

event_id_conflict = seen & ~exact_replay

version_conflict = (
    (F.col("event_version") == F.col("s_version"))
    & ~F.col("paid_amount").eqNullSafe(F.col("s_payment"))
)

# BLOCK 7: Assign processing status to each record.

classified = joined.withColumn(
    "processing_status",
    F.when(missing_id, "REJECT_MISSING_ID")
    .when(invalid_fields, "REJECT_INVALID_FIELDS")
    .when(
        F.col("paid_amount").isNull(),
        "REJECT_INVALID_PAYMENT"
    )
    .when(
        F.col("_rescued_data").isNotNull(),
        "REJECT_SCHEMA_DRIFT"
    )
    .when(exact_replay, "IGNORE_DUPLICATE_EVENT")
    .when(event_id_conflict, "REJECT_EVENT_ID_CONFLICT")
    .when(
        F.coalesce(version_conflict, F.lit(False)),
        "REJECT_VERSION_CONFLICT"
    )
    .when(
        F.col("event_version") < F.col("s_version"),
        "IGNORE_OLDER_VERSION"
    )
    .when(
        F.col("event_version") == F.col("s_version"),
        "ALREADY_APPLIED"
    )
    .when(
        (F.col("operation") == "UPDATE")
        & F.col("s_claim_id").isNull(),
        "REJECT_UPDATE_WITHOUT_BASE"
    )
    .otherwise("READY_FOR_MERGE")
)

# BLOCK 8: Check for competing approved claim updates.

ready = classified.filter(
    F.col("processing_status") == "READY_FOR_MERGE"
)

competing = (
    ready.groupBy("claim_id", "segment")
    .count()
    .filter("count > 1")
)

assert competing.limit(1).count() == 0, (
    "Multiple approved changes for the same claim key."
)

# BLOCK 9: Match the existing persistent decisions schema.
# This avoids changing the structure of downstream tables.

target_schema = spark.table(DECISIONS).schema

staged = classified.withColumn(
    "classified_at",
    F.current_timestamp()
)

staged = staged.select(*[
    F.col(field.name).cast(field.dataType).alias(field.name)
    for field in target_schema.fields
])

# BLOCK 10: Save decisions only once per delivery.

if count > 0:
    (
        DeltaTable.forName(spark, DECISIONS)
        .alias("t")
        .merge(
            staged.alias("s"),
            """
            t.source_file = s.source_file
            AND t.event_id = s.event_id
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

# BLOCK 11: Verify classifications were persisted.

remaining = pending.select(*KEYS).join(
    spark.table(DECISIONS).select(*KEYS),
    KEYS,
    "left_anti"
)

assert remaining.count() == 0

display(
    spark.table(DECISIONS)
    .groupBy("processing_status")
    .count()
    .orderBy("processing_status")
)

print("PASS: CMS classifications persisted.")
print("Newly classified events:", count)
