# Databricks notebook source

# ============================================================
# CLAIMSINSIGHT CMS MVP2 — TASK 5: AUDIT CLAIMS
# ============================================================
# PURPOSE:
# 1. Check that all incoming events were processed.
# 2. Reconcile decisions with the processing ledger.
# 3. Verify Silver and quarantine results.
# 4. Save one audit record per source file.
# 5. Avoid duplicate audits on reruns.
#
# This notebook only writes to the CMS audit table.
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DecimalType
)
from decimal import Decimal

# BLOCK 1: Define CMS pipeline tables.

BASE = "workspace.claimsinsight_cms_mvp2"

QUEUE = f"{BASE}.mvp2_processing_queue"
DECISIONS = f"{BASE}.mvp2_processing_decisions"
LEDGER = f"{BASE}.mvp2_event_processing_ledger"
SILVER = f"{BASE}.silver_claims_current_mvp2"
QUARANTINE = f"{BASE}.quarantine_claim_events_mvp2"
AUDIT = f"{BASE}.mvp2_batch_audit"

KEYS = ["source_file", "event_id"]

queue = spark.table(QUEUE)
decisions = spark.table(DECISIONS)
ledger = spark.table(LEDGER)
silver = spark.table(SILVER)
quarantine = spark.table(QUARANTINE)

# BLOCK 2: Find source files that are not yet audited.
# Use a stable file-based identifier to avoid duplicates.

audited_files = spark.table(AUDIT).select("run_id")

files = (
    queue.select("source_file").distinct()
    .withColumn("run_id", F.sha2("source_file", 256))
    .join(audited_files, "run_id", "left_anti")
    .collect()
)

print("Files awaiting audit:", len(files))

# BLOCK 3: Define the existing audit table schema.

audit_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("batch_name", StringType(), True),
    StructField("incoming_count", LongType(), True),
    StructField("merged_count", LongType(), True),
    StructField("quarantined_count", LongType(), True),
    StructField("ignored_count", LongType(), True),
    StructField("silver_row_count", LongType(), True),
    StructField(
        "silver_payment_total",
        DecimalType(18, 2),
        True
    ),
    StructField("audit_status", StringType(), True)
])

audit_rows = []

# BLOCK 4: Reconcile each incoming source file.

for file in files:

    path = file["source_file"]
    run_id = file["run_id"]

    incoming = queue.filter(
        F.col("source_file") == path
    )

    batch = decisions.filter(
        F.col("source_file") == path
    )

    incoming_count = incoming.count()
    decision_count = batch.count()

    assert incoming_count == decision_count, (
        f"Missing processing decisions: {path}"
    )

    assert (
        incoming.select(*KEYS).distinct().count()
        == incoming_count
    ), "Duplicate source delivery keys."

    assert (
        batch.select(*KEYS).distinct().count()
        == decision_count
    ), "Duplicate saved decisions."

    # BLOCK 5: Determine expected ledger outcomes.

    expected = batch.withColumn(
        "expected_status",
        F.when(
            F.col("processing_status") == "READY_FOR_MERGE",
            "MERGED"
        )
        .when(
            F.col("processing_status").startswith("REJECT_"),
            "QUARANTINED"
        )
        .when(
            F.col("processing_status") == "IGNORE_DUPLICATE_EVENT",
            "IGNORED_DUPLICATE"
        )
        .otherwise(F.col("processing_status"))
    )

    ledger_rows = ledger.select(
        *KEYS,
        F.col("processing_status").alias("ledger_status")
    )

    assert (
        ledger_rows.select(*KEYS).distinct().count()
        == ledger_rows.count()
    ), "Duplicate ledger delivery keys."

    checked = expected.join(
        ledger_rows,
        KEYS,
        "left"
    )

    mismatches = checked.filter(
        F.col("ledger_status").isNull()
        | ~F.col("expected_status").eqNullSafe(
            F.col("ledger_status")
        )
    )

    assert mismatches.limit(1).count() == 0, (
        f"Ledger reconciliation failed for {path}"
    )

    # BLOCK 6: Count processing outcomes.

    merged = checked.filter(
        F.col("expected_status") == "MERGED"
    )

    rejected = checked.filter(
        F.col("expected_status") == "QUARANTINED"
    )

    merged_count = merged.count()
    rejected_count = rejected.count()

    ignored_count = (
        decision_count - merged_count - rejected_count
    )

    assert (
        merged_count + rejected_count + ignored_count
        == incoming_count
    )

    # BLOCK 7: Verify quarantined records.

    if rejected_count > 0:

        saved_rejections = rejected.join(
            quarantine.select(
                *KEYS, "rejection_reason"
            ),
            KEYS,
            "inner"
        ).filter(
            F.col("processing_status")
            == F.col("rejection_reason")
        )

        assert saved_rejections.count() == rejected_count, (
            "Quarantine reconciliation failed."
        )

    # BLOCK 8: Verify approved claims exist in Silver.
    # A later version is acceptable because corrections
    # can replace older versions.

    if merged_count > 0:

        current = silver.select(
            F.col("claim_id").alias("silver_claim"),
            F.col("segment").alias("silver_segment"),
            F.col("event_version").alias("silver_version"),
            F.col("paid_amount").alias("silver_payment")
        )

        verified = merged.alias("d").join(
            current.alias("s"),
            (
                (F.col("d.claim_id") == F.col("s.silver_claim"))
                & (F.col("d.segment") == F.col("s.silver_segment"))
            ),
            "inner"
        ).filter(
            (F.col("silver_version") > F.col("event_version"))
            | (
                (F.col("silver_version") == F.col("event_version"))
                & F.col("silver_payment").eqNullSafe(
                    F.col("paid_amount")
                )
            )
        )

        assert verified.count() == merged_count, (
            "Silver reconciliation failed."
        )

    # BLOCK 9: Record the current Silver snapshot.
    # These are overall Silver totals, not batch payments.

    silver_count = silver.count()

    total_paid = silver.agg(
        F.sum("paid_amount").alias("total")
    ).first()["total"]

    audit_rows.append((
        run_id,
        path.rsplit("/", 1)[-1],
        incoming_count,
        merged_count,
        rejected_count,
        ignored_count,
        silver_count,
        Decimal(str(total_paid or 0)),
        "RECONCILED"
    ))

    print(
        f"PASS: {path.rsplit('/', 1)[-1]} | "
        f"incoming={incoming_count:,}, "
        f"merged={merged_count:,}, "
        f"quarantined={rejected_count}, "
        f"ignored={ignored_count}"
    )

# BLOCK 10: Save new audits with Delta MERGE.
# Existing audit records are never overwritten.

if audit_rows:

    new_audits = (
        spark.createDataFrame(audit_rows, audit_schema)
        .withColumn("audited_at", F.current_timestamp())
    )

    (
        DeltaTable.forName(spark, AUDIT)
        .alias("t")
        .merge(
            new_audits.alias("s"),
            "t.run_id = s.run_id"
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("PASS: Audit records saved.")

else:
    print("PASS: No new files to audit.")

# BLOCK 11: Display final audit results.

display(
    spark.table(AUDIT).select(
        "batch_name",
        "incoming_count",
        "merged_count",
        "quarantined_count",
        "ignored_count",
        "silver_row_count",
        "audit_status",
        "audited_at"
    ).orderBy(F.col("audited_at").desc())
)

print("PASS: CMS audit completed.")
