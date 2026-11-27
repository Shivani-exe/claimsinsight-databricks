# Databricks notebook source

# ============================================================
# CLAIMSINSIGHT CMS MVP2 — TASK 4: PROCESS CLAIMS
# ============================================================
# PURPOSE:
# Process classified CMS claim events using Delta MERGE.
#
# READY_FOR_MERGE -> Silver INSERT / UPDATE
# REJECT_*        -> Quarantine
# IGNORE_*        -> Ledger only
# ALREADY_APPLIED -> Ledger only
#
# Uses ONLY workspace.claimsinsight_cms_mvp2 tables.
# Original MVP1 and MVP2 tables remain untouched.
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# ------------------------------------------------------------
# BLOCK 1: Configure isolated CMS tables.
# ------------------------------------------------------------

BASE = "workspace.claimsinsight_cms_mvp2"

DECISIONS = f"{BASE}.mvp2_processing_decisions"
SILVER = f"{BASE}.silver_claims_current_mvp2"
QUARANTINE = f"{BASE}.quarantine_claim_events_mvp2"
LEDGER = f"{BASE}.mvp2_event_processing_ledger"
QUEUE = f"{BASE}.mvp2_processing_queue"

KEYS = ["source_file", "event_id"]

# ------------------------------------------------------------
# BLOCK 2: Read decisions not finalized in the ledger.
#
# Saved decisions remain unchanged during retries.
# This prevents reclassification after Silver is updated.
# ------------------------------------------------------------

registered = (
    spark.table(LEDGER)
    .select(*KEYS)
    .distinct()
)

pending = (
    spark.table(DECISIONS)
    .join(registered, KEYS, "left_anti")
    .cache()
)

try:
    pending_count = pending.count()

    print("Unfinished CMS events:", pending_count)

    if pending_count == 0:
        print("PASS: No unfinished CMS events.")

    else:

        # ----------------------------------------------------
        # BLOCK 3: Validate input decisions.
        #
        # Only one approved event may target a claim key
        # in a processing run.
        # ----------------------------------------------------

        assert (
            pending.select(*KEYS).distinct().count()
            == pending_count
        ), "Duplicate delivery decisions detected."

        allowed_statuses = [
            "READY_FOR_MERGE",
            "REJECT_MISSING_ID",
            "REJECT_INVALID_FIELDS",
            "REJECT_INVALID_PAYMENT",
            "REJECT_SCHEMA_DRIFT",
            "REJECT_EVENT_ID_CONFLICT",
            "REJECT_VERSION_CONFLICT",
            "REJECT_UPDATE_WITHOUT_BASE",
            "IGNORE_DUPLICATE_EVENT",
            "IGNORE_OLDER_VERSION",
            "ALREADY_APPLIED"
        ]

        assert pending.filter(
            F.col("processing_status").isNull()
            | ~F.col("processing_status").isin(allowed_statuses)
        ).limit(1).count() == 0, (
            "Unknown CMS processing status detected."
        )

        ready = pending.filter(
            F.col("processing_status") == "READY_FOR_MERGE"
        )

        ready_count = ready.count()

        assert (
            ready.select("claim_id", "segment").distinct().count()
            == ready_count
        ), "Competing updates for the same claim key."

        print("Approved CMS events:", ready_count)

        # ----------------------------------------------------
        # BLOCK 4: Prepare typed Silver records.
        #
        # Match the existing Silver schema to avoid
        # changing downstream table definitions.
        # ----------------------------------------------------

        if ready_count > 0:

            silver_schema = spark.table(SILVER).schema

            missing_columns = [
                field.name
                for field in silver_schema.fields
                if field.name not in ready.columns
            ]

            assert not missing_columns, (
                f"Missing Silver columns: {missing_columns}"
            )

            merge_source = ready.select(
                *[
                    F.col(field.name)
                    .cast(field.dataType)
                    .alias(field.name)
                    for field in silver_schema.fields
                ]
            )

            required = [
                "claim_id",
                "segment",
                "event_id",
                "event_version",
                "paid_amount"
            ]

            for c in required:
                assert merge_source.filter(
                    F.col(c).isNull()
                ).limit(1).count() == 0, (
                    f"Missing required field: {c}"
                )

            # ------------------------------------------------
            # BLOCK 5: Check Silver before MERGE.
            #
            # Reject older versions and conflicting payloads.
            # Accept identical already-applied versions
            # during recovery from a partial failure.
            # ------------------------------------------------

            business_fields = [
                c for c in [
                    "event_id",
                    "operation",
                    "paid_amount",
                    "beneficiary_id",
                    "provider_id",
                    "claim_through_date",
                    "diagnosis_code",
                    "source_updated_at"
                ]
                if c in merge_source.columns
            ]

            current = spark.table(SILVER).select(
                F.col("claim_id").alias("old_claim_id"),
                F.col("segment").alias("old_segment"),
                F.col("event_version").alias("old_version"),
                *[
                    F.col(c).alias("old_" + c)
                    for c in business_fields
                ]
            )

            compared = merge_source.alias("s").join(
                current.alias("t"),
                (
                    (F.col("s.claim_id") == F.col("t.old_claim_id"))
                    & (F.col("s.segment") == F.col("t.old_segment"))
                ),
                "left"
            )

            same_payload = F.lit(True)

            for c in business_fields:
                same_payload = same_payload & (
                    F.col("s." + c)
                    .eqNullSafe(F.col("t.old_" + c))
                )

            unsafe = compared.filter(
                (
                    F.col("old_version").isNotNull()
                    & (
                        (F.col("s.event_version") < F.col("old_version"))
                        | (
                            (F.col("s.event_version") == F.col("old_version"))
                            & ~same_payload
                        )
                    )
                )
                | (
                    (F.col("s.operation") == "UPDATE")
                    & F.col("old_claim_id").isNull()
                )
            )

            assert unsafe.limit(1).count() == 0, (
                "Stale or conflicting CMS updates detected."
            )

            # ------------------------------------------------
            # BLOCK 6: Apply version-aware Delta MERGE.
            #
            # New claim key -> INSERT
            # Newer version -> UPDATE
            # Same version -> No change
            # ------------------------------------------------

            (
                DeltaTable.forName(spark, SILVER)
                .alias("t")
                .merge(
                    merge_source.alias("s"),
                    """
                    t.claim_id = s.claim_id
                    AND t.segment = s.segment
                    """
                )
                .whenMatchedUpdateAll(
                    condition=(
                        "s.event_version > t.event_version"
                    )
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            # ------------------------------------------------
            # BLOCK 7: Verify approved events.
            #
            # Silver must contain the intended version
            # and business payload before ledger updates.
            # ------------------------------------------------

            verified = merge_source.alias("r").join(
                spark.table(SILVER).alias("s"),
                ["claim_id", "segment"],
                "inner"
            )

            matches = (
                F.col("r.event_version")
                == F.col("s.event_version")
            )

            for c in business_fields:
                matches = matches & (
                    F.col("r." + c)
                    .eqNullSafe(F.col("s." + c))
                )

            assert verified.filter(matches).count() == ready_count, (
                "Silver verification failed."
            )

            print("PASS: CMS Silver MERGE verified.")

        # ----------------------------------------------------
        # BLOCK 8: Collect rejected CMS events.
        #
        # Use original queue records to preserve values
        # such as paid_amount = NOT_A_NUMBER.
        # ----------------------------------------------------

        rejected = pending.filter(
            F.col("processing_status").startswith("REJECT_")
        ).select(
            *KEYS,
            "processing_status"
        )

        rejected_count = rejected.count()

        if rejected_count > 0:

            raw = spark.table(QUEUE).join(
                rejected,
                KEYS,
                "inner"
            )

            assert raw.count() == rejected_count, (
                "Missing or duplicate rejected CMS deliveries."
            )

            raw = (
                raw.withColumn(
                    "rejection_reason",
                    F.col("processing_status")
                )
                .withColumn(
                    "quarantined_at",
                    F.current_timestamp()
                )
            )

            # Match the existing quarantine table structure.

            quarantine_schema = spark.table(QUARANTINE).schema

            quarantine_source = raw.select(
                *[
                    (
                        F.col(field.name)
                        if field.name in raw.columns
                        else F.lit(None)
                    )
                    .cast(field.dataType)
                    .alias(field.name)
                    for field in quarantine_schema.fields
                ]
            )

            # Insert rejected deliveries only once.

            (
                DeltaTable.forName(spark, QUARANTINE)
                .alias("t")
                .merge(
                    quarantine_source.alias("s"),
                    """
                    t.source_file = s.source_file
                    AND t.event_id = s.event_id
                    """
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            # Confirm all rejected records are stored.

            checked_rejections = rejected.join(
                spark.table(QUARANTINE).select(
                    *KEYS,
                    "rejection_reason"
                ),
                KEYS,
                "inner"
            ).filter(
                F.col("processing_status")
                == F.col("rejection_reason")
            )

            assert checked_rejections.count() == rejected_count, (
                "CMS quarantine verification failed."
            )

            print("PASS: CMS quarantine verified.")

        # ----------------------------------------------------
        # BLOCK 9: Prepare final processing statuses.
        #
        # MERGED              -> Valid Silver events
        # QUARANTINED         -> Rejected events
        # IGNORED_DUPLICATE   -> Replayed event
        # Other classifications retain their status.
        # ----------------------------------------------------

        outcomes = (
            pending
            .select(*KEYS, "processing_status")
            .withColumn(
                "processing_status",
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
            .withColumn(
                "registered_at",
                F.current_timestamp()
            )
        )

        # ----------------------------------------------------
        # BLOCK 10: Save the processing ledger.
        #
        # Only register after Silver and quarantine checks.
        # Delta MERGE avoids duplicate ledger deliveries
        # during sequential retries.
        # ----------------------------------------------------

        (
            DeltaTable.forName(spark, LEDGER)
            .alias("t")
            .merge(
                outcomes.alias("s"),
                """
                t.source_file = s.source_file
                AND t.event_id = s.event_id
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        # ----------------------------------------------------
        # BLOCK 11: Confirm all decisions were finalized.
        # ----------------------------------------------------

        remaining = pending.select(*KEYS).join(
            spark.table(LEDGER).select(*KEYS),
            KEYS,
            "left_anti"
        )

        assert remaining.count() == 0, (
            "Some CMS events remain unregistered."
        )

        print("PASS: CMS processing ledger updated.")

        print("Processed deliveries:", pending_count)
        print("Approved for MERGE:", ready_count)
        print("Quarantined:", rejected_count)

finally:
    pending.unpersist()


# ------------------------------------------------------------
# BLOCK 12: Display final CMS pipeline metrics.
# ------------------------------------------------------------

silver_count = spark.table(SILVER).count()
ledger_count = spark.table(LEDGER).count()
quarantine_count = spark.table(QUARANTINE).count()

print("=" * 50)
print("CMS MVP2 PROCESSING SUMMARY")
print("=" * 50)

print("Silver claim segments:", silver_count)
print("Ledger deliveries:", ledger_count)
print("Quarantined events:", quarantine_count)

display(
    spark.table(LEDGER)
    .groupBy("processing_status")
    .count()
    .orderBy("processing_status")
)
