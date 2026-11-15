# Databricks notebook source
# Exported original MVP1 cell 1; review write operations before reproduction.

from pyspark.sql import functions as F

df = spark.table(
    "workspace.claimsinsight.bronze_inpatient"
)

important_columns = [
    "CLM_ID",
    "DESYNPUF_ID",
    "SEGMENT",
    "CLM_FROM_DT",
    "CLM_THRU_DT",
    "CLM_PMT_AMT",
    "ICD9_DGNS_CD_1"
]

display(
    df.select(*important_columns).limit(10)
)


# COMMAND ----------

# Exported original MVP1 cell 2; review write operations before reproduction.
year_counts = (
    df
    .withColumn(
        "claim_year",
        F.year(
            F.to_date("CLM_THRU_DT", "yyyyMMdd")
        )
    )
    .groupBy("claim_year")
    .count()
    .orderBy("claim_year")
)

display(year_counts)


# COMMAND ----------

# Exported original MVP1 cell 3; review write operations before reproduction.
from pyspark.sql import functions as F

invalid_date_df = (
    df
    .filter(
        F.to_date(
            F.col("CLM_THRU_DT"),
            "yyyyMMdd"
        ).isNull()
    )
)

print("Records with null/invalid claim-through dates:",
      invalid_date_df.count())

display(
    invalid_date_df.select(
        "CLM_ID",
        "DESYNPUF_ID",
        "CLM_FROM_DT",
        "CLM_THRU_DT",
        "CLM_PMT_AMT"
    ).limit(20)
)


# COMMAND ----------

# Exported original MVP1 cell 4; review write operations before reproduction.
from pyspark.sql import functions as F

date_quality = df.agg(
    F.count("*").alias("total_claims"),

    F.sum(
        F.when(F.col("CLM_FROM_DT").isNull(), 1).otherwise(0)
    ).alias("missing_start_date"),

    F.sum(
        F.when(F.col("CLM_THRU_DT").isNull(), 1).otherwise(0)
    ).alias("missing_end_date"),

    F.sum(
        F.when(
            F.col("CLM_FROM_DT").isNull() &
            F.col("CLM_THRU_DT").isNull(),
            1
        ).otherwise(0)
    ).alias("both_dates_missing")
)

display(date_quality)


# COMMAND ----------

# Exported original MVP1 cell 5; review write operations before reproduction.
from pyspark.sql import functions as F

# Check how many claim IDs appear more than once
duplicate_claim_ids = (
    df.groupBy("CLM_ID")
      .count()
      .filter(F.col("count") > 1)
)

# Check repeated combinations of claim ID and segment
duplicate_claim_segments = (
    df.groupBy("CLM_ID", "SEGMENT")
      .count()
      .filter(F.col("count") > 1)
)

print(
    "Repeated claim IDs:",
    duplicate_claim_ids.count()
)

print(
    "Repeated claim ID + segment combinations:",
    duplicate_claim_segments.count()
)

print("Segment distribution:")

display(
    df.groupBy("SEGMENT")
      .count()
      .orderBy("SEGMENT")
)


# COMMAND ----------

# Exported original MVP1 cell 6; review write operations before reproduction.
from pyspark.sql import functions as F

segment_date_check = (
    df.groupBy("SEGMENT")
    .agg(
        F.count("*").alias("total_records"),
        F.sum(
            F.when(
                F.col("CLM_FROM_DT").isNull() &
                F.col("CLM_THRU_DT").isNull(),
                1
            ).otherwise(0)
        ).alias("both_dates_missing")
    )
    .orderBy("SEGMENT")
)

display(segment_date_check)


# COMMAND ----------

# Exported original MVP1 cell 7; review write operations before reproduction.
from pyspark.sql import functions as F

segment_1 = (
    df.filter(F.col("SEGMENT") == "1")
    .select(
        "CLM_ID",
        F.col("CLM_THRU_DT").alias("segment1_date"),
        F.col("CLM_PMT_AMT").alias("segment1_payment")
    )
)

segment_2 = (
    df.filter(F.col("SEGMENT") == "2")
    .select(
        "CLM_ID",
        F.col("CLM_THRU_DT").alias("segment2_date"),
        F.col("CLM_PMT_AMT").alias("segment2_payment")
    )
)

comparison = (
    segment_2.join(segment_1, on="CLM_ID", how="left")
    .select(
        "CLM_ID",
        "segment1_date",
        "segment2_date",
        "segment1_payment",
        "segment2_payment"
    )
)

print("Segment 2 records:", comparison.count())

display(comparison.limit(10))


# COMMAND ----------

# Exported original MVP1 cell 8; review write operations before reproduction.
from pyspark.sql import functions as F

payment_comparison = (
    comparison
    .withColumn(
        "payment_1",
        F.col("segment1_payment").cast("decimal(18,2)")
    )
    .withColumn(
        "payment_2",
        F.col("segment2_payment").cast("decimal(18,2)")
    )
    .withColumn(
        "payment_difference",
        F.col("payment_2") - F.col("payment_1")
    )
)

summary = payment_comparison.agg(
    F.count("*").alias("total_segment2_records"),

    F.sum(
        F.when(F.col("payment_1").isNull(), 1).otherwise(0)
    ).alias("missing_segment1_match"),

    F.sum(
        F.when(F.col("payment_1") == F.col("payment_2"), 1)
         .otherwise(0)
    ).alias("same_payment"),

    F.sum(
        F.when(F.col("payment_1") != F.col("payment_2"), 1)
         .otherwise(0)
    ).alias("different_payment"),

    F.sum("payment_1").alias("segment1_total"),

    F.sum("payment_2").alias("segment2_total")
)

display(summary)


# COMMAND ----------

# Exported original MVP1 cell 9; review write operations before reproduction.
from pyspark.sql import functions as F

fields_to_check = [
    "CLM_FROM_DT",
    "CLM_THRU_DT",
    "PRVDR_NUM",
    "CLM_PMT_AMT",
    "CLM_ADMSN_DT",
    "ICD9_DGNS_CD_1",
    "HCPCS_CD_1"
]

checks = [
    F.count("*").alias("total_rows")
]

for field in fields_to_check:
    checks.append(
        F.count(F.col(field)).alias(
            f"{field}_populated"
        )
    )

segment_profile = (
    df.groupBy("SEGMENT")
      .agg(*checks)
      .orderBy("SEGMENT")
)

display(segment_profile)


# COMMAND ----------

# Exported original MVP1 cell 10; review write operations before reproduction.
from pyspark.sql import functions as F

s1 = (
    df.filter(F.col("SEGMENT") == "1")
    .select(
        "CLM_ID",
        F.col("DESYNPUF_ID").alias("member_1"),
        F.col("PRVDR_NUM").alias("provider_1"),
        F.col("CLM_ADMSN_DT").alias("admission_1")
    )
)

s2 = (
    df.filter(F.col("SEGMENT") == "2")
    .select(
        "CLM_ID",
        F.col("DESYNPUF_ID").alias("member_2"),
        F.col("PRVDR_NUM").alias("provider_2"),
        F.col("CLM_ADMSN_DT").alias("admission_2")
    )
)

paired = s2.join(s1, "CLM_ID", "left")

display(
    paired.agg(
        F.count("*").alias("segment2_rows"),
        F.sum(F.when(
            F.col("member_1").isNull(), 1
        ).otherwise(0)).alias("missing_segment1"),

        F.sum(F.when(
            F.col("member_1").eqNullSafe(F.col("member_2")),
            1
        ).otherwise(0)).alias("same_beneficiary"),

        F.sum(F.when(
            F.col("provider_1").eqNullSafe(F.col("provider_2")),
            1
        ).otherwise(0)).alias("same_provider"),

        F.sum(F.when(
            F.col("admission_1").eqNullSafe(F.col("admission_2")),
            1
        ).otherwise(0)).alias("same_admission_date")
    )
)
