# Databricks notebook source
# Exported original MVP1 cell 1; review write operations before reproduction.

from pyspark.sql import functions as F

bronze = spark.table(
    "workspace.claimsinsight.bronze_inpatient"
)

silver_segments = (
    bronze.select(
        F.col("CLM_ID").alias("claim_id"),
        F.col("DESYNPUF_ID").alias("beneficiary_id"),
        F.col("SEGMENT").cast("int").alias("segment"),

        F.to_date(
            "CLM_FROM_DT", "yyyyMMdd"
        ).alias("claim_from_date"),

        F.to_date(
            "CLM_THRU_DT", "yyyyMMdd"
        ).alias("claim_through_date"),

        F.expr(
            "try_cast(CLM_PMT_AMT AS DECIMAL(18,2))"
        ).alias("paid_amount"),

        F.col("PRVDR_NUM").alias("provider_id"),

        F.col("ICD9_DGNS_CD_1")
         .alias("diagnosis_code"),

        F.col("ingested_at"),
        F.col("source_file")
    )
)

(
    silver_segments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.claimsinsight.silver_inpatient_segments"
    )
)

print("Silver segment table created!")
print("Rows:", silver_segments.count())


# COMMAND ----------

# Exported original MVP1 cell 2; review write operations before reproduction.
display(
    spark.table(
        "workspace.claimsinsight.silver_inpatient_segments"
    )
    .groupBy("segment")
    .agg(
        F.count("*").alias("records"),
        F.count("claim_through_date").alias("dated_records")
    )
    .orderBy("segment")
)


# COMMAND ----------

# Exported original MVP1 cell 3; review write operations before reproduction.
from pyspark.sql import functions as F

silver = spark.table(
    "workspace.claimsinsight.silver_inpatient_segments"
)

# Count how many segments each claim contains
claim_segments = (
    silver.groupBy("claim_id")
    .agg(
        F.count("*").alias("segment_count")
    )
)

# Summarize single vs multi-segment claims
segment_summary = (
    claim_segments
    .groupBy("segment_count")
    .agg(
        F.count("*").alias("number_of_claims")
    )
    .orderBy("segment_count")
)

display(segment_summary)


# COMMAND ----------

# Exported original MVP1 cell 4; review write operations before reproduction.
from pyspark.sql import functions as F

# Get IDs of claims with exactly one segment
single_claim_ids = (
    claim_segments
    .filter(F.col("segment_count") == 1)
    .select("claim_id")
)

# Select single-segment claims from 2008
claims_2008 = (
    silver
    .join(single_claim_ids, on="claim_id", how="inner")
    .filter(F.year("claim_through_date") == 2008)
)

# Check payment quality
payment_quality = claims_2008.agg(
    F.count("*").alias("total_claims"),

    F.sum(
        F.when(F.col("paid_amount").isNull(), 1)
         .otherwise(0)
    ).alias("missing_payment"),

    F.sum(
        F.when(F.col("paid_amount") < 0, 1)
         .otherwise(0)
    ).alias("negative_payment"),

    F.sum(
        F.when(F.col("paid_amount") == 0, 1)
         .otherwise(0)
    ).alias("zero_payment"),

    F.sum(
        F.when(F.col("paid_amount") > 0, 1)
         .otherwise(0)
    ).alias("positive_payment"),

    F.sum("paid_amount").alias("total_payment")
)

display(payment_quality)


# COMMAND ----------

# Exported original MVP1 cell 5; review write operations before reproduction.
from pyspark.sql import functions as F

# Read the 2008 beneficiary source
beneficiaries = spark.table(
    "workspace.claimsinsight.bronze_beneficiary_2008"
)

# Inspect member ID quality and uniqueness
member_quality = beneficiaries.agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("DESYNPUF_ID").alias("unique_members"),
    F.sum(
        F.when(
            F.col("DESYNPUF_ID").isNull() |
            (F.trim(F.col("DESYNPUF_ID")) == ""),
            1
        ).otherwise(0)
    ).alias("missing_member_ids")
)

display(member_quality)

# Check for claims with no matching beneficiary
unmatched_claims = claims_2008.join(
    beneficiaries.select(
        F.col("DESYNPUF_ID").alias("beneficiary_id")
    ).distinct(),
    on="beneficiary_id",
    how="left_anti"
)

print(
    "Claims without matching 2008 beneficiary:",
    unmatched_claims.count()
)


# COMMAND ----------

# Exported original MVP1 cell 6; review write operations before reproduction.
unmatched_claims = claims_2008.join(
    beneficiaries.select(
        F.col("DESYNPUF_ID").alias("beneficiary_id")
    ).distinct(),
    on="beneficiary_id",
    how="left_anti"
)

print(
    "Claims without matching beneficiary:",
    unmatched_claims.count()
)


# COMMAND ----------

# Exported original MVP1 cell 7; review write operations before reproduction.
from pyspark.sql import functions as F

beneficiaries = spark.table(
    "workspace.claimsinsight.bronze_beneficiary_2008"
)

silver_beneficiaries = (
    beneficiaries.select(
        F.col("DESYNPUF_ID").alias("beneficiary_id"),

        F.to_date(
            "BENE_BIRTH_DT", "yyyyMMdd"
        ).alias("birth_date"),

        F.to_date(
            "BENE_DEATH_DT", "yyyyMMdd"
        ).alias("death_date"),

        F.col("BENE_SEX_IDENT_CD")
         .alias("sex_code"),

        F.col("BENE_RACE_CD")
         .alias("race_code"),

        F.col("SP_STATE_CODE")
         .alias("state_code"),

        F.col("BENE_COUNTY_CD")
         .alias("county_code"),

        F.col("ingested_at"),
        F.col("source_file")
    )
)

(
    silver_beneficiaries.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.claimsinsight.silver_beneficiaries_2008"
    )
)

print("Silver beneficiary table created!")

print("Rows:", silver_beneficiaries.count())


# COMMAND ----------

# Exported original MVP1 cell 8; review write operations before reproduction.
silver_check = spark.table(
    "workspace.claimsinsight.silver_beneficiaries_2008"
)

print("Rows:", silver_check.count())
print("Columns:", len(silver_check.columns))

silver_check.printSchema()

display(silver_check.limit(5))


# COMMAND ----------

# Exported original MVP1 cell 9; review write operations before reproduction.
from pyspark.sql import functions as F

silver_segments = spark.table(
    "workspace.claimsinsight.silver_inpatient_segments"
)

# Identify claims with only one segment
claim_counts = (
    silver_segments
    .groupBy("claim_id")
    .agg(F.count("*").alias("segment_count"))
)

single_claim_ids = (
    claim_counts
    .filter(F.col("segment_count") == 1)
    .select("claim_id")
)

# Select eligible 2008 claims
silver_claims_2008 = (
    silver_segments
    .join(single_claim_ids, "claim_id", "inner")
    .filter(F.year("claim_through_date") == 2008)
    .filter(F.col("paid_amount").isNotNull())
)

# Save the trusted claims table
(
    silver_claims_2008.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.claimsinsight.silver_inpatient_claims_2008"
    )
)

print("Silver claims table created successfully!")


# COMMAND ----------

# Exported original MVP1 cell 10; review write operations before reproduction.
claims_check = spark.table(
    "workspace.claimsinsight.silver_inpatient_claims_2008"
)

print("Total claims:", claims_check.count())
print(
    "Unique claim IDs:",
    claims_check.select("claim_id").distinct().count()
)

display(
    claims_check.agg(
        F.sum("paid_amount").alias("total_paid"),
        F.min("claim_through_date").alias("earliest_date"),
        F.max("claim_through_date").alias("latest_date")
    )
)
