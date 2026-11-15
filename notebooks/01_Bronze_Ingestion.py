# Databricks notebook source
# Exported original MVP1 cell 1; review write operations before reproduction.

volume_path = "/Volumes/workspace/claimsinsight/raw_files/"

files = dbutils.fs.ls(volume_path)

for file in files:
    print(f"File: {file.name}")
    print(f"Size: {file.size / (1024 * 1024):.2f} MB")
    print("---------------------------")


# COMMAND ----------

# Exported original MVP1 cell 2; review write operations before reproduction.
inpatient_path = (
    "/Volumes/workspace/claimsinsight/raw_files/"
    "DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv"
)

inpatient_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(inpatient_path)
)

print("Total rows:", inpatient_df.count())
print("Total columns:", len(inpatient_df.columns))

display(inpatient_df.limit(5))


# COMMAND ----------

# Exported original MVP1 cell 3; review write operations before reproduction.
beneficiary_path = (
    "/Volumes/workspace/claimsinsight/raw_files/"
    "DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"
)

beneficiary_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .csv(beneficiary_path)
)

print("Total beneficiary rows:", beneficiary_df.count())
print("Total columns:", len(beneficiary_df.columns))

display(beneficiary_df.limit(5))


# COMMAND ----------

# Exported original MVP1 cell 4; review write operations before reproduction.
from pyspark.sql import functions as F

bronze_inpatient = (
    inpatient_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit(inpatient_path))
)

(
    bronze_inpatient.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.claimsinsight.bronze_inpatient")
)

print("Bronze inpatient table created successfully!")


# COMMAND ----------

# Exported original MVP1 cell 5; review write operations before reproduction.
bronze_beneficiary = (
    beneficiary_df
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit(beneficiary_path))
)

(
    bronze_beneficiary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.claimsinsight.bronze_beneficiary_2008")
)

print("Bronze beneficiary table created successfully!")


# COMMAND ----------

# Exported original MVP1 cell 6; review write operations before reproduction.
tables = [
    "workspace.claimsinsight.bronze_inpatient",
    "workspace.claimsinsight.bronze_beneficiary_2008"
]

for table in tables:
    df = spark.table(table)

    print(f"Table: {table}")
    print(f"Rows: {df.count():,}")
    print(f"Columns: {len(df.columns)}")
    print("-------------------------")
