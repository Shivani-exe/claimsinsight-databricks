# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Setup + Bronze
# MAGIC
# MAGIC **Problem:** the CSVs are just files in a folder. You can't query, track or join them safely.
# MAGIC
# MAGIC **Bronze = the receiving desk.** Copy each file into a table *exactly as received* (everything as text),
# MAGIC and stamp each row with *when* it arrived and *which file* it came from. Nothing is fixed here.
# MAGIC
# MAGIC **Done when:** `bronze_inpatient` has 66,773 rows and `bronze_beneficiary_2008` has 116,352 rows.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 · Create the schema (floor) and two volumes (folders)
# MAGIC `IF NOT EXISTS` = safe to run again: it only creates what's missing.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.mvp2_landing")
print("Schema and volumes ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 · Upload the two CSVs (by hand, once)
# MAGIC Catalog → workspace → claiminsight → Volumes → **raw_files** → **Upload to this volume**:
# MAGIC - `DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv`
# MAGIC - `DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv`
# MAGIC
# MAGIC Then run the next cell to check they arrived.

# COMMAND ----------

for f in dbutils.fs.ls(RAW_VOLUME):
    print(f"{f.name:60s} {f.size / (1024 * 1024):8.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 · Read the CSVs, all columns as text
# MAGIC `inferSchema=false` → don't let Spark guess types. A state code like `06` would become `6`,
# MAGIC and an ID with leading zeros would lose them. Bronze must keep the data *exactly* as received.

# COMMAND ----------

def read_raw_csv(path):
    return (
        spark.read
        .option("header", "true")        # first line = column names
        .option("inferSchema", "false")  # keep every value as text
        .csv(path)
    )

inpatient_raw = read_raw_csv(INPATIENT_FILE)
beneficiary_raw = read_raw_csv(BENEFICIARY_FILE)

print("Inpatient  :", inpatient_raw.count(), "rows,", len(inpatient_raw.columns), "columns")
print("Beneficiary:", beneficiary_raw.count(), "rows,", len(beneficiary_raw.columns), "columns")
# PREDICT before running: inpatient should match what you found locally in pandas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 · Add the receiving stamp (lineage) and save as Delta tables
# MAGIC - `ingested_at` = when it was loaded; `source_file` = where it came from.
# MAGIC - **Delta** = a table format with version history that supports MERGE (needed in MVP2).
# MAGIC - `overwrite` = replace the table each run. Fine for a one-time full load, **not** for daily batches (→ MVP2).

# COMMAND ----------

def write_bronze(raw_df, source_path, table_name):
    bronze_df = (
        raw_df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.lit(source_path))
    )
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(t(table_name))
    )
    print(f"Wrote {t(table_name)}")

write_bronze(inpatient_raw, INPATIENT_FILE, "bronze_inpatient")
write_bronze(beneficiary_raw, BENEFICIARY_FILE, "bronze_beneficiary_2008")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 · Check: did anything get lost?

# COMMAND ----------

for name, expected in [("bronze_inpatient", 66773), ("bronze_beneficiary_2008", 116352)]:
    n = spark.table(t(name)).count()
    status = "OK" if n == expected else "MISMATCH"
    print(f"{name:28s} rows={n:,}  expected={expected:,}  {status}")
