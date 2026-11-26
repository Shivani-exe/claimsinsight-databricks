# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Config (shared settings + helper functions)
# MAGIC
# MAGIC **Problem:** every notebook needs the same names (catalog, schema, file paths) and the same cleaning rules.
# MAGIC If each notebook typed them separately, one typo or one change would break things.
# MAGIC
# MAGIC **Solution:** keep them in ONE place. Other notebooks load this one with `%run ./00_config`.
# MAGIC
# MAGIC *Analogy: one master address book that everyone in the office uses, instead of everyone keeping their own copy.*
# MAGIC
# MAGIC You never run this notebook on its own. The other notebooks run it for you.

# COMMAND ----------

from pyspark.sql import functions as F

# ---- WHERE things live (catalog = building, schema = floor) ----
CATALOG = "workspace"
SCHEMA = "claiminsight"

RAW_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw_files"        # original CSVs (uploaded by hand)
LANDING_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/mvp2_landing"  # new batches for MVP2 arrive here

INPATIENT_FILE = f"{RAW_VOLUME}/DE1_0_2008_to_2010_Inpatient_Claims_Sample_1.csv"
BENEFICIARY_FILE = f"{RAW_VOLUME}/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"


def t(name):
    """Full 3-part table name, e.g. t('bronze_inpatient') -> workspace.claiminsight.bronze_inpatient"""
    return f"{CATALOG}.{SCHEMA}.{name}"


# ---- The KEY we proved locally: one row = one segment of one claim ----
KEY_COLS = ["CLM_ID", "SEGMENT"]

# ---- Silver keeps only the columns we actually use, with the right types ----
DATE_COLS = ["CLM_FROM_DT", "CLM_THRU_DT", "CLM_ADMSN_DT", "NCH_BENE_DSCHRG_DT"]
MONEY_COLS = ["CLM_PMT_AMT", "NCH_PRMRY_PYR_CLM_PD_AMT"]
INT_COLS = ["SEGMENT", "CLM_UTLZTN_DAY_CNT"]
TEXT_COLS = ["DESYNPUF_ID", "CLM_ID", "PRVDR_NUM", "CLM_DRG_CD",
             "ADMTNG_ICD9_DGNS_CD", "ICD9_DGNS_CD_1"]

# The "business" columns: the facts about a claim segment (no load metadata)
BUSINESS_COLS = ["DESYNPUF_ID", "CLM_ID", "SEGMENT", "CLM_FROM_DT", "CLM_THRU_DT",
                 "CLM_ADMSN_DT", "NCH_BENE_DSCHRG_DT", "PRVDR_NUM", "CLM_PMT_AMT",
                 "NCH_PRMRY_PYR_CLM_PD_AMT", "CLM_UTLZTN_DAY_CNT", "CLM_DRG_CD",
                 "ADMTNG_ICD9_DGNS_CD", "ICD9_DGNS_CD_1"]


def to_silver_segments(raw_df):
    """Raw (all-text) inpatient rows -> typed Silver rows at segment grain.

    Used by BOTH 02_silver (first full load) and 04_mvp2 (new batches),
    so the cleaning rules are identical everywhere.
    """
    df = raw_df
    for c in DATE_COLS:
        df = df.withColumn(c, F.to_date(F.col(c), "yyyyMMdd"))        # "20090412" -> 2009-04-12
    for c in MONEY_COLS:
        df = df.withColumn(c, F.col(c).cast("decimal(12,2)"))          # "26000.00" -> 26000.00 (exact money)
    for c in INT_COLS:
        df = df.withColumn(c, F.col(c).cast("int"))                    # "1" -> 1
    for c in TEXT_COLS:
        df = df.withColumn(c, F.trim(F.col(c)))                        # remove stray spaces, keep as text

    df = add_row_hash(df.select(*BUSINESS_COLS, "source_file", "ingested_at"))
    return df


def add_row_hash(df):
    """Fingerprint of the business columns.

    Same facts -> same hash. A corrected amount -> different hash.
    MVP2's MERGE uses it to tell 'unchanged repeat' from 'real correction'.
    """
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in BUSINESS_COLS]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def count_key_duplicates(df, key_cols):
    """How many key values appear on more than one row? Must be 0 for a valid key.

    Spark version of the pandas check: df.duplicated(subset=[...]).sum()
    """
    return df.groupBy(*key_cols).count().filter("count > 1").count()


print(f"Config loaded: {CATALOG}.{SCHEMA}")
