# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold (business answers)
# MAGIC
# MAGIC **Problem:** analysts don't want 66,000 rows. They want answers:
# MAGIC *How much was paid each month? Which diagnoses cost the most? Which patients cost the most?*
# MAGIC
# MAGIC **Gold = the finished report.** Small summary tables built from Silver, ready for a dashboard.
# MAGIC
# MAGIC **Reconciliation:** the total money in Gold must equal the total in Silver.
# MAGIC *Analogy: after splitting a restaurant bill by person, the shares must add back up to the bill.*
# MAGIC
# MAGIC ⚠️ Synthetic data: these numbers do NOT represent real Medicare spending.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

claims = spark.table(t("silver_inpatient_claims_2008"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 1 · Monthly claims (grain: one row per month)

# COMMAND ----------

gold_monthly = (
    claims
    .withColumn("claim_month", F.trunc("CLM_FROM_DT", "month"))   # 2008-04-12 -> 2008-04-01
    .groupBy("claim_month")
    .agg(
        F.count("*").alias("claim_count"),
        F.sum("CLM_PMT_AMT").alias("total_paid"),
        F.round(F.avg("CLM_PMT_AMT"), 2).alias("avg_paid_per_claim"),
    )
    .orderBy("claim_month")
)
gold_monthly.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(t("gold_monthly_claims_2008"))
display(gold_monthly)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 2 · Spending by first diagnosis (grain: one row per diagnosis code)

# COMMAND ----------

gold_diagnosis = (
    claims
    .groupBy(F.col("ICD9_DGNS_CD_1").alias("first_diagnosis_code"))
    .agg(F.count("*").alias("claim_count"), F.sum("CLM_PMT_AMT").alias("total_paid"))
    .orderBy(F.desc("total_paid"))
)
gold_diagnosis.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(t("gold_diagnosis_spending_2008"))
display(gold_diagnosis.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold 3 · Spending per patient (grain: one row per patient)

# COMMAND ----------

gold_beneficiary = (
    claims
    .groupBy("DESYNPUF_ID")
    .agg(F.count("*").alias("claim_count"), F.sum("CLM_PMT_AMT").alias("total_paid"))
    .orderBy(F.desc("total_paid"))
)
gold_beneficiary.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(t("gold_beneficiary_spending_2008"))
display(gold_beneficiary.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliation (quality gate): every Gold table must add up to Silver

# COMMAND ----------

def total(df, col="total_paid"):
    return df.agg(F.sum(col)).collect()[0][0]

silver_total = total(claims, "CLM_PMT_AMT")
checks = {
    "monthly": total(spark.table(t("gold_monthly_claims_2008"))),
    "diagnosis": total(spark.table(t("gold_diagnosis_spending_2008"))),
    "beneficiary": total(spark.table(t("gold_beneficiary_spending_2008"))),
}

print("Silver total paid:", silver_total)
for name, value in checks.items():
    print(f"{name:12s}: {value}  {'OK' if value == silver_total else 'MISMATCH'}")
    assert value == silver_total, f"Gold {name} does not reconcile to Silver"
