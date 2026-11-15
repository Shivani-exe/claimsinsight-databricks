-- Databricks notebook source
-- Exported original MVP1 cell 1; review write operations before reproduction.

-- Step 6A: Create monthly inpatient expenditure analytics.
-- Source: Validated single-segment inpatient claims from 2008.
-- Grain: One row per claim-through month.
-- Important: Total payments are net of negative payment amounts.

CREATE OR REPLACE TABLE
workspace.claimsinsight.gold_monthly_claims_2008 AS

SELECT
    DATE_TRUNC('MONTH', claim_through_date)
        AS claim_month,

    COUNT(*) AS total_claims,

    COUNT(DISTINCT beneficiary_id)
        AS unique_beneficiaries,

    ROUND(SUM(paid_amount), 2)
        AS total_paid_amount,

    ROUND(AVG(paid_amount), 2)
        AS average_paid_amount

FROM workspace.claimsinsight.silver_inpatient_claims_2008

GROUP BY
    DATE_TRUNC('MONTH', claim_through_date);

-- COMMAND ----------

-- Exported original MVP1 cell 2; review write operations before reproduction.
-- Step 6B: Inspect the monthly expenditure results.
-- Sort by month so trends can be reviewed chronologically.

SELECT *
FROM workspace.claimsinsight.gold_monthly_claims_2008
ORDER BY claim_month;


-- COMMAND ----------

-- Exported original MVP1 cell 3; review write operations before reproduction.
-- Step 6C: Reconcile the Gold totals against the Silver source.
-- The claim counts and payment amounts should match.
-- This ensures monthly aggregation has not lost or duplicated payments.

SELECT
    (SELECT COUNT(*)
     FROM workspace.claimsinsight.silver_inpatient_claims_2008)
        AS silver_claims,

    (SELECT SUM(total_claims)
     FROM workspace.claimsinsight.gold_monthly_claims_2008)
        AS gold_claims,

    (SELECT SUM(paid_amount)
     FROM workspace.claimsinsight.silver_inpatient_claims_2008)
        AS silver_total_paid,

    (SELECT SUM(total_paid_amount)
     FROM workspace.claimsinsight.gold_monthly_claims_2008)
        AS gold_total_paid;


-- COMMAND ----------

-- Exported original MVP1 cell 4; review write operations before reproduction.
-- Step 6B: Create the beneficiary spending Gold table.
-- Grain: One row per beneficiary.
-- Source: Single-segment inpatient claims from 2008.
-- Keep negative and zero payments in the net totals.

CREATE OR REPLACE TABLE
workspace.claimsinsight.gold_beneficiary_spending_2008 AS

SELECT
    beneficiary_id,

    COUNT(*) AS total_claims,

    ROUND(SUM(paid_amount), 2)
        AS total_paid_amount,

    ROUND(AVG(paid_amount), 2)
        AS average_paid_amount,

    MAX(paid_amount)
        AS highest_claim_payment

FROM workspace.claimsinsight.silver_inpatient_claims_2008

GROUP BY beneficiary_id;


-- COMMAND ----------

-- Exported original MVP1 cell 5; review write operations before reproduction.
-- Step 6C: Identify the top 10 beneficiaries.
-- Higher net payment amounts appear first.

SELECT
    beneficiary_id,
    total_claims,
    total_paid_amount,
    average_paid_amount,
    highest_claim_payment

FROM workspace.claimsinsight.gold_beneficiary_spending_2008

ORDER BY total_paid_amount DESC
LIMIT 10;


-- COMMAND ----------

-- Exported original MVP1 cell 6; review write operations before reproduction.
-- Step 6D: Reconcile beneficiary Gold totals to Silver.
-- Both claim counts and net payment totals should match.

SELECT
    SUM(total_claims) AS gold_claims,
    SUM(total_paid_amount) AS gold_total_paid,
    COUNT(*) AS beneficiaries_with_claims

FROM workspace.claimsinsight.gold_beneficiary_spending_2008;


-- COMMAND ----------

-- Exported original MVP1 cell 7; review write operations before reproduction.
-- Step 6C: Create the diagnosis spending Gold table.
-- Grain: One row per first diagnosis code.
-- Source: Validated 2008 single-segment inpatient claims.
-- NULL diagnosis codes are retained as an unknown category.

CREATE OR REPLACE TABLE
workspace.claimsinsight.gold_diagnosis_spending_2008 AS

SELECT
    COALESCE(
        NULLIF(TRIM(diagnosis_code), ''),
        'UNKNOWN'
    ) AS diagnosis_code,

    COUNT(*) AS total_claims,

    COUNT(DISTINCT beneficiary_id)
        AS unique_beneficiaries,

    ROUND(SUM(paid_amount), 2)
        AS total_paid_amount,

    ROUND(AVG(paid_amount), 2)
        AS average_paid_amount

FROM workspace.claimsinsight.silver_inpatient_claims_2008

GROUP BY
    COALESCE(
        NULLIF(TRIM(diagnosis_code), ''),
        'UNKNOWN'
    );


-- COMMAND ----------

-- Exported original MVP1 cell 8; review write operations before reproduction.
-- Step 6D: Display the 10 diagnosis codes associated
-- with the highest net claim payments.

SELECT
    diagnosis_code,
    total_claims,
    unique_beneficiaries,
    total_paid_amount,
    average_paid_amount

FROM workspace.claimsinsight.gold_diagnosis_spending_2008

ORDER BY total_paid_amount DESC
LIMIT 10;


-- COMMAND ----------

-- Exported original MVP1 cell 9; review write operations before reproduction.
-- Step 6E: Reconcile the diagnosis Gold table with Silver.
-- Every claim should contribute to exactly one diagnosis group.

SELECT
    SUM(total_claims) AS gold_claims,

    SUM(total_paid_amount)
        AS gold_total_paid,

    COUNT(*) AS diagnosis_categories

FROM workspace.claimsinsight.gold_diagnosis_spending_2008;
