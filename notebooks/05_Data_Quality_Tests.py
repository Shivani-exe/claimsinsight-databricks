# Databricks notebook source
# Validate existing tables using SELECT statements only; never rerun ingestion here.
# The SQL runner is injectable so the same checks work in Spark or the Statement API.
from decimal import Decimal
import json
import re

# COMMAND ----------
# Keep the catalog/schema configurable while preventing SQL identifier injection.
NAMESPACE = "workspace.claimsinsight"

# Compare exact decimal values rather than converting payment totals to float.
def equal_values(measured, expected):
    if measured is None or expected is None:
        return measured is expected
    if isinstance(expected, (int, Decimal)) and not isinstance(expected, bool):
        try:
            return Decimal(str(measured)) == Decimal(str(expected))
        except (ValueError, ArithmeticError):
            return False
    return measured == expected

# Accumulate failures so every executable check appears before the notebook fails.
def record(results, name, measured, expected):
    results.append({"test_name": name,
                    "status": "PASS" if equal_values(measured, expected) else "FAIL",
                    "measured": str(measured), "expected": str(expected)})

# Quote only a validated two-part namespace; all table/column names below are fixed.
def table_names(namespace=NAMESPACE):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", namespace):
        raise ValueError("Use a catalog.schema namespace with simple identifiers")
    return {name: f"{namespace}.{name}" for name in CONTRACTS}

# Read metadata from the live catalog, rather than inferring types from samples.
def check_schema(execute, results, namespace=NAMESPACE):
    table_names(namespace)
    catalog, schema = namespace.split(".")
    rows = execute(f"SELECT table_name, column_name, full_data_type FROM {catalog}.information_schema.columns WHERE table_schema = '{schema}'")
    actual = {}
    for row in rows:
        actual.setdefault(row['table_name'], {})[row['column_name']] = row['full_data_type'].lower()
    for name, columns in CONTRACTS.items():
        record(results, f"schema.{name}.exists", name in actual, True)
        for column, datatype in columns.items():
            record(results, f"schema.{name}.{column}", actual.get(name, {}).get(column), datatype)
    return all(r['status'] == 'PASS' for r in results)

# Package each aggregate query with its named reference expectations.
def check_source_integrity(t):
    return [
        ("source.inpatient", f"""SELECT COUNT(*) AS rows,
          COUNT_IF(CLM_ID IS NULL OR TRIM(CLM_ID) = '') AS missing_claim_id,
          COUNT_IF(DESYNPUF_ID IS NULL OR TRIM(DESYNPUF_ID) = '') AS missing_beneficiary_id,
          COUNT_IF(SEGMENT IS NULL OR TRIM(SEGMENT) = '') AS missing_segment
          FROM {t['bronze_inpatient']}""",
         {"rows": 66773, "missing_claim_id": 0, "missing_beneficiary_id": 0, "missing_segment": 0}),
        ("source.beneficiary", f"""SELECT COUNT(*) AS rows,
          COUNT_IF(DESYNPUF_ID IS NULL OR TRIM(DESYNPUF_ID) = '') AS missing_id,
          COUNT(*) - COUNT(DISTINCT DESYNPUF_ID) AS duplicate_or_null_ids
          FROM {t['bronze_beneficiary_2008']}""",
         {"rows": 116352, "missing_id": 0, "duplicate_or_null_ids": 0})]

# Check segment grain without incorrectly treating Segment 2 as a standalone claim.
def check_uniqueness(t):
    s = t['silver_inpatient_segments']
    c = t['silver_inpatient_claims_2008']
    return [
        ("grain.segments", f"""SELECT COUNT(*) AS rows,
          COUNT(DISTINCT claim_id) AS claim_ids,
          COUNT(*) - COUNT(DISTINCT STRUCT(claim_id, segment)) AS repeated_pairs,
          COUNT_IF(claim_id IS NULL OR TRIM(claim_id) = '' OR beneficiary_id IS NULL
            OR TRIM(beneficiary_id) = '' OR segment IS NULL) AS missing_identifiers,
          COUNT_IF(segment = 1) AS segment1, COUNT_IF(segment = 2) AS segment2,
          COUNT_IF(segment NOT IN (1,2)) AS other_segments FROM {s}""",
         {"rows": 66773, "claim_ids": 66705, "repeated_pairs": 0,
          "missing_identifiers": 0, "segment1": 66705, "segment2": 68, "other_segments": 0}),
        ("grain.multisegment", f"""SELECT COUNT_IF(n = 2) AS two_segments,
          COUNT_IF(n > 2) AS more_than_two FROM
          (SELECT claim_id, COUNT(*) n FROM {s} GROUP BY claim_id)""",
         {"two_segments": 68, "more_than_two": 0}),
        ("grain.reporting", f"""SELECT COUNT(*) - COUNT(DISTINCT claim_id) AS repeated_ids,
          COUNT_IF(claim_id IS NULL OR TRIM(claim_id) = '' OR beneficiary_id IS NULL
            OR TRIM(beneficiary_id) = '') AS missing_ids,
          COUNT_IF(segment IS NULL OR segment != 1) AS unexpected_segment FROM {c}""",
         {"repeated_ids": 0, "missing_ids": 0, "unexpected_segment": 0}),
        ("grain.exclusion", f"""SELECT COUNT(*) AS multisegment_in_reporting FROM {c}
          INNER JOIN (SELECT claim_id FROM {s} GROUP BY claim_id HAVING COUNT(*) > 1) m
          USING (claim_id)""", {"multisegment_in_reporting": 0})]

# Compare full multisets in both directions to catch lost, extra, or changed rows.
def multiset_query(left, right):
    return f"""WITH expected AS ({left}), actual AS ({right}),
      missing AS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual),
      extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)
      SELECT (SELECT COUNT(*) FROM missing) AS missing,
             (SELECT COUNT(*) FROM extra) AS extra"""

# Reconstruct the documented cohort without filtering missing payments out of the test.
def check_cohort(t):
    s = t['silver_inpatient_segments']
    fields = ', '.join(CONTRACTS['silver_inpatient_claims_2008'])
    expected = f"""SELECT {fields} FROM {s} WHERE YEAR(claim_through_date) = 2008
      AND claim_id IN (SELECT claim_id FROM {s} GROUP BY claim_id HAVING COUNT(*) = 1)"""
    actual = f"SELECT {fields} FROM {t['silver_inpatient_claims_2008']}"
    return [("cohort.complete_and_unchanged", multiset_query(expected, actual), {"missing": 0, "extra": 0})]

# Confirm raw dates parse and exactly match Silver, including preserved nulls.
def check_date_validity(t):
    b, s, c = t['bronze_inpatient'], t['silver_inpatient_segments'], t['silver_inpatient_claims_2008']
    checks = [
        ("dates.reporting", f"""SELECT COUNT_IF(claim_through_date IS NULL OR
          YEAR(claim_through_date) != 2008) AS invalid_reporting_dates FROM {c}""",
         {"invalid_reporting_dates": 0}),
        ("dates.segment2", f"""SELECT COUNT_IF(segment = 2 AND claim_from_date IS NULL
          AND claim_through_date IS NULL) AS documented_missing_dates FROM {s}""",
         {"documented_missing_dates": 68}),
        ("segments.same_beneficiary", f"""SELECT COUNT(*) AS unmatched_segment2 FROM {s} s2
          LEFT ANTI JOIN {s} s1 ON s2.claim_id = s1.claim_id AND s2.beneficiary_id = s1.beneficiary_id
          AND s1.segment = 1 WHERE s2.segment = 2""", {"unmatched_segment2": 0})]
    for raw, parsed in [('CLM_FROM_DT','claim_from_date'), ('CLM_THRU_DT','claim_through_date')]:
        checks.append((f"dates.{raw}.parse", f"""SELECT COUNT_IF(NULLIF(TRIM({raw}), '') IS NOT NULL
          AND (TRY_TO_TIMESTAMP({raw}, 'yyyyMMdd') IS NULL OR
          DATE_FORMAT(TRY_TO_TIMESTAMP({raw}, 'yyyyMMdd'), 'yyyyMMdd') != {raw})) AS invalid_nonempty
          FROM {b}""", {"invalid_nonempty": 0}))
    # Validate claim dates, payments, first diagnosis, IDs, and provenance end to end.
    expected = f"""SELECT CLM_ID claim_id, DESYNPUF_ID beneficiary_id, TRY_CAST(SEGMENT AS INT) segment,
      CAST(TRY_TO_TIMESTAMP(CLM_FROM_DT, 'yyyyMMdd') AS DATE) claim_from_date,
      CAST(TRY_TO_TIMESTAMP(CLM_THRU_DT, 'yyyyMMdd') AS DATE) claim_through_date,
      TRY_CAST(CLM_PMT_AMT AS DECIMAL(18,2)) paid_amount, PRVDR_NUM provider_id,
      ICD9_DGNS_CD_1 diagnosis_code, ingested_at, source_file FROM {b}"""
    actual = f"SELECT {', '.join(CONTRACTS['silver_inpatient_segments'])} FROM {s}"
    checks.append(("source_to_silver.exact", multiset_query(expected, actual), {"missing": 0, "extra": 0}))
    # A nonempty malformed payment must fail, even if both layers happen to contain null.
    checks.append(("source.payment_parse", f"""SELECT COUNT_IF(NULLIF(TRIM(CLM_PMT_AMT),'') IS NOT NULL
      AND TRY_CAST(CLM_PMT_AMT AS DECIMAL(18,2)) IS NULL) AS invalid_payment FROM {b}""", {"invalid_payment": 0}))
    return checks

# Confirm beneficiary grain, linkage, population preservation, and parsed dates.
def check_beneficiary_linkage(t):
    b, s, c = t['bronze_beneficiary_2008'], t['silver_beneficiaries_2008'], t['silver_inpatient_claims_2008']
    expected = f"""SELECT DESYNPUF_ID beneficiary_id,
      CAST(TRY_TO_TIMESTAMP(BENE_BIRTH_DT, 'yyyyMMdd') AS DATE) birth_date,
      CAST(TRY_TO_TIMESTAMP(BENE_DEATH_DT, 'yyyyMMdd') AS DATE) death_date,
      BENE_SEX_IDENT_CD sex_code, BENE_RACE_CD race_code, SP_STATE_CODE state_code,
      BENE_COUNTY_CD county_code, ingested_at, source_file FROM {b}"""
    return [
        ("beneficiary.silver", f"""SELECT COUNT(*) AS rows,
          COUNT(*) - COUNT(DISTINCT beneficiary_id) AS duplicate_or_null_ids,
          COUNT_IF(beneficiary_id IS NULL OR TRIM(beneficiary_id) = '') AS missing_id FROM {s}""",
         {"rows": 116352, "duplicate_or_null_ids": 0, "missing_id": 0}),
        ("beneficiary.linkage", f"""SELECT COUNT(*) AS unmatched_claims FROM {c}
          LEFT ANTI JOIN {s} USING (beneficiary_id)""", {"unmatched_claims": 0}),
        ("beneficiary.source_preservation", multiset_query(expected,
         f"SELECT {', '.join(CONTRACTS['silver_beneficiaries_2008'])} FROM {s}"), {"missing": 0, "extra": 0}),
        ("beneficiary.date_parse", f"""SELECT
          COUNT_IF(NULLIF(TRIM(BENE_BIRTH_DT),'') IS NOT NULL AND TRY_TO_TIMESTAMP(BENE_BIRTH_DT,'yyyyMMdd') IS NULL) AS invalid_birth,
          COUNT_IF(NULLIF(TRIM(BENE_DEATH_DT),'') IS NOT NULL AND TRY_TO_TIMESTAMP(BENE_DEATH_DT,'yyyyMMdd') IS NULL) AS invalid_death
          FROM {b}""", {"invalid_birth": 0, "invalid_death": 0})]

# Keep negative/zero payments and verify the signed net total against references.
def check_financials(t):
    c = t['silver_inpatient_claims_2008']
    return [("financial.silver", f"""SELECT COUNT(*) AS claims,
      COUNT_IF(paid_amount IS NULL) AS missing_payment,
      COUNT_IF(paid_amount < 0) AS negative, COUNT_IF(paid_amount = 0) AS zero,
      COUNT_IF(paid_amount > 0) AS positive, SUM(paid_amount) AS net_payment,
      COUNT(DISTINCT beneficiary_id) AS beneficiaries,
      SUM(paid_amount) - (SUM(CASE WHEN paid_amount < 0 THEN paid_amount ELSE 0 END)
       + SUM(CASE WHEN paid_amount > 0 THEN paid_amount ELSE 0 END)) AS signed_partition_difference
      FROM {c}""", {"claims": 27475, "missing_payment": 0, "negative": 22,
      "zero": 1039, "positive": 26414, "net_payment": Decimal('254560980.00'),
      "beneficiaries": 15740, "signed_partition_difference": Decimal('0.00')})]

# Reconcile every Gold group, not only grand totals that can hide offsetting errors.
def reconcile_totals(t):
    c = t['silver_inpatient_claims_2008']
    models = [
        ('monthly', 'gold_monthly_claims_2008', 'claim_month', "DATE_TRUNC('MONTH', claim_through_date)", 12, True),
        ('beneficiary', 'gold_beneficiary_spending_2008', 'beneficiary_id', 'beneficiary_id', 15740, False),
        ('diagnosis', 'gold_diagnosis_spending_2008', 'diagnosis_code', "COALESCE(NULLIF(TRIM(diagnosis_code), ''), 'UNKNOWN')", 2010, True)]
    checks = []
    for label, name, key, expression, rows, has_distinct in models:
        gold = t[name]
        checks.append((f"gold.{label}.grain_and_totals", f"""SELECT COUNT(*) AS rows,
          COUNT(*) - COUNT(DISTINCT {key}) AS duplicate_or_null_keys,
          COUNT_IF({key} IS NULL) AS missing_key,
          COUNT_IF(total_claims IS NULL OR total_claims <= 0 OR total_paid_amount IS NULL) AS invalid_measures,
          SUM(total_claims) - (SELECT COUNT(*) FROM {c}) AS claim_difference,
          SUM(total_paid_amount) - (SELECT SUM(paid_amount) FROM {c}) AS payment_difference
          FROM {gold}""", {"rows": rows, "duplicate_or_null_keys": 0, "missing_key": 0,
          "invalid_measures": 0, "claim_difference": 0, "payment_difference": Decimal('0.00')}))
        # Match averages with the original ROUND(..., 2) policy and maximum for beneficiaries.
        measures = "COUNT(*) total_claims, ROUND(SUM(paid_amount),2) total_paid_amount, ROUND(AVG(paid_amount),2) average_paid_amount"
        columns = f"{key}, total_claims, total_paid_amount, average_paid_amount"
        if has_distinct:
            measures += ", COUNT(DISTINCT beneficiary_id) unique_beneficiaries"
            columns += ", unique_beneficiaries"
        else:
            measures += ", MAX(paid_amount) highest_claim_payment"
            columns += ", highest_claim_payment"
        expected = f"SELECT {expression} {key}, {measures} FROM {c} GROUP BY {expression}"
        checks.append((f"gold.{label}.every_group", multiset_query(expected,
                       f"SELECT {columns} FROM {gold}"), {"missing": 0, "extra": 0}))
    # Monthly beneficiary counts overlap, so reconcile only beneficiary Gold rows to distinct Silver IDs.
    checks.append(("gold.beneficiary.population", f"""SELECT
      (SELECT COUNT(*) FROM {t['gold_beneficiary_spending_2008']}) -
      (SELECT COUNT(DISTINCT beneficiary_id) FROM {c}) AS difference""", {"difference": 0}))
    return checks

# Assemble independent aggregates once; no table is collected to the Python driver.
def read_existing_tables(namespace=NAMESPACE):
    t = table_names(namespace)
    return (check_source_integrity(t) + check_uniqueness(t) + check_cohort(t)
            + check_date_validity(t) + check_beneficiary_linkage(t)
            + check_financials(t) + reconcile_totals(t))

# Catch query errors as failures and continue so diagnostics are available for all checks.
def run_tests(execute, namespace=NAMESPACE):
    results = []
    try:
        schema_ok = check_schema(execute, results, namespace)
    except Exception as error:
        record(results, 'schema.execution', type(error).__name__ + ': ' + str(error), 'successful metadata query')
        schema_ok = False
    if not schema_ok:
        return results
    for label, sql, expected in read_existing_tables(namespace):
        try:
            rows = execute(sql)
            if len(rows) != 1:
                raise ValueError('Expected one aggregate result row')
            for metric, reference in expected.items():
                record(results, f'{label}.{metric}', rows[0][metric], reference)
        except Exception as error:
            record(results, label + '.execution', type(error).__name__ + ': ' + str(error), 'successful query')
    return results

# Show measured and expected values before raising an assertion for any failed check.
def produce_summary(results, display_rows=None):
    if display_rows:
        display_rows(results)
    else:
        print(json.dumps(results, indent=2))
    failures = [r for r in results if r['status'] != 'PASS']
    if not results or failures:
        raise AssertionError(f'{len(failures)} failed checks; inspect the summary')
    return results

# COMMAND ----------
# CONTRACTS is populated from live Unity Catalog metadata at preparation time.
# Extra columns are allowed, but every observed required column/type must remain present.
CONTRACTS = {'bronze_beneficiary_2008': {'DESYNPUF_ID': 'string',
                             'BENE_BIRTH_DT': 'string',
                             'BENE_DEATH_DT': 'string',
                             'BENE_SEX_IDENT_CD': 'string',
                             'BENE_RACE_CD': 'string',
                             'BENE_ESRD_IND': 'string',
                             'SP_STATE_CODE': 'string',
                             'BENE_COUNTY_CD': 'string',
                             'BENE_HI_CVRAGE_TOT_MONS': 'string',
                             'BENE_SMI_CVRAGE_TOT_MONS': 'string',
                             'BENE_HMO_CVRAGE_TOT_MONS': 'string',
                             'PLAN_CVRG_MOS_NUM': 'string',
                             'SP_ALZHDMTA': 'string',
                             'SP_CHF': 'string',
                             'SP_CHRNKIDN': 'string',
                             'SP_CNCR': 'string',
                             'SP_COPD': 'string',
                             'SP_DEPRESSN': 'string',
                             'SP_DIABETES': 'string',
                             'SP_ISCHMCHT': 'string',
                             'SP_OSTEOPRS': 'string',
                             'SP_RA_OA': 'string',
                             'SP_STRKETIA': 'string',
                             'MEDREIMB_IP': 'string',
                             'BENRES_IP': 'string',
                             'PPPYMT_IP': 'string',
                             'MEDREIMB_OP': 'string',
                             'BENRES_OP': 'string',
                             'PPPYMT_OP': 'string',
                             'MEDREIMB_CAR': 'string',
                             'BENRES_CAR': 'string',
                             'PPPYMT_CAR': 'string',
                             'ingested_at': 'timestamp',
                             'source_file': 'string'},
 'bronze_inpatient': {'DESYNPUF_ID': 'string',
                      'CLM_ID': 'string',
                      'SEGMENT': 'string',
                      'CLM_FROM_DT': 'string',
                      'CLM_THRU_DT': 'string',
                      'PRVDR_NUM': 'string',
                      'CLM_PMT_AMT': 'string',
                      'NCH_PRMRY_PYR_CLM_PD_AMT': 'string',
                      'AT_PHYSN_NPI': 'string',
                      'OP_PHYSN_NPI': 'string',
                      'OT_PHYSN_NPI': 'string',
                      'CLM_ADMSN_DT': 'string',
                      'ADMTNG_ICD9_DGNS_CD': 'string',
                      'CLM_PASS_THRU_PER_DIEM_AMT': 'string',
                      'NCH_BENE_IP_DDCTBL_AMT': 'string',
                      'NCH_BENE_PTA_COINSRNC_LBLTY_AM': 'string',
                      'NCH_BENE_BLOOD_DDCTBL_LBLTY_AM': 'string',
                      'CLM_UTLZTN_DAY_CNT': 'string',
                      'NCH_BENE_DSCHRG_DT': 'string',
                      'CLM_DRG_CD': 'string',
                      'ICD9_DGNS_CD_1': 'string',
                      'ICD9_DGNS_CD_2': 'string',
                      'ICD9_DGNS_CD_3': 'string',
                      'ICD9_DGNS_CD_4': 'string',
                      'ICD9_DGNS_CD_5': 'string',
                      'ICD9_DGNS_CD_6': 'string',
                      'ICD9_DGNS_CD_7': 'string',
                      'ICD9_DGNS_CD_8': 'string',
                      'ICD9_DGNS_CD_9': 'string',
                      'ICD9_DGNS_CD_10': 'string',
                      'ICD9_PRCDR_CD_1': 'string',
                      'ICD9_PRCDR_CD_2': 'string',
                      'ICD9_PRCDR_CD_3': 'string',
                      'ICD9_PRCDR_CD_4': 'string',
                      'ICD9_PRCDR_CD_5': 'string',
                      'ICD9_PRCDR_CD_6': 'string',
                      'HCPCS_CD_1': 'string',
                      'HCPCS_CD_2': 'string',
                      'HCPCS_CD_3': 'string',
                      'HCPCS_CD_4': 'string',
                      'HCPCS_CD_5': 'string',
                      'HCPCS_CD_6': 'string',
                      'HCPCS_CD_7': 'string',
                      'HCPCS_CD_8': 'string',
                      'HCPCS_CD_9': 'string',
                      'HCPCS_CD_10': 'string',
                      'HCPCS_CD_11': 'string',
                      'HCPCS_CD_12': 'string',
                      'HCPCS_CD_13': 'string',
                      'HCPCS_CD_14': 'string',
                      'HCPCS_CD_15': 'string',
                      'HCPCS_CD_16': 'string',
                      'HCPCS_CD_17': 'string',
                      'HCPCS_CD_18': 'string',
                      'HCPCS_CD_19': 'string',
                      'HCPCS_CD_20': 'string',
                      'HCPCS_CD_21': 'string',
                      'HCPCS_CD_22': 'string',
                      'HCPCS_CD_23': 'string',
                      'HCPCS_CD_24': 'string',
                      'HCPCS_CD_25': 'string',
                      'HCPCS_CD_26': 'string',
                      'HCPCS_CD_27': 'string',
                      'HCPCS_CD_28': 'string',
                      'HCPCS_CD_29': 'string',
                      'HCPCS_CD_30': 'string',
                      'HCPCS_CD_31': 'string',
                      'HCPCS_CD_32': 'string',
                      'HCPCS_CD_33': 'string',
                      'HCPCS_CD_34': 'string',
                      'HCPCS_CD_35': 'string',
                      'HCPCS_CD_36': 'string',
                      'HCPCS_CD_37': 'string',
                      'HCPCS_CD_38': 'string',
                      'HCPCS_CD_39': 'string',
                      'HCPCS_CD_40': 'string',
                      'HCPCS_CD_41': 'string',
                      'HCPCS_CD_42': 'string',
                      'HCPCS_CD_43': 'string',
                      'HCPCS_CD_44': 'string',
                      'HCPCS_CD_45': 'string',
                      'ingested_at': 'timestamp',
                      'source_file': 'string'},
 'gold_beneficiary_spending_2008': {'beneficiary_id': 'string',
                                    'total_claims': 'bigint',
                                    'total_paid_amount': 'decimal(29,2)',
                                    'average_paid_amount': 'decimal(19,2)',
                                    'highest_claim_payment': 'decimal(18,2)'},
 'gold_diagnosis_spending_2008': {'diagnosis_code': 'string',
                                  'total_claims': 'bigint',
                                  'unique_beneficiaries': 'bigint',
                                  'total_paid_amount': 'decimal(29,2)',
                                  'average_paid_amount': 'decimal(19,2)'},
 'gold_monthly_claims_2008': {'claim_month': 'timestamp',
                              'total_claims': 'bigint',
                              'unique_beneficiaries': 'bigint',
                              'total_paid_amount': 'decimal(29,2)',
                              'average_paid_amount': 'decimal(19,2)'},
 'silver_beneficiaries_2008': {'beneficiary_id': 'string',
                               'birth_date': 'date',
                               'death_date': 'date',
                               'sex_code': 'string',
                               'race_code': 'string',
                               'state_code': 'string',
                               'county_code': 'string',
                               'ingested_at': 'timestamp',
                               'source_file': 'string'},
 'silver_inpatient_claims_2008': {'claim_id': 'string',
                                  'beneficiary_id': 'string',
                                  'segment': 'int',
                                  'claim_from_date': 'date',
                                  'claim_through_date': 'date',
                                  'paid_amount': 'decimal(18,2)',
                                  'provider_id': 'string',
                                  'diagnosis_code': 'string',
                                  'ingested_at': 'timestamp',
                                  'source_file': 'string'},
 'silver_inpatient_segments': {'claim_id': 'string',
                               'beneficiary_id': 'string',
                               'segment': 'int',
                               'claim_from_date': 'date',
                               'claim_through_date': 'date',
                               'paid_amount': 'decimal(18,2)',
                               'provider_id': 'string',
                               'diagnosis_code': 'string',
                               'ingested_at': 'timestamp',
                               'source_file': 'string'}}

# COMMAND ----------
# Databricks injects spark/display; importing locally deliberately performs no live queries.
if 'spark' in globals():
    # Collect only the small metadata/aggregate result sets, never source claim rows.
    def execute_spark(sql):
        return [row.asDict() for row in spark.sql(sql).collect()]

    # Run the read-only checks and fail the notebook after displaying the full summary.
    test_results = run_tests(execute_spark)
    produce_summary(test_results, lambda rows: display(spark.createDataFrame(rows)))
