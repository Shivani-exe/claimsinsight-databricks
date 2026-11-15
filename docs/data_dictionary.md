# Data dictionary

Inspected from live Unity Catalog metadata on November 15, 2026. All tables are Delta tables in `workspace.claimsinsight`. Source fields remain strings in Bronze; the two provenance fields are added by ingestion.

## Core field meanings

| Field | Meaning |
| --- | --- |
| `DESYNPUF_ID` / `beneficiary_id` | Synthetic beneficiary linking identifier; preserve as string |
| `CLM_ID` / `claim_id` | Source claim identifier; repeated IDs can represent multiple segments |
| `SEGMENT` / `segment` | Source segment label; string in Bronze, integer in Silver |
| `CLM_FROM_DT` / `claim_from_date` | Service-period start parsed from yyyyMMdd; not a payment date |
| `CLM_THRU_DT` / `claim_through_date` | Service-period end; determines the reporting year and month |
| `CLM_PMT_AMT` / `paid_amount` | Signed claim payment amount; negative and zero values are retained |
| `PRVDR_NUM` / `provider_id` | Source provider code, preserved as string |
| `ICD9_DGNS_CD_1` / `diagnosis_code` | First recorded diagnosis code; Gold maps null/blank to UNKNOWN |
| `birth_date`, `death_date` | Parsed beneficiary dates; missing death date is not replaced |
| `sex_code`, `race_code`, `state_code`, `county_code` | Preserved source codes; no descriptive dimensions added |
| `ingested_at` | Timestamp added at ingestion |
| `source_file` | Original raw-volume CSV path |
| `claim_month` | Timestamp truncated to first instant of the claim-through month |
| `total_claims` | Number of eligible claims in the group, not number of segments |
| `unique_beneficiaries` | Distinct beneficiaries within a month/diagnosis; not additive across groups |
| `total_paid_amount` | Signed net sum of eligible paid amounts |
| `average_paid_amount` | Group average rounded to two decimal places |
| `highest_claim_payment` | Maximum signed eligible claim payment for a beneficiary |

## Bronze source fields

The complete source column names and actual types are listed below. Additional diagnosis, procedure, HCPCS, physician, coverage, condition, and reimbursement fields are retained for traceability but are not all transformed or analyzed in MVP1. Consult the CMS codebook for their definitions; no unsupported interpretations are assigned here.

## `bronze_beneficiary_2008`

| Column | Actual type |
| --- | --- |
| `DESYNPUF_ID` | `string` |
| `BENE_BIRTH_DT` | `string` |
| `BENE_DEATH_DT` | `string` |
| `BENE_SEX_IDENT_CD` | `string` |
| `BENE_RACE_CD` | `string` |
| `BENE_ESRD_IND` | `string` |
| `SP_STATE_CODE` | `string` |
| `BENE_COUNTY_CD` | `string` |
| `BENE_HI_CVRAGE_TOT_MONS` | `string` |
| `BENE_SMI_CVRAGE_TOT_MONS` | `string` |
| `BENE_HMO_CVRAGE_TOT_MONS` | `string` |
| `PLAN_CVRG_MOS_NUM` | `string` |
| `SP_ALZHDMTA` | `string` |
| `SP_CHF` | `string` |
| `SP_CHRNKIDN` | `string` |
| `SP_CNCR` | `string` |
| `SP_COPD` | `string` |
| `SP_DEPRESSN` | `string` |
| `SP_DIABETES` | `string` |
| `SP_ISCHMCHT` | `string` |
| `SP_OSTEOPRS` | `string` |
| `SP_RA_OA` | `string` |
| `SP_STRKETIA` | `string` |
| `MEDREIMB_IP` | `string` |
| `BENRES_IP` | `string` |
| `PPPYMT_IP` | `string` |
| `MEDREIMB_OP` | `string` |
| `BENRES_OP` | `string` |
| `PPPYMT_OP` | `string` |
| `MEDREIMB_CAR` | `string` |
| `BENRES_CAR` | `string` |
| `PPPYMT_CAR` | `string` |
| `ingested_at` | `timestamp` |
| `source_file` | `string` |

## `bronze_inpatient`

| Column | Actual type |
| --- | --- |
| `DESYNPUF_ID` | `string` |
| `CLM_ID` | `string` |
| `SEGMENT` | `string` |
| `CLM_FROM_DT` | `string` |
| `CLM_THRU_DT` | `string` |
| `PRVDR_NUM` | `string` |
| `CLM_PMT_AMT` | `string` |
| `NCH_PRMRY_PYR_CLM_PD_AMT` | `string` |
| `AT_PHYSN_NPI` | `string` |
| `OP_PHYSN_NPI` | `string` |
| `OT_PHYSN_NPI` | `string` |
| `CLM_ADMSN_DT` | `string` |
| `ADMTNG_ICD9_DGNS_CD` | `string` |
| `CLM_PASS_THRU_PER_DIEM_AMT` | `string` |
| `NCH_BENE_IP_DDCTBL_AMT` | `string` |
| `NCH_BENE_PTA_COINSRNC_LBLTY_AM` | `string` |
| `NCH_BENE_BLOOD_DDCTBL_LBLTY_AM` | `string` |
| `CLM_UTLZTN_DAY_CNT` | `string` |
| `NCH_BENE_DSCHRG_DT` | `string` |
| `CLM_DRG_CD` | `string` |
| `ICD9_DGNS_CD_1` | `string` |
| `ICD9_DGNS_CD_2` | `string` |
| `ICD9_DGNS_CD_3` | `string` |
| `ICD9_DGNS_CD_4` | `string` |
| `ICD9_DGNS_CD_5` | `string` |
| `ICD9_DGNS_CD_6` | `string` |
| `ICD9_DGNS_CD_7` | `string` |
| `ICD9_DGNS_CD_8` | `string` |
| `ICD9_DGNS_CD_9` | `string` |
| `ICD9_DGNS_CD_10` | `string` |
| `ICD9_PRCDR_CD_1` | `string` |
| `ICD9_PRCDR_CD_2` | `string` |
| `ICD9_PRCDR_CD_3` | `string` |
| `ICD9_PRCDR_CD_4` | `string` |
| `ICD9_PRCDR_CD_5` | `string` |
| `ICD9_PRCDR_CD_6` | `string` |
| `HCPCS_CD_1` | `string` |
| `HCPCS_CD_2` | `string` |
| `HCPCS_CD_3` | `string` |
| `HCPCS_CD_4` | `string` |
| `HCPCS_CD_5` | `string` |
| `HCPCS_CD_6` | `string` |
| `HCPCS_CD_7` | `string` |
| `HCPCS_CD_8` | `string` |
| `HCPCS_CD_9` | `string` |
| `HCPCS_CD_10` | `string` |
| `HCPCS_CD_11` | `string` |
| `HCPCS_CD_12` | `string` |
| `HCPCS_CD_13` | `string` |
| `HCPCS_CD_14` | `string` |
| `HCPCS_CD_15` | `string` |
| `HCPCS_CD_16` | `string` |
| `HCPCS_CD_17` | `string` |
| `HCPCS_CD_18` | `string` |
| `HCPCS_CD_19` | `string` |
| `HCPCS_CD_20` | `string` |
| `HCPCS_CD_21` | `string` |
| `HCPCS_CD_22` | `string` |
| `HCPCS_CD_23` | `string` |
| `HCPCS_CD_24` | `string` |
| `HCPCS_CD_25` | `string` |
| `HCPCS_CD_26` | `string` |
| `HCPCS_CD_27` | `string` |
| `HCPCS_CD_28` | `string` |
| `HCPCS_CD_29` | `string` |
| `HCPCS_CD_30` | `string` |
| `HCPCS_CD_31` | `string` |
| `HCPCS_CD_32` | `string` |
| `HCPCS_CD_33` | `string` |
| `HCPCS_CD_34` | `string` |
| `HCPCS_CD_35` | `string` |
| `HCPCS_CD_36` | `string` |
| `HCPCS_CD_37` | `string` |
| `HCPCS_CD_38` | `string` |
| `HCPCS_CD_39` | `string` |
| `HCPCS_CD_40` | `string` |
| `HCPCS_CD_41` | `string` |
| `HCPCS_CD_42` | `string` |
| `HCPCS_CD_43` | `string` |
| `HCPCS_CD_44` | `string` |
| `HCPCS_CD_45` | `string` |
| `ingested_at` | `timestamp` |
| `source_file` | `string` |

## `gold_beneficiary_spending_2008`

| Column | Actual type |
| --- | --- |
| `beneficiary_id` | `string` |
| `total_claims` | `bigint` |
| `total_paid_amount` | `decimal(29,2)` |
| `average_paid_amount` | `decimal(19,2)` |
| `highest_claim_payment` | `decimal(18,2)` |

## `gold_diagnosis_spending_2008`

| Column | Actual type |
| --- | --- |
| `diagnosis_code` | `string` |
| `total_claims` | `bigint` |
| `unique_beneficiaries` | `bigint` |
| `total_paid_amount` | `decimal(29,2)` |
| `average_paid_amount` | `decimal(19,2)` |

## `gold_monthly_claims_2008`

| Column | Actual type |
| --- | --- |
| `claim_month` | `timestamp` |
| `total_claims` | `bigint` |
| `unique_beneficiaries` | `bigint` |
| `total_paid_amount` | `decimal(29,2)` |
| `average_paid_amount` | `decimal(19,2)` |

## `silver_beneficiaries_2008`

| Column | Actual type |
| --- | --- |
| `beneficiary_id` | `string` |
| `birth_date` | `date` |
| `death_date` | `date` |
| `sex_code` | `string` |
| `race_code` | `string` |
| `state_code` | `string` |
| `county_code` | `string` |
| `ingested_at` | `timestamp` |
| `source_file` | `string` |

## `silver_inpatient_claims_2008`

| Column | Actual type |
| --- | --- |
| `claim_id` | `string` |
| `beneficiary_id` | `string` |
| `segment` | `int` |
| `claim_from_date` | `date` |
| `claim_through_date` | `date` |
| `paid_amount` | `decimal(18,2)` |
| `provider_id` | `string` |
| `diagnosis_code` | `string` |
| `ingested_at` | `timestamp` |
| `source_file` | `string` |

## `silver_inpatient_segments`

| Column | Actual type |
| --- | --- |
| `claim_id` | `string` |
| `beneficiary_id` | `string` |
| `segment` | `int` |
| `claim_from_date` | `date` |
| `claim_through_date` | `date` |
| `paid_amount` | `decimal(18,2)` |
| `provider_id` | `string` |
| `diagnosis_code` | `string` |
| `ingested_at` | `timestamp` |
| `source_file` | `string` |
