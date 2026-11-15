# Architecture and reproduction boundaries

Databricks Free Edition provides the workspace, serverless execution, Unity Catalog, Delta tables, and AI/BI dashboard. No external orchestrator, streaming service, or additional cloud infrastructure is part of MVP1.

## Data flow

1. CMS Sample 1 CSVs are uploaded into `/Volumes/workspace/claimsinsight/raw_files/`.
2. Notebook 01 reads headers with schema inference disabled, preserving original fields as strings. Bronze adds `ingested_at` and a literal `source_file` path.
3. Notebook 02 profiles duplicates, claim dates, segments, and payments. Repeated claim IDs are distinguished from duplicate claim/segment combinations.
4. Notebook 03 casts segment numbers, parses `yyyyMMdd` dates, casts payments with `try_cast`, and preserves all source claim segments. It separately produces typed beneficiaries and the single-segment, claim-through-2008 reporting cohort.
5. Notebook 04 creates three Gold aggregates from that reporting cohort. Diagnosis aggregation normalizes null/blank first codes to `UNKNOWN`.
6. The existing dashboard queries Gold. Step 8 does not modify its definition.
7. Notebook 05 reads all layers, verifies metadata and data contracts, and fails if checks fail.

## Data grains and dependencies

`bronze_inpatient` → `silver_inpatient_segments` → `silver_inpatient_claims_2008` → all three Gold models.

`bronze_beneficiary_2008` → `silver_beneficiaries_2008`, linked to the reporting cohort on `beneficiary_id`. Beneficiary joins are validation checks; the cohort is not filtered by join success. A future unmatched beneficiary therefore causes a visible failure instead of disappearing from the total.

All eight tables use the `workspace.claimsinsight` namespace. The test notebook accepts a validated `catalog.schema` argument in its functions; the Databricks auto-run cell uses `NAMESPACE`. Original pipeline notebooks retain their existing namespace and paths.

## Repository versus workspace

The repository contains source exports of notebooks 01–04 and the new notebook 05. It does not contain CSV files, claim row extracts, credentials, warehouse provisioning, or a dashboard export. Only schema and aggregate test results are saved as evidence.

Notebooks 01, 03, and 04 perform overwrite/replace writes when intentionally run for reproduction. Step 8 ran only SELECT statements against existing datasets. Importing or executing notebook 05 does not require rerunning the writers.

See the README for reproduction order and the data dictionary for actual inspected column types. The raw volume must already exist and contain the exact two filenames expected by notebook 01. Creating a parallel copy under different names requires reviewing the hardcoded original paths; this task did not redesign that pipeline.
