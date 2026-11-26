# CMS real-data pipeline (MVP1 + MVP2)

Runs the ClaimsInsight pipeline on the real DE-SynPUF inpatient data.
Uses its own schema (claimsinsight_v1), so existing tables are not touched.

Run order: 00_config (loaded by the others) → 01 → 02 → 03 → 04

MVP1
- 01 Bronze: raw CSVs as tables + lineage stamps
- 02 Silver: typed, key checked (CLM_ID + SEGMENT), quality gates
- 03 Gold: monthly / diagnosis / beneficiary totals, reconciled to Silver

MVP2 (04)
- Simulated batches: 2009, then 2010 with planted repeats, corrections and invalid rows
- Validation with rejection reasons → quarantine
- MERGE on CLM_ID + SEGMENT (update only when row_hash changes)
- Audit table per run + skip guard for already-loaded batches

Results: batch_002 → 13,572 inserted, 10 updated, 8 quarantined.
Rerun → skipped; forced rerun → 0 inserted, 0 updated.

Next: Auto Loader, version-based updates, Lakeflow Jobs.