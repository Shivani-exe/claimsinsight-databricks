# Verification and evidence

## What was retrieved and executed

Repository preparation used read-only Databricks APIs to inspect seven Unity Catalog tables, export the five deployed CMS task notebooks and the CMS setup/generator notebook, retrieve the visual Job configuration, retrieve its completed run, and preserve the workspace bundle YAML. Source paths and UTF-8/LF hashes are in [the manifest](../evidence/source_manifest.json).

Thirteen read-only SQL query groups were executed against the existing serverless warehouse. Their SQL and aggregate results are retained in [live verification evidence](../evidence/live_verification.json). The pipeline was **not rerun**, and no notebook write operation, Job configuration change, table repair, or checkpoint reset was performed.

## Observed Lakeflow execution

Job ID: `352752500046244`. Run ID: `935354143644293`. The run began at **2026-11-27 05:42:19 UTC**, which is **November 26, 2026, 11:42:19 PM America/Chicago**. [Actual run evidence](../evidence/job_run.json) reports `TERMINATED / SUCCESS` and attempt number 0 for all five tasks:

| Task key, as deployed | Observed result |
| --- | --- |
| `ingest_claims` | SUCCESS |
| `stage_events` | SUCCESS |
| `classify_events` | SUCCESS |
| `process_claims` | SUCCESS |
| `ausit_claims` | SUCCESS |

The last key is a spelling error in the deployed Job, not a sixth task. The saved bundle YAML uses `audit_claims` and different notebook names. Both artifacts are preserved as evidence; no bundle deployment equivalence is claimed.

## Independent table checks

| Query group | Actual result |
| --- | --- |
| `table_counts` | Bronze, queue, decisions, and ledger: 38,921 each; Silver: 38,803; quarantine: 8; audits: 2 |
| `bronze_batches` | Batch 001: 25,231 deliveries and distinct event IDs; Batch 002: 13,690 each |
| `decision_outcomes` | Batch 001: 25,231 approved; Batch 002: 13,582 approved, 100 duplicate ignores, 5 missing-ID rejects, 3 payment rejects |
| `ledger_outcomes` | Batch 001: 25,231 MERGED; Batch 002: 13,582 MERGED, 100 IGNORED_DUPLICATE, 8 QUARANTINED |
| `quarantine_reasons` | Five missing-ID and three payment rejections; all three `NOT_A_NUMBER` values preserved |
| `silver_state` | 38,803 rows = 38,803 distinct claim-segment keys; 10 version-2 UPDATEs; no null payments; 33 negative and 1,103 zero payments |
| `audit_rows` | Both source files RECONCILED with the counts below |
| `delivery_grains` | No repeated or missing delivery keys in Bronze, queue, decisions, ledger, or quarantine |
| `backlogs` | Unstaged Bronze, unclassified queue, and unfinished decisions: 0 each |
| `ledger_reconciliation` | 0 decision-to-ledger outcome mismatches |
| `correction_payments` | 10 corrections matched original CMS keys; all are +$100; total change +$1,000.00 |
| `audit_reconciliation` | 0 source-file audit/ledger count mismatches |
| `source_years` | Original source: 66,773 rows; claim-start 2009: 25,231, 2010: 13,572; 68 missing start dates |

These actual results match the supplied scenario expectations: 38,921 total deliveries, 38,803 current Silver segments, 10 corrections, 100 duplicate ignores, and 8 quarantined deliveries. Expectations were not substituted for measurements or used to alter data.

## Batch audit snapshots

| Source file | Incoming | Merged deliveries | Quarantined | Ignored | Whole-Silver rows | Whole-Silver signed payment state |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `cms_batch_001.csv` | 25,231 | 25,231 | 0 | 0 | 25,231 | $244,810,270.00 |
| `cms_batch_002.csv` | 13,690 | 13,582 | 8 | 100 | 38,803 | $377,480,600.00 |

The Batch 001 audit is a historical snapshot, while current Silver includes Batch 002. `merged_count` is derived from approved decisions/final ledger outcomes, not Delta's native inserted/updated operation metrics. The second batch's 13,582 approved deliveries include 10 corrections; it adds 13,572 current keys. Silver totals are **whole-table synthetic segment-state sums**, not per-batch expenditure or actual Medicare spending.

## Local checks and boundaries

Run `python scripts/check_repository.py`. It compiles source text without importing Spark, checks repository-relative Markdown links, parses JSON artifacts, confirms the observed Job dependency chain, verifies notebook source hashes, and scans deliverables for credential patterns and disallowed data/state files. The bundle YAML was additionally parsed locally and compared with the original export; its paths were found to differ from deployed settings.

These are source/repository checks, not a new live pipeline execution or proof of transformation correctness. No hosted CI run is claimed for this branch.

Local result: **PASS** for 7 Python source compilations, 24 relative links, 5 parsed JSON files, 6 unchanged notebook exports, the 5 deployed task dependencies, and credential-pattern/data-file scans. The actual bundle YAML also parsed successfully. Saved live metrics were cross-checked against the documented counts using exact decimal comparisons.

Remaining verification gaps:

- No second complete CMS Job rerun, injected-failure/recovery run, or checkpoint-reset test was performed or evidenced here. Sequential retry support is implemented but broad rerun safety remains unverified.
- No concurrent-writer test, real-time latency test, clean-workspace rebuild, or performance benchmark was performed.
- No observed schema-drift, stale-version, event-ID conflict, update-without-base, same-version non-payment conflict, or null-operation test result was retrieved for this CMS run. See [quality limits](data_quality.md).
- Notebook exports represent the current workspace source, not an immutable source-code snapshot attached to the historical Job run.
- Audit notebooks skip previously audited files. A successful audit task does not mean all historical audit assertions were rerun against the current table state; the separate read-only queries above check current counts and linkage.
- Queries observe tables across separate transactions, not a single atomic cross-table snapshot.
- Bootstrap still depends on existing source/template tables. The saved bundle file is not verified deployable as-is.

No missing task notebook export or final-count evidence remains. Configuration mismatches and the above untested behaviors remain documented; implementation logic was not refactored during finalization.
