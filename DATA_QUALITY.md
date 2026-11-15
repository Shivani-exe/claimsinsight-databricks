# Data quality

The validation notebook reads the existing eight Delta tables. It does not repair records, overwrite data, change the dashboard, or claim that every Segment 2 record is an invalid claim.

## Execution and evidence

On November 15, 2026, **240 checks passed, 0 failed**, using the notebook's SQL and Python result evaluation through the authenticated Databricks Statement Execution API on the existing warehouse. That includes **169 table/column schema checks** and **71 data checks**. The Spark notebook display adapter was not executed in Databricks; the shared SQL and validation functions were executed against live tables. [Full measured/expected evidence](docs/live_test_results.json) and [test inventory](docs/test_inventory.md) distinguish these results from offline CI.

## Controls

| Area | Checks and expected behavior |
| --- | --- |
| Source | 66,773 inpatient and 116,352 beneficiary rows; required original columns and types; nonblank IDs and segment identifiers |
| Claim grain | Unique `(claim_id, segment)`; 66,705 claim IDs; 68 two-segment claims; no repeated reporting claim IDs; no multi-segment reporting claims |
| Cohort completeness | Bidirectional `EXCEPT ALL` on all reporting fields against single-segment, claim-through-2008 Silver segments, before any null-payment filter |
| Financial | Decimal schemas; no missing eligible payments; 22 negative, 1,039 zero, 26,414 positive payments; signed net total $254,560,980.00 |
| Dates | Real DATE fields; all reporting claim-through dates in 2008; no nonempty malformed source claim dates; exact source-to-Silver parsing; 68 Segment 2 records with both dates missing |
| Segment linkage | Every Segment 2 matches Segment 1 by claim ID and beneficiary; no artificial date replacement |
| Beneficiary | Unique, nonblank identifiers; source-to-Silver population and attribute preservation; no unmatched reporting claims; valid nonempty birth/death dates |
| Gold | Unique documented grain, nonnull measures, positive claim counts; exact net totals and counts; bidirectional comparison of every group including averages, distinct counts, and maxima |

Null/blank first-diagnosis values map to `UNKNOWN` only in Gold. Diagnosis counts include that group. Monthly distinct-beneficiary counts overlap across months and must not be summed to obtain an annual population.

## How failures behave

The notebook accumulates names, PASS/FAIL, measured, and expected values. Query exceptions become failed checks; other independent queries continue. A schema failure stops data-query execution because missing or incorrectly typed fields invalidate downstream interpretation. The displayed summary then raises `AssertionError` if any result failed. No empty result is considered a pass.

Expected values are a fixed-sample regression baseline, not substitutes for live calculations. Payment expectations use `Decimal`; comparison does not round, coerce null to zero, or introduce tolerances. Gold average checks reproduce the existing `ROUND(AVG(...), 2)` policy. Aggregate totals remain exact.

## Cost and practical limits

One metadata query checks all eight schemas. Each data query collects only a small aggregate result, and several related metrics share an aggregate scan. Full comparisons use server-side `EXCEPT ALL`; they can scan tables more than once, but catch duplication and offsetting errors. This is appropriate for this sample; no performance claim or benchmark is made. No caching or paid infrastructure is required.

These tests observe tables over multiple queries, not one cross-table transaction. Run after pipeline writes have finished. Step 8 did not rerun any transformation or ingestion writer.

## Investigating a failure

1. Read the failed test's measured/expected values and any execution error.
2. Check table accessibility and schema before examining records.
3. Inspect failed keys inside Databricks with read-only queries; do not publish raw row samples.
4. Determine whether the source, transformation, or approved cohort changed.
5. Fix the actual issue or document an intentional baseline change with evidence. Never alter records to force a pass.

## Known unresolved findings

Multi-segment payment semantics remain unresolved; financial reporting excludes those claims in full. Negative/zero payments remain included. Synthetic records cannot substantiate actual Medicare expenditure or clinical conclusions. A passing test run establishes the listed contracts for the observed tables, not general production reliability.
