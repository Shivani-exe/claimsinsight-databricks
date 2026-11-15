# Step 8 verification

## Executed

On November 15, 2026, the official Databricks CLI authenticated to the user's existing Free Edition workspace. Unity Catalog inspection confirmed all eight expected Delta tables and their column contracts. The existing dashboard was identified but not modified.

The four original notebooks were exported through the workspace API. The Gold notebook's exported source is SQL and is stored with a `.sql` extension. Explanatory comments were added before exported cells; transformation code was preserved. No source notebook was rerun.

The functions and SQL in `05_Data_Quality_Tests.py` were executed through the Databricks SQL Statement Execution API against the existing serverless warehouse. **240 checks passed, 0 failed:** 169 schema/table checks and 71 data checks. The final run used the delivered `scripts/run_live_tests.py` end to end. UTC timestamps, statement count, and the notebook SHA-256 are recorded in `live_execution.json`; full names and values are in `live_test_results.json` and `test_inventory.md`.

The live run validated Bronze counts and identifiers, segment grain, complete reporting cohort, parsed dates and source preservation, beneficiary linkage, signed payments, Gold grain, grand totals, and every Gold group. The current live results match the supplied references; no new data-contract failures were discovered.

Offline verification compiled all Python notebook, script, and test files, then executed **13 standard-library unit tests** successfully:

| Test | Result |
| --- | --- |
| `test_decimal_precision` | PASS |
| `test_invalid_numeric_values_fail` | PASS |
| `test_record_preserves_failed_measurement` | PASS |
| `test_summary_failure_displays_before_raising` | PASS |
| `test_namespace_rejects_sql_fragments` | PASS |
| `test_schema_rejects_nondecimal_payments` | PASS |
| `test_missing_schema_stops_data_queries` | PASS |
| `test_query_error_is_reported_and_continues` | PASS |
| `test_one_cent_mismatch_is_detected` | PASS |
| `test_empty_query_response_is_failure` | PASS |
| `test_decode_keeps_decimal_strings` | PASS |
| `test_executor_accepts_newline_after_select` | PASS |
| `test_decode_rejects_failed_or_partial_results` | PASS |

These tests use mocked metadata/scalar response fixtures to exercise validation and failure handling. They do not execute Spark transformations or validate live data. The live SQL execution provides separate evidence for the actual query logic.

## Boundaries and remaining validation

- The notebook's Spark display/auto-run adapter has not been executed inside a Databricks notebook session. To verify that UI path, import notebook 05 and Run All; existing data is read-only.
- The reusable CLI runner completed successfully with a temporary clean authentication profile. The user's original on-disk profile remains unchanged; its token-format issue must be corrected for normal CLI use.
- GitHub Actions is configured for offline checks only. Its result must be checked on the pull request; a local pass does not establish a hosted CI pass.
- Data was observed across multiple read queries, not a global atomic snapshot. No ingestion or transformation writers were run during validation.
- No end-to-end clean-workspace rebuild, incremental-load test, performance benchmark, or dashboard export/rebuild was performed.

## Connection finding

The local profile's initially pasted PAT contained a control character. Authentication succeeded after excluding that character for the preparation-time child process. For final end-to-end verification, a temporary CLI profile held the clean credential and was deleted automatically after the run. The original profile was not changed. No token or profile content was printed or copied into this repository. Reconfigure the local CLI profile with a clean token if normal CLI commands still report an invalid Authorization header. The reusable repository runner uses the standard CLI profile unchanged.

End-to-end verification also caught a runner-only bug: the initial SELECT/WITH guard rejected valid multiline SELECT queries. It now accepts keyword boundaries, with a regression test. The final run passed all 240 checks. This fix did not change the data, SQL checks, or baseline expectations.

## Review checklist

Review the original writers before attempting reproduction in another workspace; they overwrite/replace fixed table names. Review the supplied fixed-sample baselines and the unresolved multi-segment policy. Merge the Step 8 pull request only after reviewing the files and the hosted CI result. Later MVPs remain proposed scope, not delivered features.
