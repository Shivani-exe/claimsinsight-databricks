# Engineering decisions

| Decision | Reason and tradeoff |
| --- | --- |
| Preserve original records in Bronze | Retains traceability and allows parsing to be investigated without editing source values. Bronze source fields remain strings. |
| Preserve claim segments in typed Silver | A claim ID can legitimately have two source segments; uniqueness is tested at `(claim_id, segment)`. |
| Exclude entire multi-segment claims from reporting | Paired payments can differ, and summation/deduplication semantics have not been established. Exclusion narrows analytical coverage but avoids inventing financial meaning. |
| Keep zero and negative payments | Net reporting includes the actual signed values. Removing them would change the cohort and payment total. |
| Use claim-through date for time reporting | This is the documented analytical period. It does not claim to identify an actual payment date. |
| Use first recorded diagnosis only | Avoids allocating one payment across multiple codes without an established attribution rule. It is not a complete comorbidity model. |
| Map blank/null first diagnosis to UNKNOWN in Gold | Retains every reporting claim in exactly one diagnosis group. |
| Use decimal financial types | Silver is `decimal(18,2)`; existing Gold totals are `decimal(29,2)`. Validation uses exact Decimal comparisons and preserves negative values. |
| Compare whole cohorts and every Gold group | Total-only checks can miss offsetting errors, changed keys, or dropped rows. `EXCEPT ALL` also preserves duplicate multiplicity. |
| Validate otherwise-eligible claims before the payment filter | The original writer removes null paid amounts. Completeness checks expose future silent exclusions; current eligible payment nulls are zero. |
| Separate offline CI from live data validation | GitHub Actions can check Python syntax and the runner's failure handling without a Databricks credential. Live results require authenticated workspace execution. |
| Keep a small SQL/Python test runner | Functions have clear responsibilities and no framework dependency. SQL runs in Spark or over the Statement API. |
| Preserve the existing dashboard and tables | Step 8 adds validation and documentation without changing analytical behavior. Original notebook write operations were exported, not executed. |
| Present only synthetic analytical results | The dataset cannot measure actual Medicare expenditure, savings, or clinical outcomes. |

No inference about multi-segment payment semantics was implemented. That question remains explicitly open for a later MVP.
