# MVP roadmap

Only MVP1 is implemented. The later stages below are proposed portfolio milestones; no previously agreed MVP2–MVP6 specification was available during Step 8.

| Stage | Status | Proposed scope and exit evidence |
| --- | --- | --- |
| MVP1: synthetic inpatient analytics | Implemented; Step 8 review pending | Batch Bronze/Silver/Gold, existing dashboard, read-only quality checks, documentation, and offline CI. Live reference checks passed; review repository changes before merging. |
| MVP2: clarify claim semantics | Proposed | Research CMS segment/payment definitions, document evidence, and test approved handling before extending cohort coverage. Do not assume segment payments should be added. |
| MVP3: incremental batch processing | Proposed | Define stable input identifiers, idempotent loads, and rerun behavior with small fixtures. Retain raw lineage and reconciliation. |
| MVP4: operational checks | Proposed | Add run-level audit records, failure reporting, and a documented recovery procedure using supported Databricks capabilities. |
| MVP5: analytical enrichment | Proposed | Evaluate code descriptions, beneficiary attributes, and additional years or claim types with explicit attribution and join-grain rules. |
| MVP6: deployment and interview review | Proposed | Assess environment parameterization and authenticated integration CI only if justified; document deployment, access, cost limits, and a concise project walkthrough. |

Each future stage requires a separate scope and acceptance criteria. Kafka, Airflow, dbt, new paid services, and extra cloud infrastructure are not required or introduced in MVP1.
