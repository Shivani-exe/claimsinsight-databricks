# Architecture and execution

## Data flow and grains

All seven Delta tables are in `workspace.claimsinsight_cms_mvp2`; [actual column/type contracts](table_contracts.json) were retrieved from Unity Catalog.

| Table | Grain and role |
| --- | --- |
| `bronze_claim_events_mvp2` | Every ingested delivery, preserving CSV strings, rescued fields, timestamp, and file path |
| `mvp2_processing_queue` | Persistent staged deliveries; no validation or Silver writes in this step |
| `mvp2_processing_decisions` | One saved classification per `(source_file, event_id)` with typed payload |
| `silver_claims_current_mvp2` | Current version per `(claim_id, segment)`; payments are `decimal(18,2)` |
| `quarantine_claim_events_mvp2` | Rejected delivery with original string values and rejection reason |
| `mvp2_event_processing_ledger` | Final delivery outcome per `(source_file, event_id)` |
| `mvp2_batch_audit` | One insert-only audit per source-file hash; counts plus whole-Silver snapshot |

The job order is `ingest_claims → stage_events → classify_events → process_claims → ausit_claims`. The final key is spelled this way in the deployed Job; its purpose is audit. Each downstream task requires its predecessor's success. The job limits concurrent runs to one and enables queueing; no schedule is present in the retrieved settings.

## Storage and source mapping

Volume root: `/Volumes/workspace/claimsinsight/mvp2_cms_upgrade/`.

| Path suffix | Use |
| --- | --- |
| `landing/cms_batch_001`, `landing/cms_batch_002` | Generated CMS-derived CSV batches |
| `incoming/` | Delivered `cms_batch_001.csv` and `cms_batch_002.csv` |
| `schemas/claims` | Auto Loader schema tracking |
| `checkpoints/autoloader_claims` | File-ingestion stream progress |
| `checkpoints/bronze_processing_queue` | Bronze-to-queue Delta stream progress |

The source is `workspace.claimsinsight.bronze_inpatient`. The generator selects years using the first four characters of `CLM_FROM_DT`, parses `CLM_THRU_DT`, preserves `CLM_PMT_AMT`, and maps **`ADMTNG_ICD9_DGNS_CD`** to `diagnosis_code`. This is the admitting diagnosis field, unlike MVP1's first recorded diagnosis field. Rows without required source values are filtered during batch preparation; 68 missing-date source segment records are outside this selected cohort. No new interpretation of multi-segment finances is introduced.

## Existing workspace execution

Use the deployed `ClaimsInsight_CMS_MVP2_Pipeline` job after reviewing its input files. The five task notebooks target only the isolated CMS schema. Keep deliveries immutable and preserve both checkpoints, decisions, and ledger tables across ordinary runs. The current job has already succeeded; repository preparation did not run it again.

## Separate-environment reproduction

1. Provide the CMS Sample 1 inpatient source as the original Bronze table and access to a Unity Catalog volume. Import the actual [setup export](../setup/00_CMS_Setup_and_Batches.py).
2. Review its cells before execution. It creates the volume/folders, generates both batches with overwrite writes in **landing**, stages only Batch 001, and initializes empty CMS tables by cloning schemas from seven existing `workspace.claimsinsight` template tables. Those legacy templates are an explicit bootstrap dependency; their earlier 19-event prototype is not the subject of this branch. [Table contracts](table_contracts.json) describe the retrieved CMS structures for manual preparation if those templates are unavailable.
3. Run only the preparation cells intended for an empty environment. The initializer asserts the CMS tables are empty and will fail once data is populated. The setup notebook also contains a later Batch 002 staging cell; do not execute it before the first batch run.
4. Import the five task notebooks. If reusing original Job paths, import them under the numbered workspace names recorded in [the manifest](../evidence/source_manifest.json). Otherwise configure a separate sequential Job using the imported locations and the same task dependency chain.
5. Run the five tasks for Batch 001, inspect its audit, then run the setup notebook's Batch 002 staging cell and execute the chain again. Verify table counts and ledger/quarantine outcomes with the read-only queries in [the evidence](../evidence/live_verification.json).

No clean-environment rebuild was performed here. The [deployed Job JSON](../config/deployed_job.json) is an actual API snapshot, including workspace-specific paths and IDs. The separately preserved [bundle YAML](../config/workspace_bundle/databricks.yml) has different, unnumbered paths and `audit_claims` instead of `ausit_claims`; deployment equivalence is unverified. Neither configuration was deployed or changed during documentation.
