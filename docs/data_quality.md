# Validation and quarantine

Rules below are from the exported classifier and processor. Implemented rules are distinct from scenarios observed in the retrieved CMS execution.

## Keys and ordering

- Delivery identity: `(source_file, event_id)`. Classification also asserts that pending `event_id` values are unique across the pending set and that source-file/event identifiers are present. A duplicate pending event ID stops the task; it is not silently deduplicated.
- Business identity: `(claim_id, segment)`. More than one approved event for a key in a processing pass stops the task, avoiding an ambiguous MERGE.
- Saved decisions are not reclassified on retries. The ledger excludes finalized deliveries from further processing.

## Classification precedence

| Rule | Decision |
| --- | --- |
| Missing/blank claim identifier | `REJECT_MISSING_ID` |
| Failed required-field/type checks, missing/nonpositive segment/version, absent beneficiary/provider/date/timestamp, or unsupported operation | `REJECT_INVALID_FIELDS` |
| Payment cannot be cast to `decimal(18,2)` | `REJECT_INVALID_PAYMENT` |
| Nonnull `_rescued_data` | `REJECT_SCHEMA_DRIFT` |
| Historical event ID with identical raw payload fingerprint | `IGNORE_DUPLICATE_EVENT` |
| Historical event ID with different raw fingerprint | `REJECT_EVENT_ID_CONFLICT` |
| Same version with different payment | `REJECT_VERSION_CONFLICT` |
| Older version | `IGNORE_OLDER_VERSION` |
| Same version, no prior rejection | `ALREADY_APPLIED` |
| UPDATE without a matching Silver key | `REJECT_UPDATE_WITHOUT_BASE` |
| Otherwise accepted | `READY_FOR_MERGE` |

This is the implementation's ordered first-match logic; one record does not receive every applicable reason. Payment checks reject null/unparseable values, not zero or negative payments. Current Silver retains 33 negative and 1,103 zero payment values.

`READY_FOR_MERGE` becomes `MERGED` in the ledger after Silver verification; all `REJECT_*` statuses become `QUARANTINED`; `IGNORE_DUPLICATE_EVENT` becomes `IGNORED_DUPLICATE`. Other statuses retain their names.

## Quarantine and observed scenarios

The processor rejoins rejected decisions to the raw queue, then insert-only MERGEs quarantine by delivery key. This preserves values such as `NOT_A_NUMBER`; it does not repair or delete the original delivery. Quarantine reason and count are verified before registering the ledger.

The observed Batch 002 contains five `REJECT_MISSING_ID` deliveries and three `REJECT_INVALID_PAYMENT` deliveries. All three invalid strings remain in quarantine. One hundred historical replays were classified and registered as ignored duplicates. Other rejection/version branches are implemented but not established as tested by this run.

## Known limits from inspection

- Replay fingerprints use raw string payloads, so formatting differences can produce a different hash even when values appear semantically equivalent.
- Same-version conflict classification compares **payment**, not the complete payload. A new event ID with a changed non-payment field and unchanged version/payment can become `ALREADY_APPLIED`. The later full-payload guard applies to approved MERGE candidates, not every same-version classification.
- The operation-membership expression does not explicitly check `operation IS NULL`. Spark's null boolean semantics can leave that case unclassified as invalid. This scenario was not present in the verified batch and was not changed in this documentation task.
- Schema rescue is implemented, but no schema-drift injection result was retrieved. CSV parse failures and rescued fields should not be assumed to cover every malformed-input case.
- Birth/death fields, beneficiary master linkage, diagnosis reference validation, delete events, and adjudication rules are outside this pipeline's implemented checks.
- Published CMS DE-SynPUF records are synthetic; scenario metadata and invalid/correction examples were generated for this experiment.

No rule was rewritten to obtain expected counts. See [verification](verification.md) for actual checks and remaining gaps.
