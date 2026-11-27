# Engineering decisions

| Decision | Rationale and tradeoff |
| --- | --- |
| Isolate CMS tables and volume paths | Preserves MVP1 and earlier experiments while processing the full selected CMS years. |
| Separate ingestion and staging checkpoints | File discovery and Delta-source progress are independent stream states. Neither checkpoint replaces business-level delivery tracking. |
| Use `availableNow` | Processes available input incrementally and then stops; suitable for this scheduled/on-demand batch design. No continuous latency claim is made. |
| Separate delivery and claim keys | File/event identity tracks repeated deliveries, while claim/segment identity identifies mutable Silver state. Repeated events in another file remain visible in Bronze and the ledger. |
| Persist decisions before applying writes | A sequential retry resumes the same decision instead of reclassifying against an already-updated Silver table. |
| Compare raw payload fingerprints | Distinguishes exact historical replays from event-ID reuse with changed content. String-format differences are a known limitation. |
| Require newer versions for Silver updates | Avoids overwriting current state with older events. Versions are simulated experiment metadata, not native CMS correction identifiers. |
| Verify Silver payload before final ledger registration | Supports recovery when Silver succeeded but later writes did not. The processor permits an identical already-applied version; stale/conflicting candidates stop processing. |
| Preserve raw rejected values | Quarantine is explainable and traceable, rather than losing invalid text through casts. |
| Insert-only ledger and audit MERGEs | Avoids duplicate delivery outcomes and source-file audit rows under the assumed sequential retries. Already-audited files are skipped, so audits are historical snapshots rather than continuous rechecks. |
| Limit concurrent Job runs to one | The logic assumes sequential processing. This setting does not prevent a user or a different job from writing concurrently. |
| Keep transactions separate | Silver, quarantine, and ledger operations commit independently. Recovery relies on saved decisions, verification, and sequential retries; there is no atomic multi-table commit or universal exactly-once guarantee. |
| Preserve actual deployed artifacts | Exported source and both configurations remain unchanged. Mismatched bundle paths and the `ausit_claims` task-key typo are documented rather than silently corrected. |

Successful processing of Batch 002 demonstrates the observed inserts/corrections/rejections/replays. It does not demonstrate all failure interleavings, a clean rebuild, or concurrent-writer safety. No new features or performance optimizations were added during this repository finalization.
