# Operations runbook

## Quality gate fails

1. Open `data/processed/metrics/quality_report.json` and identify the failed gate.
2. For rejects, group `silver/rejected_records.csv` by source file and error.
3. For duplicates, verify the producer retry pattern and the composite idempotency key.
4. For settlement failures, filter `gold/settlement_reconciliation.csv` to
   `MISSING_SETTLEMENT` and `AMOUNT_MISMATCH`.
5. Correct the source or transformation, rerun the affected interval, and
   compare control totals before publication.

## Kafka consumer lag

Check producer rate, partition distribution, consumer-group lag, processing
errors, and sink throughput. Scale partitions/consumers only after locating the
bottleneck. Preserve message keys so the required ordering contract is clear.

## Schema change

Capture the new source sample, compare it with the contract, decide whether the
change is additive or breaking, update the mapper and tests, backfill a small
interval, and reconcile the result. Unknown fields may remain in Bronze, but
Silver publication should follow an explicit reviewed contract.

## Replay

Bronze is the replay boundary. Reprocess by source/date into a new versioned
Silver target, validate counts/amounts and duplicates, then promote atomically.
Do not overwrite the only copy of the original source payload.

## Settlement discrepancy

Break differences down by business date and channel, then identify missing IDs,
duplicates, status-filter mismatches, late arrivals, and amount differences.
Publish exception records separately and do not mark the dataset complete until
the control owner accepts or resolves the exceptions.
