# Architecture and correctness model

## Event lifecycle

1. A strict versioned contract validates IDs, ISO-8601 timezone offsets, currency, integer minor units, debit/credit side and revision.
2. Inside `BEGIN IMMEDIATE`, the engine looks up `event_id`; identical payload returns a duplicate acknowledgement, conflicting payload is quarantined.
3. New logical legs require revision 1. Later revisions must increment without gaps and preserve transfer, account, counterparty, currency and side.
4. In one SQLite commit, the engine appends a canonical log event and hash proof, ends any previous ledger revision, creates a new active revision, and recomputes explainable alerts.
5. Reconciliation reads the latest active debit and credit legs. A missing leg is PENDING; mirrored matching legs are SETTLED; other two-leg results are MISMATCH.
6. The API creates short-lived read connections and exposes the resulting state to the dashboard.

## Temporal semantics

Business time: `effective_at`. Knowledge/system time: `sys_from` and `sys_to`. A version is visible iff:

```sql
WHERE effective_at <= :business_cutoff
  AND sys_from <= :knowledge_cutoff
  AND (sys_to IS NULL OR sys_to > :knowledge_cutoff)
```

`recorded_at` is generated at application ingestion time and is monotonically increased by a microsecond when a caller supplies a timestamp earlier than the last accepted event. This permits repeatable test histories while preserving ordered revisions. At scale, use database commit order or sequence as source of system truth; do not trust device timestamps.

**Important:** A correction's `effective_at` can predate its original record, but its `sys_from` always identifies when that correction became known. A replay uses the original stored `recorded_at` instead of the replay wall clock so past answers remain stable.

## Audit log

`H_0 = "0" * 64` and `H_n = SHA256(H_(n-1) + "|" + recorded_at + "|" + canonical_json(event))`.

The verifier recomputes each link. Projection replay refuses to operate on a detected tampered chain. This detects accidental/unauthorized modifications to the stored chain, **but a sufficiently privileged operator can rewrite both rows and hashes**. External hash notarization, signatures or WORM storage would be required for a stronger tamper-proof claim.

## Consistency boundaries

The local core guarantees atomic raw+projection writes and retry-safe effects by `event_id`; it cannot guarantee distributed exactly-once transport. The optional Kafka consumer commits an offset *after* the SQLite transaction. Crashes between DB commit and Kafka offset commit cause reprocessing, which is harmless for the same `event_id`+payload. Offset state is also stored for audit, not used as a primary deduplication key.

The local system uses a single-writer SQLite WAL database for a reproducible engineering slice. This is **not** a clustered storage or a high-throughput Spark processing layer.

## Threat boundaries

User-supplied identifiers use parameterized SQL. The dashboard escapes displayed values. No real bank credentials or financial account information are used. The dashboard and simulation endpoint have no login mechanism; bind the demo to loopback, do not put the current app directly on a public internet endpoint.

## Interfaces

- `Engine.ingest` → accepted / duplicate / quarantined
- `Engine.reconciliation` → current paired settlement status
- `Engine.balance_as_of` → business time AND knowledge time balance
- `Engine.verify_audit` → chain validity, checked count, failing sequence
- `Engine.replay` → full derived-state rebuild under one transaction
- `Engine.simulate` → hypothetical state under savepoint, rolled back
- `Engine.lineage` → physical source event IDs, positions and hash evidence

## Extension points

Use an outbox for fanout, distributed lock/transaction boundary for concurrent writers, Iceberg or Delta tables for versioned analytical state, Debezium-style CDC connectors, and event sourcing with stronger partition/tenant management. Separate command/control and query projection services as write volume grows.
