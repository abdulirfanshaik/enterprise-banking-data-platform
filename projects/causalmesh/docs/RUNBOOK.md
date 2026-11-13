# Operational runbook

## Standard developer checks

```bash
pytest -q
causalmesh stats
causalmesh audit
curl -f http://127.0.0.1:8000/healthz
curl -s http://127.0.0.1:8000/metrics
```

## High quarantine count

Inspect recent records and reasons:

```bash
python - <<'PY'
import sqlite3
con=sqlite3.connect('data/causalmesh.db')
for row in con.execute('select reason,count(*) from quarantine group by reason order by count(*) desc'):
    print(*row)
PY
```

Do not blindly retry conflicts; `event_id` reuse with a different payload indicates a producer contract violation. Validate upstream revision increment and stable leg identity. This demo keeps quarantine in the database rather than an independently durable DLQ topic.

## Settlement mismatch

Open `/api/transfers/TRX-...`, check both legs' amount, currency and reciprocal counterparty fields. Cross-check evidence through `/api/lineage/TRX-...`. Create an additional versioned correction event, **not** an in-place edit.

## Audit integrity failure

**Stop replay and treat the source as untrusted.** Preserve a forensic copy, inspect changes to raw log and system access, and restore from an independent trusted backup. The verifier does not make tampering impossible.

## Full projection rebuild

```bash
causalmesh audit && causalmesh replay && causalmesh stats
```

Replay is a single transaction; the source log is unchanged. Avoid unbounded snapshots while a live writer is ingesting; coordinate a maintenance window for a stateful production adaptation.

## Kafka broker unavailable

The standalone CLI and FastAPI path remain functional without Kafka. Restart the optional dev broker and consumer after validating its configured advertised address. If Kafka redelivers after a crash, the identical event is acknowledged as `duplicate`.

## Capacity and backup

SQLite WAL requires local disk, not shared network filesystems. Size the DB and maintain checkpoints/backups before long-lived usage. For production, replace local single-writer storage with a managed transactional database and resilient analytical store.
