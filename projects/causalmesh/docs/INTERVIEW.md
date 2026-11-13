# Senior engineering interview walkthrough (claims limited to actual project)

**Problem.** Financial pipelines can succeed computationally yet fail the core business invariant: the debit and credit legs do not reconcile or retroactive correction destroys the audit trail.

**Design.** I built a local reference system whose raw event log and current projection commit together. Message IDs are idempotent, event contracts reject invalid revisions, and a hash chain verifies the source. The interesting modeling choice is two independent clocks: event effectiveness and when the system learned about the event.

**Tradeoff.** I chose a single-host SQLite WAL engine to make a full correctness demonstration testable without cloud credentials. Kafka and Parquet are optional pathways; they do not constitute production-scale distributed processing by themselves. I'd move to partition-aware transactional ingest and a lakehouse table format when scaling.

**Reliability.** I tested out-of-order legs, duplicates, invalid contracts, conflicting IDs, tamper detection, corrections, point-in-time queries, deterministic replay and zero-persistence simulation. The system prevents replay of a log that fails audit verification.

**Observability.** It exposes Prometheus-readable counters, reconciliation explanations, hash-chain status, per-transfer provenance and an interactive dashboard.

**Future path.** I would add CDC, schema registry, multi-region DR, signed external audit anchors, tenant security/RBAC, distributed tracing, controlled backfills and capacity testing before production deployment.

**Avoid saying:** "I built this at JPMorgan", "zero data loss at scale", "globally exactly once", "production financial transactions", "AI/ML real-time fraud engine", "enterprise ready", or numeric performance results unless separately verified in a real environment.
