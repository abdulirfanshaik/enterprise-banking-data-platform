# Architecture Decision Records (ADRs)

## ADR-001: Portable transactional reference engine

**Decision:** use SQLite with WAL and `synchronous=FULL` as the local source of truth rather than introducing a multi-service database dependency for the runnable baseline. **Why:** tests run in seconds with no Docker or cloud credentials; full raw+projection atomicity can be demonstrated directly. **Tradeoff:** single-host/single-writer; does not model horizontal scale.

## ADR-002: At-least-once transport, idempotent effects

**Decision:** deduplicate with immutable `event_id` and canonical payload; treat the broker's offset as delivery progress, not a business event identity. **Why:** retry after broker/consumer crashes should not create duplicate balances. **Tradeoff:** dedup state and input schema are controlled by a single local database; high-scale systems would partition and coordinate state.

## ADR-003: Two independent clocks

**Decision:** represent `effective_at` separately from `sys_from` and `sys_to`. **Why:** compliance and reconciliation require both *what actually applied* and *what was known at that moment*. **Tradeoff:** queries are more complex and all historical versions must be retained or archived.

## ADR-004: Integer minor units

**Decision:** reject floats and cap event size. **Why:** prevents binary float rounding errors and absurd amounts. **Tradeoff:** currency-specific precision must be extended to cover currencies with 0 or 3 decimal places; current supported demo currencies use two minor units.

## ADR-005: Crypto evidence with explicit limitation

**Decision:** hash chain raw log with a canonical JSON representation. **Why:** deterministic audit validation. **Tradeoff:** no external anchoring, and privileged writers can forge a whole replacement chain.

## ADR-006: Rollback-only what-if simulation

**Decision:** use a SQLite savepoint inside a transaction and always roll it back. **Why:** what-if should reuse actual validation and reconciliation code without creating phantom events. **Tradeoff:** simulations operate on the local current snapshot, not a distributed snapshot spanning upstream/downstream systems.

## ADR-007: Optional, not fake, integrations

**Decision:** Kafka and DuckDB integrations are real optional code paths, not claims of an actively deployed enterprise pipeline. **Why:** the zero-infrastructure core must run first. **Tradeoff:** remote/storage integrations need environment-specific integration and load tests before readiness claims.
