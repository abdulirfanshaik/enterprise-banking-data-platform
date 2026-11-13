# Security, privacy, and scope

- **Synthetic-only:** generator creates pseudonymous test IDs; no actual bank names, customer PII, account credentials, or payment instructions.
- **No execution:** debit/credit data are simulated accounting messages; no funds transfer API exists.
- **Input validation:** whitelisted fields, allowable currencies, positive bounded integer minor units, strong revision order, UTC-aware timestamps and parameterized SQL.
- **Audit assurance:** tamper-evident hash chain, *not* cryptographic proof against a privileged attacker who can rewrite the full chain. Production requires signing, remote anchoring, WORM backup, trusted time and key management.
- **Auth boundary:** the demo API has NO OAuth/RBAC. Bind `uvicorn` to `127.0.0.1` and avoid public exposure. The Docker Compose host mapping is loopback-only.
- **Data lifecycle:** current code has no retention jobs, PII erasure framework, legal hold, encryption at rest, or tenant isolation. These need design work before handling real data.
- **Dependencies:** use isolated environments, vulnerability scanning, dependency locks, and regular updates. `pip install -e` is developer-friendly, not a production supply-chain guarantee.
- **Brokers:** Redpanda Compose is a single-node development profile. No TLS/SASL here; never deploy it publicly without securing transport, authentication and network controls.
- **Operational safety:** use immutable backups and verify integrity before deterministic replays. Do not edit ledger versions directly.

This project is a portfolio demonstration and is not a regulated banking system or substitute for formal financial controls.
