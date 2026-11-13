# Contributing

Use Python 3.11+, run `pip install -e '.[dev]'`, and verify `pytest -q` before PRs. Use only synthetic event payloads; never commit sensitive customer data, access keys, actual account numbers, or production log samples. Any modification to event contracts should include compatibility and temporal-history tests. For audit alterations, include an explicit threat-model note. A green unit suite validates the local core but does not certify distributed correctness or security.
