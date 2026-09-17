# Verified demo results

Generated on 2026-09-17 with the default deterministic seed and `make demo`.

## Source and quality counts

| Measure | Result |
|---|---:|
| Logical transactions generated | 3,000 |
| Physical source rows, including injected duplicates | 3,060 |
| Canonical Silver payments | 3,000 |
| Malformed rows quarantined | 3 |
| Duplicate rows removed | 57 |
| Matched settlements | 2,692 |
| Missing settlements | 85 |
| Amount mismatches | 11 |
| Payments not expected to settle | 212 |
| Rule-based review alerts | 290 |
| Customer 360 records | 250 |
| Daily regulatory-style rows | 120 |
| Overall configurable quality gate | PASS |

## Canonical payments by channel

| Payment type | Rows | Total synthetic value (INR) |
|---|---:|---:|
| CARD | 623 | 5,154,594.69 |
| NEFT | 564 | 6,433,201.88 |
| RTGS | 252 | 207,039,524.63 |
| UPI | 1,561 | 17,171,767.85 |

These are reproducible **portfolio demo results**, not employer production
metrics. Rerunning with a different seed or volume changes the results.
