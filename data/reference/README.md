# Optional reference data

The runnable demo does not require downloads. If you add public data, keep the
raw download unchanged and record the source URL, retrieval date, license, and
checksum.

Recommended sources:

- RBI Database on Indian Economy: Payment System Indicators and bank-wise
  payment statistics: https://data.rbi.org.in/
- NPCI UPI Product Statistics: https://www.npci.org.in/what-we-do/upi/product-statistics
- IBM AMLSim: https://github.com/IBM/AMLSim
- IBM AML-Data: https://github.com/IBM/AML-Data

RBI and NPCI publications are aggregate reference datasets. They should be used
for trend comparison or regulatory-style totals, not represented as account- or
customer-level transaction feeds. IBM AMLSim and AML-Data are synthetic.

Suggested normalized reference schemas:

```text
npci_upi_monthly.csv: month, participating_banks, volume_millions, value_inr_crore
rbi_payment_monthly.csv: month, payment_system, volume, value_inr
```
