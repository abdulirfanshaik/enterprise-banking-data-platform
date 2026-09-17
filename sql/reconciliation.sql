-- Daily control totals and exception drill-down.
WITH payment_totals AS (
  SELECT event_date, payment_type,
         COUNT(*) AS payment_count,
         ROUND(SUM(CAST(payment_amount AS NUMERIC)), 2) AS payment_value
  FROM settlement_reconciliation
  WHERE reconciliation_status <> 'NOT_EXPECTED'
  GROUP BY event_date, payment_type
),
settlement_totals AS (
  SELECT event_date, payment_type,
         SUM(CASE WHEN reconciliation_status IN ('MATCHED', 'AMOUNT_MISMATCH') THEN 1 ELSE 0 END) AS settlement_count,
         ROUND(SUM(CASE WHEN settled_amount <> '' THEN CAST(settled_amount AS NUMERIC) ELSE 0 END), 2) AS settlement_value
  FROM settlement_reconciliation
  WHERE reconciliation_status <> 'NOT_EXPECTED'
  GROUP BY event_date, payment_type
)
SELECT p.event_date, p.payment_type, p.payment_count, s.settlement_count,
       p.payment_value, s.settlement_value,
       p.payment_count - s.settlement_count AS count_difference,
       ROUND(p.payment_value - s.settlement_value, 2) AS value_difference
FROM payment_totals p
JOIN settlement_totals s USING (event_date, payment_type)
WHERE p.payment_count <> s.settlement_count OR p.payment_value <> s.settlement_value
ORDER BY p.event_date, p.payment_type;

-- Transaction-level exceptions.
SELECT *
FROM settlement_reconciliation
WHERE reconciliation_status IN ('MISSING_SETTLEMENT', 'AMOUNT_MISMATCH')
ORDER BY event_date, payment_type, payment_id;
