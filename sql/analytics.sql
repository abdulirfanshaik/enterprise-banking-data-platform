-- Payment mix.
SELECT payment_type,
       COUNT(*) AS transaction_count,
       ROUND(SUM(CAST(amount AS NUMERIC)), 2) AS transaction_value,
       ROUND(AVG(CAST(amount AS NUMERIC)), 2) AS average_ticket
FROM payments
GROUP BY payment_type
ORDER BY transaction_value DESC;

-- Highest-risk customers based on generated alerts.
SELECT c.customer_id, c.customer_name, c.risk_rating,
       CAST(c.payment_value AS NUMERIC) AS payment_value,
       CAST(c.alert_count AS INTEGER) AS alert_count
FROM customer_360 c
WHERE CAST(c.alert_count AS INTEGER) > 0
ORDER BY alert_count DESC, payment_value DESC
LIMIT 25;

-- Data latency observable (SQLite syntax).
SELECT source_system,
       ROUND(AVG((julianday(ingestion_timestamp) - julianday(event_timestamp)) * 86400), 2)
         AS average_latency_seconds
FROM payments
GROUP BY source_system;
