select
    payment_id,
    source_system,
    source_account,
    destination_account,
    customer_id,
    payment_type,
    cast(amount as decimal(18, 2)) as amount,
    currency,
    cast(event_timestamp as timestamp) as event_timestamp,
    status,
    merchant_category,
    device_id,
    cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
    source_file
from read_csv_auto('../data/processed/silver/payments.csv', header = true)
