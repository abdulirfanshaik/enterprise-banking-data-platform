select
    cast(event_timestamp as date) as event_date,
    payment_type,
    count(*) as transaction_count,
    sum(amount) as transaction_value,
    sum(case when status = 'SUCCESS' then 1 else 0 end) as success_count,
    sum(case when status = 'FAILED' then 1 else 0 end) as failed_count
from {{ ref('stg_payments') }}
group by 1, 2
