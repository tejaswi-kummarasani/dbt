{{ config(materialized = 'table') }}

select
    payment_id,
    order_id,
    payment_method,
    upper(payment_status) as payment_status,
    paid_amount::numeric(10,2) as paid_amount,
    paid_at,
    payment_date::timestamp as payment_date
from {{ source('raw_tables', 'raw_payments') }}