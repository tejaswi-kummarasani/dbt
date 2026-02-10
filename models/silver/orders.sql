{{ config(materialized = 'table') }}

select
    order_id,
    customer_id,
    upper(order_status) as order_status,
    order_date::date as order_date,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('raw_tables', 'raw_orders') }}