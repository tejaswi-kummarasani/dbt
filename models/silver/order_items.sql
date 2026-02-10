{{ config(materialized = 'table') }}


select
    order_item_id,
    order_id,
    product_id,
    quantity::int as quantity,
    unit_price::numeric(10,2) as unit_price,
    quantity * unit_price as item_total,
    created_at::timestamp as created_at
from {{ source('raw_tables', 'raw_order_items') }}