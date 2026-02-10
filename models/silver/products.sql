
{{ config(materialized = 'table') }}

select
    product_id,
    trim(product_name) as product_name,
    category,
    brand,
    price::numeric(10,2) as price,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at
from {{ source('raw_tables', 'raw_products') }}