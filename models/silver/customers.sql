{{ config(materialized = 'table') }}

select
    customer_id,
    trim(first_name) as first_name,
    trim(last_name)  as last_name,
    lower(email)     as email,
    phone,
    created_at::timestamp as created_at,
    updated_at::timestamp as updated_at,
    city
from {{ source('raw_tables', 'raw_customers') }}

