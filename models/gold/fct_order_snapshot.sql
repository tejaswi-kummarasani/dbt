{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with order_base as (

    select *
    from {{ ref('orders') }}

),

customers_base (
    select *
    from {{ ref('customers') }}
)

order_totals as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        count(o.order_id) as total_orders,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date

    from customers_base c
    left join order_base o
        on c.customer_id = o.customer_id
    group by 1,2,3,4

),

business_status as (

    select *
    from {{ ref('ep_order_business_status') }}

),

final as (

    select
        o.order_id,
        o.customer_id,
        o.order_date,
        ot.order_total,
        bs.business_status,

        -- derived last modified logic
        greatest(
            o.updated_at,
            coalesce(ot.updated_at, o.updated_at)
        ) as last_modified_at,

        current_timestamp as dbt_updated_at

    from order_base o
    left join order_totals ot
        on o.order_id = ot.order_id
    left join business_status bs
        on o.order_id = bs.order_id

)

select *
from final

{% if is_incremental() %}

where last_modified_at >
    (select max(last_modified_at)
     from {{ this }})

{% endif %}