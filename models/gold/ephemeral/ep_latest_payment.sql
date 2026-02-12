{{ config(materialized = 'ephemeral') }}


with ranked as (

    select
        order_id,
        payment_status,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as rn

    from {{ ref('payments') }}

)

select
    order_id,
    payment_status
from ranked
where rn = 1