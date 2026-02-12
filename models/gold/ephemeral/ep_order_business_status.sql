{{ config(materialized='ephemeral') }}

select
    o.order_id,

    case
        when p.payment_status = 'failed' then 'payment_failed'
        when o.order_status = 'cancelled' then 'cancelled'
        when o.order_status = 'delivered' then 'completed'
        else 'in_progress'
    end as business_status
from {{ ref('orders') }} o
left join {{ ref('ep_latest_payment') }} p
    on o.order_id = p.order_id
    