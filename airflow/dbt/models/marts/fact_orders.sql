with base as (
    select 
       order_id,
       user_id,
       order_amount,
       currency,
       order_ts,
       status
    from {{ref('stg_orders')}}

)

select
   user_id,
   count(order_id) as total_orders,
   sum(order_amount) as total_orders,
   min(order_ts) as first_order,
   max(order_ts) as last_order

from base
group by user_id