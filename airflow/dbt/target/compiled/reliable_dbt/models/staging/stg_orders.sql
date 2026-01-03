with source as (
    select *
    from "analytics"."public"."raw_orders"
)

select 
   order_id,
   user_id,
   order_amount,
   upper(currency) as currency,
   order_ts,
   status
from source