
  create view "analytics"."public"."stg_orders__dbt_tmp"
    
    
  as (
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
  );