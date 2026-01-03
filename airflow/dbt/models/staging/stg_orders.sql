with source as (
    select *
    from {{ source('raw', 'raw_orders') }}
)

select 
   order_id,
   user_id,
   order_amount,
   upper(currency) as currency,
   order_ts,
   status
from source


