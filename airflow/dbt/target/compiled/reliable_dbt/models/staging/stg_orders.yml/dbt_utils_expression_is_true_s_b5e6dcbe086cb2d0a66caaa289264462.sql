



select
    1
from "analytics"."public"."stg_orders"

where not(order_amount order_amount >= 0)

