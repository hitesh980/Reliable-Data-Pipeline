select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from "analytics"."public"."stg_orders"

where not(order_amount order_amount >= 0)


      
    ) dbt_internal_test