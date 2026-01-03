select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select orders
from "analytics"."public"."stg_orders"
where orders is null



      
    ) dbt_internal_test