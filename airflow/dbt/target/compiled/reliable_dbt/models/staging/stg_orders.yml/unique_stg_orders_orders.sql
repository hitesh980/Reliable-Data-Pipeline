
    
    

select
    orders as unique_field,
    count(*) as n_records

from "analytics"."public"."stg_orders"
where orders is not null
group by orders
having count(*) > 1


