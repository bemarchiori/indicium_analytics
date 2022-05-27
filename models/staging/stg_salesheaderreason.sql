with 
stg_salesheaderreason as (
    select 
    *
    from 
    {{source('adv_works','sales_salesorderheadersalesreason')}} sr
)

select * from stg_salesheaderreason