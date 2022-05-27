with stg_salesreason as (
    select 
    salesreasonid
    ,name
    ,reasontype
    ,modifieddate
    from 
    {{source('adv_works','sales_salesreason')}}
)
select * from stg_salesreason