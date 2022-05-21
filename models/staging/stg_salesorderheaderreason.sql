with stg_salesreason as (
    select 
    salesreasonid
    ,name
    ,reasontype
    ,modifieddate
    from 
    {{source('adv_works','sales_salesreason')}}
)
,stg_salesheaderreason as (
    select 
    sr.salesorderid
    ,sr.salesreasonid
    ,r.name as reason
    ,r.reasontype as reasontype
    ,sr.modifieddate
    from 
    {{source('adv_works','sales_salesorderheadersalesreason')}} sr
    left join stg_salesreason r on r.salesreasonid = sr.salesreasonid

)

select * from stg_salesheaderreason