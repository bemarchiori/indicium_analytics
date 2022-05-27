with stg_salesreason as (
    select 
    salesreasonid
    ,name
    ,reasontype
    ,modifieddate
    from 
    {{source('adv_works','sales_salesreason')}}
),
transformed as (
    select 
    row_number () over (order by salesreasonid) as sales_reason_sk
    ,salesreasonid 
    ,name
    ,reasontype
    from stg_salesreason
)
select * from transformed