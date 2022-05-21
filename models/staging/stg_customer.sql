with stg_customer as (
    select
    customerid
    ,personid
    ,storeid
    ,territoryid
    ,rowguid
    ,modifieddate
    from {{source('adv_works','sales_customer')}} st

)

select * from stg_customer