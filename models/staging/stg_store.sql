with stg_store as (
    select
    businessentityid
    ,name
    ,salespersonid
    ,demographics
    --,rowguid
    ,modifieddate   
    from {{source('adv_works','sales_store')}} st

)

select * from stg_store