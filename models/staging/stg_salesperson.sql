with stg_salesperson as (
    select 
    businessentityid
    ,territoryid
    ,salesquota
    ,bonus
    ,commissionpct
    ,salesytd
    ,saleslastyear
    --,rowguid
    ,modifieddate
    from {{source('adv_works','sales_salesperson')}} st

)
select * from stg_salesperson