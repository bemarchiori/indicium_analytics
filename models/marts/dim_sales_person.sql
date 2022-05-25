with stage as (
    select
    s.businessentityid
    ,s.territoryid
    ,s.salesquota
    ,s.bonus
    ,s.commissionpct
    ,s.salesytd
    ,s.saleslastyear
    from {{ref('stg_salesperson')}} s
),
transformed as (
    select 
    row_number () over (order by s.businessentityid) as sales_person_sk
    ,s.businessentityid
    ,s.territoryid
    from stage s
)

select * from transformed