with stage as (
    select *
    from {{ref('stg_salesterritory')}} s

)
,transformed as (
    select
    row_number() over (order by s.territoryid) as sales_territory_sk
    ,s.territoryid
    ,s.territory_name
    ,s.countryregioncode
    ,s.group

    from stage s
)

select * from stage
