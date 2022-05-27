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
person as (
    select 
    businessentityid as person_id
    ,concat(s.firstname,' ',coalesce(middlename,''),' ',lastname) as fullname
    from {{ref('stg_person')}} s
),
transformed as (
    select 
    row_number () over (order by s.businessentityid) as sales_person_sk
    ,s.businessentityid as sales_person_id
    ,p.fullname
    ,s.territoryid
    from stage s
    left join person p on p.person_id = s.businessentityid
)

select * from transformed