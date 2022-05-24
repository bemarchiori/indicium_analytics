with stage_store as (
    select 
    s.businessentityid
    ,row_number() over (order by s.businessentityid) as store_sk
    ,s.name as nome_loja
    ,'ST' as tipo
    from {{ref('stg_store')}} s
    order by s.businessentityid
),
dim_person as (
    select
    d.businessentityid
    ,d.person_sk
    ,d.full_name as nome_pessoa
    ,d.tipo as tipo
    from {{ref('dim_person')}} d
),
dim_salesterritory as (
    select 
    d.sales_territory_sk
    ,d.territoryid
    ,d.territory_name
    ,d.countryregioncode
    ,d.group
    from {{ref('dim_sales_territory')}} d
),
stage_customer as (
    select 
    s.customerid
    ,s.personid
    ,s.storeid
    ,s.territoryid
    from {{ref('stg_customer')}} s
    order by s.customerid
),
union_customers as (
    select 
    s.customerid
    ,ss.nome_loja as customer_name
    ,st.territory_name 
    from stage_customer s
    left join dim_salesterritory st on st.territoryid=s.territoryid
    left join stage_store ss on ss.businessentityid = s.storeid
    where s.storeid is not null

    union all

    select 
    s.customerid
    ,p.nome_pessoa as customer_name
    ,st.territory_name 
    from stage_customer s
    left join dim_salesterritory st on st.territoryid=s.territoryid
    left join dim_person p on p.businessentityid=s.personid
    where s.storeid is null

    order by customerid
),
transformed as (
    select 
    row_number() over (order by s.customerid) as customer_sk
    ,s.customerid
    ,s.customer_name
    ,s.territory_name
    
    from union_customers s
    order by s.customerid
)
select * from transformed
order by 1
