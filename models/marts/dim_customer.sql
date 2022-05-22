with stage_store as (
    select 
    s.businessentityid
    ,s.name as nome_loja
    ,'ST' as tipo
    from {{ref('stg_store')}} s
),
stage_person as (
    select
    *
    from {{ref('dim_person')}} s

)
select * from stage_person