with stage as (
    select
    *
    from {{ref('stg_salesreason')}} s
    
)

select * from stage