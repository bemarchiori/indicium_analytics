with stg_salesorderheader as (
    select *
    from {{ref('stg_salesorderheader')}}  s 
),
transformed as (
     select 1 

)
select * from stg_salesorderheader