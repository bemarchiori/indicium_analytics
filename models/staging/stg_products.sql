with data_source as (
    select 
    productid
    ,name as nome
    ,productnumber as num_serie
    from {{source('adv_works','production_product')}}
    order by productid
)

select * from data_source