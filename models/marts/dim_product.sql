with 
    staging as (
        select *
        from {{ref('stg_products')}}
    ),
    transformed as (
        select 
        row_number() over (order by productid) as product_sk
        ,productid
        ,nome
        ,num_serie
        ,categoria
        ,subcategoria
        from staging
    )

select * from transformed order by productid