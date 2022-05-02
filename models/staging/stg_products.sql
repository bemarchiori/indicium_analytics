with 
    product_category as (
        select 
        productcategoryid
        ,name as nome
        from {{source('adv_works','production_productcategory')}}
    ),
    product_subcategory as (
        select
        productsubcategoryid
        ,pc.nome as categoria
        ,name as subcategoria

        from {{source('adv_works','production_productsubcategory')}} ps
        left join product_category pc on ps.productcategoryid=pc.productcategoryid

    ),
    product_data_source as (
    select 
    productid
    ,name as nome
    ,productnumber as num_serie
    ,ps.categoria
    ,ps.subcategoria
    from {{source('adv_works','production_product')}} product
    left join product_subcategory ps on ps.productsubcategoryid = product.productsubcategoryid
    order by productid
)

select * from product_data_source