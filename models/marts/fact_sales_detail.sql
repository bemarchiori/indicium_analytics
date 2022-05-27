with 
stage_sales_order_detail as (
    select 
    s.salesorderid
    ,s.salesorderdetailid
    ,s.orderqty
    ,s.productid
    ,s.unitprice
    ,s.unitpricediscount

    from {{ref('stg_salesorderdetail')}} s
 
),
dim_product as (
    select 
    d.product_sk as product_fk
    ,d.productid
    from {{ref('dim_product')}} d

),
transformed as (
    select
    s.salesorderid
    ,s.salesorderdetailid
    ,product.product_fk
    ,s.orderqty
    ,s.unitprice
    ,s.unitpricediscount
    from stage_sales_order_detail s 
    left join dim_product product on product.productid = s.productid
)

select * from transformed