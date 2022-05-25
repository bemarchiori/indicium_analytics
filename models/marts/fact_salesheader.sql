with stage_sales_order_header as (
    select
    s.salesorderid
    ,s.orderdate
    ,s.duedate
    ,s.shipdate
    ,s.status
    ,s.onlineorderflag
    ,s.customerid
    ,s.territoryid
    ,s.shiptoaddressid
    ,s.billtoaddressid
    ,s.creditcardid
    ,s.subtotal
    ,s.taxamt
    ,s.freight
    from {{ref('stg_salesorderheader')}} s
),
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
dim_customer as (
    select d.customer_sk as customer_fk
    ,d.customerid
    from {{ref('dim_customer')}} d
),
dim_address as (
   select d.address_sk as address_fk
    ,d.addressid
    from {{ref('dim_address')}} d
),
dim_creditcard as (
   select d.creditcard_sk as creditcard_fk
    ,d.creditcardid
    from {{ref('dim_creditcard')}} d
),
dim_sales_territory as (
   select d.sales_territory_sk as sales_territory_fk
    ,d.territoryid
    from {{ref('dim_sales_territory')}} d
),
dim_sales_reason as (
    select 
    d.sales_reason_sk as sales_reason_fk
    ,d.salesorderid
    from {{ref('dim_sales_reason')}} d

),
sales_header_fact as (
    select
    s.salesorderid as sales_order_id
    ,detail.salesorderdetailid as sales_order_detail_id
    ,customer.customer_fk
    ,shipaddress.address_fk as ship_address_fk
    ,billaddress.address_fk as bill_address_fk
    ,creditcard.creditcard_fk
    ,territory.sales_territory_fk
    ,reason.sales_reason_fk
    ,product.product_fk
    ,s.orderdate
    ,s.duedate
    ,s.shipdate
    ,case s.status
    when 1 then 'In process'
    when 2 then 'Approved'
    when 3 then 'Backordered'
    when 4 then 'Rejected'
    when 5 then 'Shipped'
    when 6 then 'Cancelled' end as status
    ,s.onlineorderflag
    ,detail.orderqty as order_quantity
    ,detail.unitprice as unit_price
    ,detail.unitpricediscount as unit_price_discount
    ,subtotal
    ,taxamt
    ,freight
    from stage_sales_order_header s
    left join stage_sales_order_detail detail on detail.salesorderid=s.salesorderid
    left join dim_product product on product.productid = detail.productid
    left join dim_customer customer on customer.customerid = s.customerid
    left join dim_creditcard creditcard on creditcard.creditcardid = s.creditcardid
    left join dim_address billaddress on billaddress.addressid = s.billtoaddressid
    left join dim_address shipaddress on shipaddress.addressid = s.shiptoaddressid
    left join dim_sales_territory territory on territory.territoryid = s.territoryid
    left join dim_sales_reason reason on reason.salesorderid = s.salesorderid

)

select * from sales_header_fact