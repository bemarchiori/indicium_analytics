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
    ,s.billtoaddressid
    ,s.creditcardid
    ,s.subtotal
    ,s.taxamt
    ,s.freight
    from {{ref('stg_salesorderheader')}} s
),
dim_customer as (
    select d.customer_sk as customer_fk
    ,d.customerid
    from {{ref('dim_customer')}} d
),
dim_address as (
   select d.address_sk as customer_fk
    ,d.address_id
    from {{ref('dim_address')}} d

)
select * from dim_address