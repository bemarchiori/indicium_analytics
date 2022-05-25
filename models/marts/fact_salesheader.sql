with 



,stage_sales_order_header as (
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
)

select * from stage_sales_order_header