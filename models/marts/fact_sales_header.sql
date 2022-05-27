with stg_salesorderheader as (
    select *
    from {{ref('stg_salesorderheader')}}  s 
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
dim_sales_person as (
    select 
    d.sales_person_sk as sales_person_fk
    ,d.sales_person_id
    from {{ref('dim_sales_person')}} d
 
),
transformed as (
    select 
    row_number() over (order by s.salesorderid) as sales_order_sk
    ,s.salesorderid
    ,customer.customer_fk
    ,sales_person.sales_person_fk
    ,sales_territory.sales_territory_fk
    ,bill.address_fk as bill_address_fk
    ,ship.address_fk as ship_address_fk
    ,credit.creditcard_fk
    ,orderdate
    ,duedate
    ,shipdate
    ,case s.status
    when 1 then 'In process'
    when 2 then 'Approved'
    when 3 then 'Backordered'
    when 4 then 'Rejected'
    when 5 then 'Shipped'
    when 6 then 'Cancelled' end as status
    ,subtotal
    ,taxamt
    ,freight
    ,totaldue

    from stg_salesorderheader s
    left join dim_customer customer on customer.customerid=s.customerid
    left join dim_sales_person sales_person on sales_person.sales_person_id = s.salespersonid
    left join dim_sales_territory sales_territory on sales_territory.territoryid = s.territoryid
    left join dim_address ship on ship.addressid = s.shiptoaddressid
    left join dim_address bill on bill.addressid = s.billtoaddressid
    left join dim_creditcard credit on credit.creditcardid = s.creditcardid
)
select * from transformed