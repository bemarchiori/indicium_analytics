with stage as (
    select
    salesorderid
    ,revisionnumber
    ,orderdate
    ,duedate
    ,shipdate
    ,status
    ,onlineorderflag
    ,purchaseordernumber
    --,accountnumber
    ,customerid
    ,salespersonid
    ,territoryid
    ,billtoaddressid
    ,shiptoaddressid
    --,shipmethodid
    ,creditcardid
    --,creditcardapprovalcode
    --,currencyrateid
    ,subtotal
    ,taxamt
    ,freight
    ,totaldue
    --,comment
    --,rowguid
    modifieddate
    from {{source('adv_works','sales_salesorderheader')}}
)

select * from stage
