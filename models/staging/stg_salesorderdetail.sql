with stage as (
    select 
    salesorderid
    ,salesorderdetailid
    --,carriertrackingnumber
    ,orderqty
    ,productid
    ,specialofferid
    ,unitprice
    ,unitpricediscount
    --,rowguid
    ,modifieddate
    from
    {{source('adv_works','sales_salesorderdetail')}}
    order by salesorderid,salesorderdetailid
)

select * from stage