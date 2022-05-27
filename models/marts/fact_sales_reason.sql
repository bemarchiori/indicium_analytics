with stg_salesheader_reason as (
    select 
    *
    from {{ref('stg_salesheaderreason')}} s
),
reason as (
    select
    s.salesreasonid
    ,s.sales_reason_sk
    from {{ref('dim_reason')}} s
),
stg_salesorderdetail as (
    select 
    s.salesorderid
    ,s.salesorderdetailid
    from {{ref('stg_salesorderdetail')}} s
),
fact_sales_reason as (
    select 
    row_number() over (order by sr.salesorderid) as fact_order_sk
    ,d.salesorderdetailid as sales_order_detail_fk
    ,r.sales_reason_sk as sales_reason_fk
    from stg_salesheader_reason sr
    left join reason r on r.salesreasonid = sr.salesreasonid
    left join stg_salesorderdetail d on d.salesorderid=sr.salesorderid

)

select * from fact_sales_reason