with stage as (
    select
    s.salesorderid
    ,s.salesreasonid
    ,s.reason
    ,s.reasontype
    from {{ref('stg_salesreason')}} s
    
),
transformed as (
    select 
    row_number() over (order by salesorderid) as sales_reason_sk
    ,s.salesorderid
    ,s.salesreasonid
    ,s.reason
    ,s.reasontype
    from stage s

)

select * from transformed