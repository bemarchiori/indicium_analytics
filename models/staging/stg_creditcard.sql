with stage as (
    select 
    creditcardid
    ,cardtype
    ,cardnumber
    ,expmonth
    ,expyear
    ,modifieddate
    from
     {{source('adv_works','sales_creditcard')}} s 
    order by s.creditcardid
)

select * from stage