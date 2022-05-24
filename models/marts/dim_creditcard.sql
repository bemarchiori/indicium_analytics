with stage as (
    select 
    s.creditcardid
    ,row_number() over (order by s.creditcardid) as creditcard_sk
    ,s.cardtype
    ,s.cardnumber
    ,s.expmonth
    ,s.expyear
    ,s.modifieddate
    from {{ref('stg_creditcard')}} s

),
transformed as (
    select 
    s.creditcardid
    ,row_number() over (order by s.creditcardid) as creditcard_sk
    ,s.cardtype
    from stage s
)

select * from transformed