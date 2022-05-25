with stage as (
    select *
    from {{ref('stg_person_address')}} s

),
transformed as (
    select 
    row_number() over(order by s.addressid) as address_sk
    ,s.addressline1
    ,s.addressline2
    ,s.city
    ,s.stateprovincecode
    ,s.countryregioncode
    ,s.country
    from stage s

)

select * from transformed