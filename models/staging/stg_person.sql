with data_source as (
    SELECT businessentityid
        ,persontype
        --,namestyle
        --,title
        ,firstname
        ,middlename
        ,lastname
        --,suffix
        --,emailpromotion
        --,additionalcontactinfo
        --,demographics
        ,rowguid
        ,modifieddate

FROM {{source('adv_works','person_person')}}
order by businessentityid 
)

select * from data_source