with stage as (
    select * 
    from {{ref('stg_person')}}
),
transformed as (
    select 
    s.businessentityid
    ,row_number() over (order by s.businessentityid) as person_sk
    ,s.firstname
    ,s.middlename
    ,s.lastname
    ,s.firstname||' '||coalesce(s.middlename,'')||' '||coalesce(s.lastname,'') as full_name

    from stage s
)

select * from transformed t
order by t.person_sk