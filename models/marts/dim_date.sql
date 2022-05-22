with stage as (
    select distinct
    s.orderdate
    from {{ref('stg_salesorderheader')}} s
    order by s.orderdate 
)
,transformed as (
    select 
    row_number() over (order by s.orderdate) as sk_date
    ,date(s.orderdate) as date
    ,extract (DAY from s.orderdate) as dia
    ,extract (MONTH from s.orderdate) as mes
    ,extract (YEAR from s.orderdate) as ano
    ,extract (week from s.orderdate) as semana_ano
    ,case extract (DAYOFWEEK from s.orderdate)
    when 1 then 'Domingo'
    when 2 then 'Segunda-feira'
    when 3 then 'Terça-feira'
    when 4 then 'Quarta-feira'
    when 5 then 'Quinta-feira'
    when 6 then 'Sexta-feira'
    when 7 then 'Sábado' end as dia_semana
    ,extract (QUARTER from s.orderdate) as trimestre_ano

    from stage s
    order by s.orderdate
)

select * from transformed