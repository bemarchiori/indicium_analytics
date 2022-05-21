with stg_salesterritory as (
        select
        st.territoryid
        ,st.name as territory_name
        ,st.countryregioncode
        ,st.group
        ,st.salesytd
        ,st.saleslastyear
        ,st.costytd
        ,st.costlastyear
        --,st.rowguid
        ,st.modifieddate  
        from {{source('adv_works','sales_salesterritory')}} st
        order by st.territoryid
    )

select * from stg_salesterritory
