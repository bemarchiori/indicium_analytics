with stg_salesheader_reason(
    select 
    * 
    from {{ref('stg_salesheaderreason')}}
)

selct * from stg_salesheader_reason