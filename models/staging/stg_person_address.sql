with stg_countryregion as (
	select 
		c.countryregioncode
		,c.name
		,c.modifieddate 
	from {{source('adv_works','person_countryregion')}} c 
),
	stg_stateprovince as (
		select
		s.stateprovinceid
		,s.stateprovincecode 
		,s.countryregioncode 
		,c.name  as country
		--,s.isonlystateprovinceflag 
		,s.name as stateprovince
		,s.territoryid
		--,s.rowguid
		,s.modifieddate 
		from {{source('adv_works','person_stateprovince')}} s 
		left join stg_countryregion c on c.countryregioncode =s.countryregioncode 
	),
	stg_address as (
		select 
		a.addressid
		,a.addressline1
		,a.addressline2 
		,a.city 
		,s.stateprovincecode
		,s.stateprovince
		,s.countryregioncode 
		,s.country
		,a.postalcode 
		,a.spatiallocation 
		--,a.rowguid 
		--,a.modifieddate 
		from {{source('adv_works','person_address')}} a
		left join stg_stateprovince s on s.stateprovinceid =a.stateprovinceid 
	)
	
select * from stg_address  


