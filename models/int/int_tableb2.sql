{{ config(materialized='table') }}
select 'person_A' as ID
union all
select 'person_A' as ID
union all
select 'person_B' as ID
union all
select 'person_C' as ID
union all
select 'person_D' as ID
union all
select 'person_E' as ID