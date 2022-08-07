{{
    config(
        materialized='incremental',
        unique_key='diagnosis_id',
        post_hook=[ "insert into stage.RunHistory values ({{ this }}, getdate());" ]
    )
}}

with a as (
select a.diagnosis_id, pt_id, diagnosis_date, b.update_date as LastUpdateDate 
from {{ source('crsrc', 'epic_diagnosis') }} as a
inner join {{ source('crsrc', 'epic_diagnosis_cr_stat_alter') }} as b on a.diagnosis_id = b.diagnosis_id
where a.diagnosis_code = 'covid'
and b.diagnosis_ini = 'EDG'
)

select * , getdate() as LoadDatetime from a
{% if is_incremental() %}
        where LastUpdateDate  > (select ifnull(max(LastUpdateDate), '1900-01-01')as rd from  {{ this }} )     
{% endif %}
