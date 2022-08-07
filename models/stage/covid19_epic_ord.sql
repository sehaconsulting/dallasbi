{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}

with a as (
select a.order_id, pt_id, order_date, b.update_date as LastUpdateDate 
from {{ source('crsrc', 'epic_order') }} as a
inner join {{ source('crsrc', 'epic_order_cr_stat_alter') }} as b on a.order_id = b.order_id
where a.order_code = 'covid'
and b.order_ini = 'ORD'
)

select * , getdate() as LoadDatetime from a
{% if is_incremental() %}
        where LastUpdateDate  > (select ifnull(max(LastUpdateDate), '1900-01-01')as rd from  {{ this }} )     
{% endif %}

