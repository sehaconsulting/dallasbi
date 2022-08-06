{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}


select order_id, pt_id, order_date, getdate() as LoadDatetime 
from {{ source('crsrc', 'epic_order') }} as a

{% if is_incremental() %}
        inner join {{ source('crsrc', 'epic_order_cr_stat_alter') }} as b on a.order_id = b.order_id
        where b.update_date > (select max(LoadDatetime) as rd from  {{ this }} )
{% endif %}
and a.order_code = 'covid'
and b.order_ini = 'ORD'
