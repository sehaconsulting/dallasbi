{{ config(materialized='incremental'
        , unique_key = 'order_id')
             }}
/*
select 1 as userid, '2022-01-01 08:00:00' as EventTime, 'ord_placed' as EventType, '2022-01-03 11:00:00' as LoadDatetime
union all
select 2 as userid, '2022-01-02 08:00:00' as EventTime, 'ord_placed' as EventType, '2022-01-03 11:00:00' as LoadDatetime
union all
select 3 as userid, '2022-01-03 08:00:00' as EventTime, 'ord_placed' as EventType, '2022-01-03 11:00:00' as LoadDatetime
*/

select order_id, pt_id, order_date, getdate() as LoadDatetime 
from {{ source('crsrc', 'epic_order') }} as a

    {% if is_incremental() %}
        inner join {{ source('crsrc', 'epic_order_cr_stat_alter') }} as b on a.order_id = b.order_id
        where b.update_date > (select max(LoadDatetime) as rd from  {{ this }} )
    {% endif %}
and a.order_code = 'covid'
and b.order_ini = 'ORD'
