{{ config(materialized='incremental') }}

select order_id as event_id, pt_id, order_date as event_date, lastupdatedate, 'ORD' as event_type, getdate() as loaddatetime 
from {{ref('covid19_epic_ord')}}
        {% if is_incremental() %}
        where lastupdatedate > (select ifnull(max(lastupdatedate), '1900-01-01') as rd from {{this}} where event_type = 'ORD' )
        {% endif %}

    union all

select diagnosis_id  as event_id, pt_id, diagnosis_date as event_date, lastupdatedate, 'EDG' as event_type, getdate() as loaddatetime 
from {{ref('covid19_epic_diagnosis')}}
        {% if is_incremental() %}
        where lastupdatedate > (select ifnull(max(lastupdatedate), '1900-01-01') as rd from {{this}} where event_type = 'EDG' )
        {% endif %}