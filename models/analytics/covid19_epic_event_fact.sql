{{ config(materialized='incremental') }}

select order_id, pt_id, order_date, lastupdatedate, 'ORD' as SourceType, getdate() as loaddatetime 
from {{ref('covid19_epic_ord')}}
        {% if is_incremental() %}
        where lastupdatedate > (select ifnull(max(lastupdatedate), '1900-01-01' as rd from {{this}} where SourceType = 'ORD' )
        {% endif %}

    union all

select diagnosis_id, pt_id, diagnosis_date, lastupdatedate, 'EDG' as SourceType, getdate() as loaddatetime 
from {{ref('covid19_epic_diagnosis')}}
        {% if is_incremental() %}
        where lastupdatedate > (select ifnull(max(lastupdatedate), '1900-01-01' as rd from {{this}} where SourceType = 'EDG' )
        {% endif %}