{{ config(materialized='table'
            , tag = ["covid"]

        ) }}

select a.order_id, pt_id, order_date, b.update_date as LastUpdateDate, getdate() as LoadDatetime
from {{ source('crsrc', 'epic_order') }} as a
inner join {{ source('crsrc', 'epic_order_cr_stat_alter') }} as b on a.order_id = b.order_id 
where a.order_code 
    {%- if target.name == 'covid'  -%}

     
      = 'covid'

    {%- else -%}
	 
      <> 'covid'

    {%- endif -%}