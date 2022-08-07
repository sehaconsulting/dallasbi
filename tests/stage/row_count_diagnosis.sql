with cc as (
select ifnull(sum(1), 0) as f 
from {{ref('covid19_epic_diagnosis')}}
where cast(loaddatetime as date) = '2022-08-06' --based on system date
)


select *
from cc
where f = 0