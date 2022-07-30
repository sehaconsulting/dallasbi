{{ config(materialized='table') }}
select 1 as userid, '2022-01-01 09:00:00' as EventTime, 'sign_complete' as EventType, '2022-01-03 10:00:00' as LoadDatetime
union all
select 2 as userid, '2022-01-02 09:00:00' as EventTime, 'sign_complete' as EventType, '2022-01-03 10:00:00' as LoadDatetime
union all
select 3 as userid, '2022-01-03 09:00:00' as EventTime, 'sign_complete' as EventType, '2022-01-03 10:00:00' as LoadDatetime


