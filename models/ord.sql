{{ config(materialized='table') }}
select 1 as userid, '2022-01-01 08:00:00' as EventTime, 'ord_placed' as EventType, '2022-01-03 11:00:00' as LoadDatetime
union all
select 2 as userid, '2022-01-02 08:00:00' as EventTime, 'ord_placed' as EventType, '2022-01-03 11:00:00' as LoadDatetime
union all
select 3 as userid, '2022-01-03 08:00:00' as EventTime, 'ord_placed' as EventType, '2022-01-03 11:00:00' as LoadDatetime