{{ config(materialized='table') }}
select * from {{ source('crsrc', 'epic_order') }}