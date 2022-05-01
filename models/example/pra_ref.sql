{{ config(materialized='table') }}
select
  {{ cents_to_dollars_test(100) }} as amount_usd