{{ config(materialized='incremental') }}

select userid, eventtime, eventtype, loaddatetime from "DEV"."TEAMA"."AUTH"
        {% if is_incremental() %}
        where loaddatetime > (select max(LastRunDate) as rd from "DEV"."TEAMA"."RunHistory" where eventname = 'Auth' )
        {% endif %}

    union all

select userid, eventtime, eventtype, loaddatetime from "DEV"."TEAMA"."ORD"
        {% if is_incremental() %}
        where loaddatetime > (select max(LastRunDate) as rd from "DEV"."TEAMA"."RunHistory" where eventname = 'Ord' )
        {% endif %}