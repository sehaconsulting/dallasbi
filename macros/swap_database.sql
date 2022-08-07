# macros/swap_database.sql
{% macro swap_database() %}

    {% set sql='alter database prod swap with dev' %}
    {% do run_query(sql) %}
    {{ log("database swapped", info=True) }}

{% endmacro %}