# macros/swap_table.sql
{% macro swap_table(tablename) %}

    {% set sql='alter table dev.final.' ~ tablename ~ ' swap with dev.stage.' ~ tablename %}
    {% do run_query(sql) %}
    {{ log(sql, info=True) }}

{% endmacro %}