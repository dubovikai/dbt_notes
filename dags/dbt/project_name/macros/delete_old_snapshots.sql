{% macro delete_old_snapshots(target_table) %}
{#
Leave only two latest two months data in the snapshot
#}
    DELETE FROM {{ target_table }}
    WHERE dbt_valid_to < TIMESTAMP(DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH))
{% endmacro %}
