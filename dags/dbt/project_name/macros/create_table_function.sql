{% macro create_table_function() %}
{#
This is a post-hook which creates table function with filter by data_owner_id
#}
    {%- if execute and flags.WHICH in ('run', 'build') -%}
        {% set tf_name = this.database+"."+this.schema+".tf_"+this.identifier %}

        CREATE OR REPLACE TABLE FUNCTION `{{ tf_name }}`(data_owner_id_array ARRAY<STRING>) AS
        SELECT * FROM {{ this }}
        WHERE TO_HEX(data_owner_id) IN UNNEST(data_owner_id_array)

        {{ log("Table function created: "+tf_name, info=True) }}
    {%- endif -%}
{% endmacro %}