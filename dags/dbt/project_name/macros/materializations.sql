
-- noqa: disable=TMP
{% materialization table_function, adapter='bigquery' %}
{#
This is a custom materialization method which builds table function in Bigquery.

To materialize DBT model AS table function use the following config keys:
- materialized='table_function'
- function_arguments='arg1 type, arg2 array<type>'
#}
    {%- set database = model['database'] -%}
    {%- set schema = model['schema'] -%}

    {%- set identifier = "`{}.{}.{}`".format(database, schema, model['alias']) -%}

    {%- set table_function_sql -%}
    CREATE OR REPLACE TABLE FUNCTION {{ identifier }}({{ model.config['function_arguments'] or '' }})
    AS (
        {{ sql }}
    );
    {%- endset -%}

    {% call statement('main') -%}
    {{ table_function_sql }}
    {%- endcall %}

    {%- set relation = api.Relation.create(database=model['database'], schema=schema, identifier=model['alias'], type='table') -%}
    {{ return({'relations': [relation]}) }}
{% endmaterialization %}
