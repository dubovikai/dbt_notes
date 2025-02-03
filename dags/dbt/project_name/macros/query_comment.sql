{% macro query_comment(node) %}
{#
This is a BigQuery job labels provider. Returns Key: Value dict of job labels names and values.
#}
    {%- set comment_dict = {} -%}
    {%- do comment_dict.update(bq_job_type='dbt_process') -%}
    {%- if node is not none -%}
      {%- do comment_dict.update(additional_info=node.identifier) -%}
    {% else %}
      {%- do comment_dict.update(node_id='internal') -%}
    {%- endif -%}
    {% do return(tojson(comment_dict)) %}
{% endmacro %}
