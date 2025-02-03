{% test check_base_counters(model, default_check = {}, checks=[]) %}
{#-
Comparison of counter values.
Expected input table: snapshot with label column as group identifier.

Parameters:
- model: Name of the dbt snapshot used for comparison.
- checks: formatted list of checks:
  checks = [
    {
      "label": "total",
      "limit_range": {"cohort_id": [100, 200]},
      "ctl_values_list": [
        "cohort_id",
        "anomaly_id",
        "table_id",
        "column_id",
        "report_name"
      ],
      "all_diff_limit": 0.9,
      "all_not_decreased": true,
      "all_not_zero": true,
      "filters": ["previous_state.cohort_id = 123"]
    }
  ], where:
        - label: group identifier, see dags/dbt/project_name/macros/utils.sql macro create_control_snapshot 
        - limit_range: Dictionary with counter names from snapshot table as keys and absolute limits as values.
        - ctl_values_list: List of snapshot columns for checks: all_diff_limit, all_not_decreased, all_not_zero to be applied 
        - all_diff_limit: Maximum allowed relative difference.
        - all_not_decreased: Checking if current counter value is less than previous one.
        - all_not_zero: Comparison of counter values with 0
        - filters: filters to be applied for checking
- default_check: has the same structure as checks element. Parameters from this object are used if not passed in check object
    for limit_range default_check config will be updated by check config
-#}
    {%- set compiled_checks = [] %}
    {%- for check in checks %}
        {%- set compiled_check = {
            "label": "",
            "label_condition" : "",
            "filters": [],
            "check_conditions": []
        } %}

        {%- do compiled_check.update({"label": check["label"]}) %}
        {%- do compiled_check.update({"label_condition": "label = {label_name}".format(label_name=check["label"])}) %}

        {% set limit_range = default_check.get('limit_range', {}) %}
        {% do limit_range.update(check.get('limit_range', {}) ) %}
        {%- for col, range in limit_range.items() %}
            {%- set lower_limit = "current_state.{col} < {min_value}".format(col=col, min_value=range[0]) if range[0] != None else "" %}
            {%- set upper_limit = "current_state.{col} > {max_value}".format(col=col, max_value=range[1]) if range[1] != None else "" %}
            {%- set cond = lower_limit + (' OR ' if lower_limit and upper_limit else '') + upper_limit %}
            {%- do compiled_check["check_conditions"].append({
                "col": col,
                "check": "limit_range: [{min_value}, {max_value}]".format(min_value=range[0], max_value=range[1]),
                "cond": cond})
            %}
        {%- endfor %}

        {% set all_diff_limit = check.get('all_diff_limit', default_check.get('all_diff_limit')) %}
        {%- if all_diff_limit %}
            {%- for col in (check.get('ctl_values_list', []) + default_check.get('ctl_values_list', [])) | unique %}
                {%- set diff ='IFNULL(ABS(SAFE_DIVIDE(current_state.{col}, previous_state.{col}) - 1), 1)'.format(col=col) %}
                {%- set cond = "{diff} >= {limit}".format(diff=diff, limit=all_diff_limit) %}
                {%- do compiled_check["check_conditions"].append({
                    "col": col,
                    "check": "diff_limit: {limit}".format(limit=all_diff_limit),
                    "cond": cond
                }) %}
            {%- endfor %}
        {%- endif %}

        {%- if check.get('all_not_decreased', default_check.get('all_not_decreased', False)) == True %}
            {%- for col in (check.get('ctl_values_list', []) + default_check.get('ctl_values_list', [])) | unique %}
                {%- set cond = "previous_state.{col} > current_state.{col}".format(col=col) %}
                {%- do compiled_check["check_conditions"].append({"col": col, "check": "not decreased", "cond": cond}) %}
            {%- endfor %}
        {%- endif %}

        {%- if check.get('all_not_zero', default_check.get('all_not_zero', False)) == True %}
            {%- for col in (check.get('ctl_values_list', []) + default_check.get('ctl_values_list', [])) | unique %}
                {%- set cond = "IFNULL(current_state.{col}, 0) = 0".format(col=col) %}
                {%- do compiled_check["check_conditions"].append({"col": col, "check": "not zero", "cond": cond}) %}
            {%- endfor %}
        {%- endif %}

        {%- do compiled_check["filters"].extend(check.get('filters', [])) %}
        {%- do compiled_check["filters"].extend(default_check.get('filters', [])) %}

        {%- do compiled_checks.append(compiled_check) %}

    {%- endfor %}


    {%- set all_columns = [] %}
    {%- for check in checks %}
        {%- do all_columns.extend(check.get('ctl_values_list', [])) %}
        {%- do all_columns.extend(check.get('limit', {}).keys() | list) %}

        {%- do all_columns.extend(default_check.get('ctl_values_list', [])) %}
        {%- do all_columns.extend(default_check.get('limit', {}).keys() | list) %}
    {%- endfor %}
    {%- set all_columns = all_columns | unique | sort %}

    WITH current_state AS (
        SELECT *
        FROM {{ model }}
        WHERE dbt_valid_to IS NULL
    ),

    previous_state AS (
        SELECT *
        FROM {{ model }}
        QUALIFY dbt_valid_to = MAX(dbt_valid_to) OVER (PARTITION BY row_id)
    )

    {%- for compiled_check in compiled_checks %}
    SELECT
        previous_state.label,
        previous_state.row_cohort,
        previous_state.row_id,
        {%- for col in all_columns %}
            previous_state.{{ col }} AS previous_{{ col }},
            current_state.{{ col }} AS current_{{ col }},
            {%- for cond in compiled_check['check_conditions'] if cond['col'] == col %}
                {{ 'SPLIT(CONCAT(' if loop.first }}
                    IF({{ cond['cond'] }}, "{{ cond['check'] }}&&", ""){{ ',' if not loop.last }}
                {{ '), "&&") AS failed_checks_{col},'.format(col=col) if loop.last }}
            {%- endfor -%}
        {%- endfor -%}
        
    FROM previous_state
    LEFT JOIN current_state
        ON previous_state.row_id = current_state.row_id
    WHERE previous_state.label = "{{ compiled_check['label'] }}"
    
        {%- for fltr in compiled_check['filters'] %}
        AND {{ fltr }}
        {%- endfor %}

        AND (
            {% for cond in compiled_check['check_conditions'] %}
                {{ cond['cond'] }} {{ 'OR' if not loop.last }}
            {% endfor %}
        )

    {{- "UNION ALL" if not loop.last }}

    {%- endfor %}

{% endtest %}
