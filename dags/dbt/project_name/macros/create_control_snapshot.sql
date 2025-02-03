{% macro create_control_snapshot(
    model_name,
    groupings_labels,
    ctl_values_list=[],
    calc_columns={},
    filters=[]
) %}
{#
SELECT statement defining standard control snapshot for using in tests
groupings_labels - Dict[str, List[str]], where key - row "label", value - "list of groupings"
    rows which grouped by "list of groupings" will be marked with "label"
#}
    {%- set list_of_groupings = [] -%}
    {%- for grs in groupings_labels.values() -%}
        {%- set _ = list_of_groupings.extend(grs) -%}
    {%- endfor -%}
    {%- set list_of_unique_groupings = list_of_groupings | unique | sort -%}

    WITH snap AS (
        SELECT
            CURRENT_TIMESTAMP() AS updated_at,

            {%- for gr in list_of_unique_groupings %}
                {{ calc_columns.get(gr, gr) }} AS {{ gr }},
            {%- endfor %}

            CASE
            {%- for label_name, label_grs in groupings_labels.items() %}
            WHEN TRUE
                {%- for gr in list_of_unique_groupings %}
                    AND GROUPING({{ calc_columns.get(gr, gr) }}) = {{ '0' if gr in label_grs else '1' }}
                {%- endfor %}
                THEN '{{ label_name }}'
            {%- endfor %}
            ELSE 'unknown_label' END AS label,
            
            COUNT(*) AS all_count,

            {%- for column in ctl_values_list %}
                {%- if column in calc_columns %}
                    {{ calc_columns[column] }} AS {{ column }},
                {%- else%}
                    COUNT(DISTINCT {{ column }}) AS {{ column }},
                {%- endif %}
            {% endfor %}

        FROM {{ ref(model_name) }} AS members_eligibility
        {% if filters %}
        WHERE {{ filters | join(' AND ') }}
        {% endif %}
        GROUP BY GROUPING SETS (
            {%- for grs in groupings_labels.values() %}
                ({{ grs | join(', ') }}){{ ',' if not loop.last }}
            {%- endfor %}
        )
    )

    SELECT
        *,
        FARM_FINGERPRINT(TO_JSON_STRING(
            STRUCT(

                {%- for gr in list_of_unique_groupings %}
                    {{ gr }},
                {%- endfor %}

                label
            )
        )) AS row_id,

        CASE label
        {%- for label_name, label_grs in groupings_labels.items() %}
            WHEN "{{ label_name }}" THEN TO_JSON_STRING(STRUCT(label {{ ',' if label_grs }}{{ label_grs | sort | join(",") }}))
        {%- endfor %}
        ELSE 'unknown_cohort'
        END AS row_cohort
    FROM snap
{% endmacro %}
