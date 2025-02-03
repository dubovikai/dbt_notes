-- Removes tables and views from the given run configuration
-- Usage in production:
--    dbt run-operation cleanup_dataset
-- To only see the commands that it is about to perform:
--    dbt run-operation cleanup_dataset --args '{"dry_run": True}'
{% macro cleanup_dataset(dry_run=False) %}
    {% if execute %}
        {% set current_model_locations = {} %}

        {% for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed", "snapshot"]) %}
            {% if not node.database in current_model_locations %}
                {% do current_model_locations.update({node.database: {}}) %}
            {% endif %}
            {% if not node.schema in current_model_locations[node.database] %}
                {% do current_model_locations[node.database].update({node.schema: []}) %}
            {% endif %}
            {% set table_name = node.alias if node.alias else node.name %}
            {% do current_model_locations[node.database][node.schema].append(table_name) %}
        {% endfor %}
    {% endif %}

    {% set cleanup_query %}

        WITH models_to_drop AS (
            {% for database in current_model_locations.keys() %}
                {% if loop.index > 1 %} union all {% endif %}
                {% for dataset, tables  in current_model_locations[database].items() %}
                    {% if loop.index > 1 %} union all {% endif %}
                    SELECT
                        table_type,
                        table_catalog,
                        table_schema,
                        table_name,
                        CASE
                            WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                            WHEN table_type = 'VIEW' THEN 'VIEW'
                            WHEN table_type = 'EXTERNAL' THEN 'EXTERNAL TABLE'
                        END AS relation_type,
                        ARRAY_TO_STRING([table_catalog, table_schema, table_name], '.') AS relation_name
                    FROM {{ dataset }}.INFORMATION_SCHEMA.TABLES
                    WHERE NOT (table_name IN ('{{ "', '".join(tables) }}'))

                    UNION ALL

                    SELECT
                        routine_type,
                        routine_catalog,
                        routine_schema,
                        routine_name,
                        CASE
                            WHEN routine_type = 'TABLE FUNCTION' THEN 'TABLE FUNCTION'
                        END AS relation_type,
                        ARRAY_TO_STRING([routine_catalog, routine_schema, routine_name], '.') AS relation_name
                    FROM {{ dataset }}.INFORMATION_SCHEMA.ROUTINES
                    WHERE REGEXP_REPLACE(routine_name, "^tf_", "") NOT IN ('{{ "', '".join(tables) }}')
                        AND routine_name NOT IN ('{{ "', '".join(tables) }}')
                {% endfor %}
            {% endfor %}
        ),
        drop_commands AS (
            SELECT 'DROP ' || relation_type || ' `' || relation_name || '`;' AS command
            FROM models_to_drop
        )

        SELECT command
        FROM drop_commands
        -- intentionally exclude unhandled table_types, including 'external table`
        WHERE command IS NOT NULL

    {% endset %}
    {% set drop_commands = run_query(cleanup_query).columns[0].values() %}
    {% if drop_commands %}
        {% for drop_command in drop_commands %}
            {% do log(drop_command, True) %}
            {% if dry_run | as_bool == False %}
                {% do run_query(drop_command) %}
            {% endif %}
        {% endfor %}
    {% else %}
        {% do log('No relations to clean.', True) %}
    {% endif %}
{%- endmacro -%}