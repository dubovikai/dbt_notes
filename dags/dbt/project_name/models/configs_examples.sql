{{-
    config(
        materialized='table_function',
        function_arguments='start_date date, end_date date'
    )
-}}

{{- 
  config(
    materialized='table',
    cluster_by='cluster_field',
    post_hook=['{{ create_table_function() }}'],
    tags=['some_tag'],
    min_period_between_updates_hours=2
  )
-}}