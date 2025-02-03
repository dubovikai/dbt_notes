{% snapshot model_name_ctl_snapshot %}
{{
    config(
        strategy='timestamp',
        unique_key='row_id',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
        post_hook='{{ delete_old_snapshots(this) }}'
    )
}}

{{ 
    create_control_snapshot(
        model_name='model_name',
        groupings_labels = {'month': ['event_month'], 'total': []},
        ctl_values_list=[
            'event_id',
            'client_id',
            'event_type'
        ],
        calc_columns={
            "event_month": "DATE_TRUNC(DATE(event_month), MONTH)"
        },
        filters=['DATE(event_date) <= CURRENT_DATE()']
    )
}}

{% endsnapshot %}
