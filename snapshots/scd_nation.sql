{% snapshot scd_nation %}
{{ config(
    target_schema='snapshots',
    unique_key='N_NATIONKEY',
    strategy='check',
    check_cols=['N_COMMENT','N_REGIONKEY', 'N_NAME','N_NATIONKEY']
) }}

SELECT
    N_COMMENT,
    N_REGIONKEY,
    N_NAME,
    N_NATIONKEY,
    base_ts
    
FROM {{ source('tpch', 'nation') }}

{% endsnapshot %}