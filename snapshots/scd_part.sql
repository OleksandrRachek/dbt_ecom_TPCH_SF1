{% snapshot scd_part %}
{{ config(
    target_schema='snapshots',
    unique_key='P_PARTKEY',
    strategy='check',
    check_cols=['P_BRAND', 'P_COMMENT', 'P_MFGR',' P_NAME',' P_SIZE', 'P_TYPE', 'P_PARTKEY', 'P_RETAILPRICE', 'P_CONTAINER']
) }}

SELECT
    P_BRAND,
    P_COMMENT,
    P_MFGR,
    P_NAME,
    P_SIZE,
    P_TYPE,
    P_PARTKEY,
    P_RETAILPRICE,
    P_CONTAINER,
    base_ts
FROM {{ source('tpch', 'part')}}

{% endsnapshot %}