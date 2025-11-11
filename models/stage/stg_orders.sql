{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('tpch', 'orders') }}
),

renamed AS (
SELECT
    trim(o_orderkey)::number AS order_id,
    trim(o_custkey)::number AS customer_id,
    TRIM(o_orderstatus) AS order_status,
    trim(o_totalprice)::FLOAT AS order_total_price,
    trim(o_orderdate)::DATE AS order_date,
    trim(o_orderpriority) as order_priority,
    trim(o_clerk) as clerk,
    trim(o_shippriority) AS priority,
    trim(o_comment) as comment,
    CURRENT_TIMESTAMP() AS load_ts,
    base_ts
FROM source)


SELECT * FROM renamed

{% if is_incremental() %}
-- Only load new or updated orders
WHERE order_id NOT IN (SELECT order_id FROM {{ this }})
{% endif %}