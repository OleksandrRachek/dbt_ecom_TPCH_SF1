{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('tpch', 'customer') }}
),

renamed as 
(SELECT
trim(C_CUSTKEY)::number as customer_id,
trim(C_NAME) as customer_name,
trim(C_ACCTBAL)::float as account_balance,
trim(C_MKTSEGMENT) as customer_segment,
trim(C_PHONE) as customer_phone,
trim(C_ADDRESS) as customer_address,
trim(C_NATIONKEY)::number as nation_id,
trim(C_COMMENT) as customer_comment,
current_timestamp as load_ts,
base_ts
FROM source)

select * from renamed

{% if is_incremental() %}
-- Only load new customers
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}
