{{ config(
    materialized='incremental',
    unique_key='supplier_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('tpch', 'supplier') }}
),

renamed AS (

SELECT 
    trim(S_PHONE) as supplier_phone,
    trim(S_NATIONKEY)::number as nation_id,
    trim(S_COMMENT) as supplier_comment,
    trim(S_SUPPKEY)::number as supplier_id,
    trim(S_ACCTBAL)::float as account_balance,
    trim(S_ADDRESS) as supplier_address,
    trim(S_NAME) as supplier_name,
    CURRENT_TIMESTAMP() as load_ts,
    base_ts
FROM source


)

SELECT * FROM renamed

{% if is_incremental() %}
-- Only new suppliers
WHERE supplier_id NOT IN (SELECT supplier_id FROM {{ this }})
{% endif %}