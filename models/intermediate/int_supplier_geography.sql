{{ config(
    materialized='incremental',
    unique_key='supplier_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}


WITH supplier AS (
    SELECT * FROM {{ ref('stg_supplier') }}
),
nation AS (
    SELECT * FROM {{ ref('stg_nation') }}
),
region AS (
    SELECT * FROM {{ ref('stg_region') }}
)

SELECT
    s.supplier_id,
    s.supplier_name,
    s.account_balance,
    n.nation_name supplier_nation,
    r.region_name supplier_region
FROM supplier s
JOIN nation n ON s.nation_id = n.nation_id
JOIN region r ON n.region_id = r.region_id

{% if is_incremental() %}
-- Only new suppliers
WHERE supplier_id NOT IN (SELECT supplier_id FROM {{ this }})
{% endif %}