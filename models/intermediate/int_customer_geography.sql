{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}


WITH customer AS (
    SELECT * FROM {{ ref('stg_customer') }}
),
nation AS (
    SELECT * FROM {{ ref('stg_nation') }}
),
region AS (
    SELECT * FROM {{ ref('stg_region') }}
)

SELECT
    c.customer_id,
    c.customer_name,
    c.customer_segment ,
    c.account_balance ,
    n.nation_name customer_nation,
    r.region_name customer_region,
    c.base_ts
FROM customer c
JOIN nation n ON c.nation_id = n.nation_id
JOIN region r ON n.region_id = r.region_id



{% if is_incremental() %}
-- Only load new customers
WHERE customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}
