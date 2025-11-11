{{ config(materialized='table') }}

SELECT
    part_id,
    part_name,
    part_type,
    retail_price,
    margin,
    available_qty,
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ ref('int_product_pricing') }}