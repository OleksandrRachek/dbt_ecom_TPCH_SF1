{{ config(materialized='table') }}

SELECT
  part_id,
  part_name,
  SPLIT_PART(part_type, '-', 1) AS category,
  SPLIT_PART(part_type, '-', 2) AS sub_category,
  retail_price
FROM {{ ref('dim_product') }}