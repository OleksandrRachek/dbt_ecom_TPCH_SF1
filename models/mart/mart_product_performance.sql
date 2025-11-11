{{ config(materialized='view') }}

WITH sales AS (
    
    SELECT * FROM {{ ref('fct_sales') }}
)

SELECT
    part_id,
    part_name,
    part_type,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(quantity) AS total_quantity_sold,
    SUM(net_revenue) AS total_revenue,
    AVG(gross_margin) AS avg_margin
FROM sales
GROUP BY 1,2,3