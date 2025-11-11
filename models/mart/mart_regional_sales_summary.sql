{{ config(materialized='view') }}

WITH sales AS (
    
    SELECT * FROM {{ ref('fct_sales') }}
)

SELECT
    customer_region,
    supplier_region,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(net_revenue) AS total_revenue,
    AVG(gross_margin) AS avg_margin
FROM sales
GROUP BY 1,2