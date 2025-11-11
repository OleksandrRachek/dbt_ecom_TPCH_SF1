{{ config(materialized='view') }}

WITH sales AS (
   
    SELECT * FROM {{ ref('fct_sales') }}
)

SELECT
    supplier_name,
    supplier_region,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(net_revenue) AS total_revenue,
    AVG(gross_margin) AS avg_margin,
    SUM(quantity) AS total_quantity_supplied
FROM sales
GROUP BY 1,2