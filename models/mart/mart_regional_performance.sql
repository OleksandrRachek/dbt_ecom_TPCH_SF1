{{ config(materialized='view') }}

SELECT
    customer_region,
    supplier_region,
    SUM(net_revenue) AS total_revenue,
    SUM(gross_margin) AS total_margin,
    COUNT(DISTINCT order_id) AS orders_count,
    ROUND(SUM(gross_margin) / NULLIF(SUM(net_revenue), 0), 3) AS avg_margin_pct
FROM {{ ref('fct_sales') }}
GROUP BY 1, 2