{{ config(materialized='incremental', unique_key='order_date') }}

SELECT
    order_date,
    SUM(net_revenue) AS total_revenue,
    SUM(gross_margin) AS total_margin,
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(gross_margin) / NULLIF(SUM(net_revenue), 0), 3) AS margin_pct
FROM {{ ref('fct_sales') }}


{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}


GROUP BY order_date