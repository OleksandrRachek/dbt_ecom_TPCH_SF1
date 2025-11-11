{{ config (materialized='view')}}

WITH customer_sales AS (
    SELECT
        customer_id,
        SUM(net_revenue) AS lifetime_revenue,
        SUM(gross_margin) AS lifetime_margin,
        COUNT(DISTINCT order_id) AS total_orders,
        MAX(order_date) AS last_order_date
    FROM {{ ref('fct_sales') }}
    GROUP BY 1
)

SELECT
    customer_id,
    lifetime_revenue,
    lifetime_margin,
    total_orders,
    lifetime_margin / NULLIF(total_orders, 0) AS avg_margin_per_order,
    DATEDIFF('day', last_order_date, CURRENT_DATE()) AS days_since_last_order,
    CASE
        WHEN lifetime_revenue > 1000000 THEN 'VIP'
        WHEN lifetime_revenue BETWEEN 500000 AND 1000000 THEN 'LOYAL'
        WHEN lifetime_revenue BETWEEN 100000 AND 500000 THEN 'REGULAR'
        ELSE 'OCCASIONAL'
    END AS customer_value_segment,
    CURRENT_TIMESTAMP() AS last_updated
FROM customer_sales
