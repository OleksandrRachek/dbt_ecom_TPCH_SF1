{{ config(materialized='table') }}

WITH customer_orders AS (
  SELECT * FROM {{ ref('fct_sales') }}
)

SELECT  
  customer_id,
  total_orders_per_customer,
  total_revenue_per_customer,
  value_segment,
  min(order_age) as min_order_age
FROM customer_orders
GROUP BY customer_id,
  total_orders_per_customer,
  total_revenue_per_customer,
  value_segment
