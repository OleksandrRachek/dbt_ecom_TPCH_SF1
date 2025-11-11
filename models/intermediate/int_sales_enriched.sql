{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_order_lineitems') }}
),
customers AS (
    SELECT * FROM {{ ref('int_customer_geography') }}
)

SELECT


    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.order_total_price,
    o.order_priority,
    o.clerk,
    o.line_number,
    o.part_id,
    o.supplier_id,
    o.quantity,
    o.discount,
    o.tax,
    o.ship_date,
    o.receipt_date,
    o.extended_price,
    o.comment,
    o.shipping_struct,
    o.return_flag,
    o.commit_date,
    o.ship_mode,
    o.net_revenue,
    o.supply_cost,
    o.available_qty,
    o.part_name,
    o.part_type,
    o.retail_price,
    o.brand,
    o.supplier_name,
    o.supplier_nation,
    o.supplier_region,
    o.margin as gross_margin,
    c.customer_name,
    c.customer_segment,
    c.customer_nation,
    c.customer_region,
    SUM(o.net_revenue) OVER (PARTITION BY c.customer_name) as total_revenue_per_customer,
    COUNT(DISTINCT o.order_id) OVER (PARTITION BY c.customer_name) as total_orders_per_customer,
    SUM(o.net_revenue) OVER (PARTITION BY o.order_id) as avg_revenue_per_order,
    SUM(o.margin) OVER (PARTITION BY o.brand) total_gross_margin,
    SUM(o.net_revenue) OVER (PARTITION BY o.brand) total_net_revenue,
    o.base_ts
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id





{% if is_incremental() %}
-- Incremental filter: only load line items with new order keys or line numbers
WHERE (o.order_id, line_number) NOT IN (
    SELECT order_id, line_number FROM {{ this }}
)
{% endif %}