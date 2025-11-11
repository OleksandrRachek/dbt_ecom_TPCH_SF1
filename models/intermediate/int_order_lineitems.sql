{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
lineitems AS (
    SELECT * FROM {{ ref('int_lineitem_part_supp_join') }}
)


SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.order_total_price,
    o.order_priority,
    o.clerk,
    o.comment,
    l.line_number,
    l.part_id,
    l.supplier_id,
    l.quantity,
    l.discount,
    l.tax,
    l.ship_date,
    l.receipt_date,
    l.extended_price,
    l.comment comment_lineitem,
    l.shipping_struct,
    l.return_flag,
    l.commit_date,
    l.ship_mode,
    l.net_revenue,
    l.supply_cost,
    l.available_qty,
    l.part_name,
    l.part_type,
    l.retail_price,
    l.brand,
    l.supplier_name,
    l.supplier_nation,
    l.supplier_region,
    l.margin,
    l.base_ts


FROM orders o
JOIN lineitems l ON o.order_id = l.order_id




{% if is_incremental() %}
-- Incremental filter: only load line items with new order keys or line numbers
WHERE (o.order_id, line_number) NOT IN (
    SELECT order_id, line_number FROM {{ this }}
)
{% endif %}