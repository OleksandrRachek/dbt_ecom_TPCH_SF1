{{ config(materialized='table') }}

SELECT
    supplier_id,
    supplier_name,
    supplier_region,
    fulfilled_orders,
    avg_delivery_days,
    CURRENT_TIMESTAMP() AS last_updated
FROM {{ ref('int_supplier_shipping_summary') }}