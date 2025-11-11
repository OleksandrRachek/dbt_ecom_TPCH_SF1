
{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}



SELECT
    
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.order_total_price,
    o.order_priority,
    o.clerk,
    o.comment,
    o.line_number,
    o.part_id,
    o.supplier_id,
    o.quantity,
    
    o.discount,
    o.tax,
    o.ship_date,
    o.receipt_date,
    o.extended_price,
  
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
    o.gross_margin,
    o.customer_name,
    o.customer_segment,
    o.customer_nation,
    o.customer_region,
    datediff(day, o.order_date, o.receipt_date) days_to_delivery,
    datediff(day, o.ship_date, o.receipt_date)  ship_days,
    datediff(day, o.order_date, base_ts::date) order_age,
    total_revenue_per_customer,
    total_orders_per_customer,
    avg_revenue_per_order,
    total_gross_margin/NULLIF(total_net_revenue,0) as margin_percentage,
    CASE
        WHEN o.total_revenue_per_customer > 500000 THEN 'HIGH VALUE'
        WHEN o.total_revenue_per_customer BETWEEN 100000 AND 500000 THEN 'MID VALUE'
        ELSE 'LOW VALUE'
    END AS value_segment,
    CASE 
        WHEN o.avg_revenue_per_order>1000 THEN '>1000'
        WHEN o.avg_revenue_per_order Between 101 and 1000 THEN '100...1000'
        ELSE '<100' END as order_revenue_category,


    o.base_ts



FROM {{ ref('int_sales_enriched') }} o



{% if is_incremental() %}
-- Incremental filter: only load line items with new order keys or line numbers
WHERE (o.order_id, o.line_number) NOT IN (
    SELECT order_id, line_number FROM {{ this }}
)
{% endif %}