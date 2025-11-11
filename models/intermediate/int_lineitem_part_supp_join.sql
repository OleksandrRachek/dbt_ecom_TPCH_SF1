{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}


WITH lineitems AS (
  SELECT * FROM {{ ref('stg_lineitem') }}
),
partsupp AS (
  SELECT * FROM {{ ref('stg_partsupp') }}
),

part AS (
  SELECT * from {{ref('int_product_pricing')}}
)


SELECT
  l.order_id,
  l.line_number,
  l.part_id,
  l.supp_id supplier_id,
  l.quantity,
  l.extended_price,
  l.discount,
  l.tax,
  l.ship_date,
  l.receipt_date,
  l.comment,
  l.shipping_struct,
  l.return_flag,
  l.commit_date,
  l.ship_mode,
  (l.extended_price * (1 - l.discount) * (1 + l.tax)) AS net_revenue,
  ps.supply_cost,
  ps.available_qty,
  p.part_name,
  p.part_type,
  p.retail_price,
  p.brand,
  p.supplier_name,
  p.supplier_nation,
  p.supplier_region,
  p.margin,

  l.base_ts
FROM lineitems l
LEFT JOIN partsupp ps 
  ON l.part_id = ps.part_id AND l.supp_id = ps.supplier_id
  join part  p 
  on ps.part_id = p.part_id and ps.supplier_id=p.supplier_id
   
 

 



{% if is_incremental() %}
-- Incremental filter: only load line items with new order keys or line numbers
WHERE (order_id, line_number) NOT IN (
    SELECT order_id, line_number FROM {{ this }}
)
{% endif %}