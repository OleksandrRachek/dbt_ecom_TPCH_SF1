{{ config(materialized='table') }}

WITH lineitems AS (
  SELECT * FROM {{ ref('stg_lineitem') }}
),
suppliers AS (
  SELECT * FROM {{ ref('int_supplier_geography') }}
)

SELECT
  s.supplier_id,
  s.supplier_name,
  s.supplier_region,
  AVG(DATEDIFF('day', l.ship_date, l.receipt_date)) AS avg_delivery_days,
  COUNT(DISTINCT l.order_id) AS fulfilled_orders
FROM lineitems l
JOIN suppliers s ON l.supp_id = s.supplier_id
GROUP BY 1,2,3