{{ config(materialized='table') }}

WITH lineitems AS (
  SELECT * FROM {{ ref('stg_lineitem') }}
)

SELECT
  order_id,
  AVG(discount) AS avg_discount_rate,
  SUM(extended_price * discount) AS total_discount_amount
FROM lineitems
GROUP BY 1