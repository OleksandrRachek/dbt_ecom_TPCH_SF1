{{ config(materialized='table') }}

WITH part AS (
    SELECT * FROM {{ ref('stg_part') }}
),
partsupp AS (
    SELECT * FROM {{ ref('stg_partsupp') }}
),
supplier AS (
    SELECT * FROM {{ ref('int_supplier_geography') }}
)

SELECT
    p.part_id,
    ps.supplier_id,
    p.part_name,
    p.part_type,
    p.brand,
    p.retail_price,
    ps.available_qty,
    ps.supply_cost,
    s.supplier_name,
    s.supplier_nation, 
    s.supplier_region,
    (p.retail_price - ps.supply_cost) AS margin,
    p.base_ts
FROM part p
JOIN partsupp ps ON p.part_id = ps.part_id
JOIN supplier s ON ps.supplier_id = s.supplier_id