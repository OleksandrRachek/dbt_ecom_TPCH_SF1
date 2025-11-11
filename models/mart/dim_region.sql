{{ config(materialized='table') }}

SELECT DISTINCT

customer_nation,
customer_region,
    base_ts
FROM {{ ref('int_customer_geography') }}