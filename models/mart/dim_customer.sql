{{ config(materialized='table') }}

SELECT
    
    customer_id,
    customer_name,
    customer_segment ,
    account_balance ,
    customer_region,
    base_ts
FROM {{ ref('int_customer_geography') }}