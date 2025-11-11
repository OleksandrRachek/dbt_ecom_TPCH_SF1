{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_number'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ source('tpch', 'lineitem') }}
),

renamed AS (


SELECT
    trim(L_PARTKEY)::number as part_id,
    trim(L_EXTENDEDPRICE)::float as extended_price,
    trim(L_RECEIPTDATE)::date as receipt_date,
    trim(L_TAX)::float as tax,
    trim(L_LINENUMBER)::number as line_number,
    trim(L_COMMENT) as comment,
    trim(L_SHIPINSTRUCT) as shipping_struct,
    trim(L_RETURNFLAG) as return_flag,
    trim(L_LINESTATUS) as line_status,
    trim(L_COMMITDATE)::date as commit_date,
    trim(L_SHIPDATE)::date as ship_date,
    trim(L_SUPPKEY)::number as supp_id,
    trim(L_ORDERKEY)::number as order_id,
    trim(L_QUANTITY)::float as quantity,
    trim(L_DISCOUNT)::float as discount,
    trim(L_SHIPMODE) as ship_mode,
    CURRENT_TIMESTAMP() as load_ts,
    base_ts
FROM source)

SELECT * FROM renamed

{% if is_incremental() %}
-- Incremental filter: only load line items with new order keys or line numbers
WHERE (order_id, line_number) NOT IN (
    SELECT order_id, line_number FROM {{ this }}
)
{% endif %}