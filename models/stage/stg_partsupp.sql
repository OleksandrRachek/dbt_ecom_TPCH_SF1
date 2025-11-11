SELECT 
    trim(PS_PARTKEY)::number as part_id,
    trim(PS_SUPPKEY)::number as supplier_id,
    trim(PS_COMMENT) as partsupp_comment,
    trim(PS_SUPPLYCOST)::float as supply_cost,
    trim(PS_AVAILQTY)::float as available_qty,
    CURRENT_TIMESTAMP() as load_ts,
    base_ts
FROM {{ source('tpch', 'partsupp') }}

