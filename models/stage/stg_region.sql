SELECT 
    trim(R_REGIONKEY)::NUMBER as region_id,
    trim(R_COMMENT) as region_comment,
    trim(R_NAME) as region_name,
    CURRENT_TIMESTAMP as load_ts,
    base_ts

FROM {{ source('tpch', 'region') }}
