SELECT 
    trim(P_COMMENT) as part_comment,
    trim(P_BRAND) as brand,
    trim(P_MFGR) as manufacturer,
    trim(P_NAME) as part_name,
    trim(P_SIZE) as part_size,
    trim(P_TYPE) as part_type,
    trim(P_PARTKEY)::number as part_id,
    trim(P_RETAILPRICE)::float as retail_price,
    trim(P_CONTAINER) as part_container,
    CURRENT_TIMESTAMP() as load_ts,
    base_ts
FROM {{ref('scd_part')}}
WHERE DBT_VALID_TO is null