SELECT 
    trim(N_COMMENT) as nation_comment,
    trim(N_REGIONKEY)::number as region_id,
    trim(N_NAME) as nation_name,
    trim(N_NATIONKEY)::NUMBER as nation_id,
    CURRENT_TIMESTAMP() as load_ts,
    base_ts
FROM {{ ref('scd_nation') }}
WHERE DBT_VALID_TO is null