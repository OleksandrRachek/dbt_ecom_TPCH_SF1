{{ config(materialized='ephemeral') }}

select * from {{ source('tpch', 'part') }}