{{ config (materialized='view')}}

WITH customer_sales AS (
    SELECT
        *
    FROM {{ ref('fct_sales') }}
),

seed as 
(select * from {{ref('region_rollup')}})


SELECT
     s.region_group,
        sum(c.net_revenue) total_net_revenue
FROM customer_sales c join seed s on c.customer_region = s.region_name
group by s.region_group