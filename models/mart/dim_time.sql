{{ config(materialized='table') }}

SELECT
  date_day,
  YEAR(date_day) AS year,
  MONTH(date_day) AS month,
  QUARTER(date_day) AS quarter,
  CASE WHEN MONTH(date_day) IN (10,11,12) THEN 'Q4-FY' ELSE CONCAT('Q', QUARTER(date_day)) END AS fiscal_quarter
FROM {{ ref('int_date_spine') }}