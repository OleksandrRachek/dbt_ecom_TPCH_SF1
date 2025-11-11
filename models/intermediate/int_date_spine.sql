{{ config(materialized='table') }}

WITH dates AS (
  SELECT DATEADD(day, seq4(), '1990-01-01') AS date_day
  FROM TABLE(GENERATOR(ROWCOUNT => 15000))
)

SELECT
  date_day,
  YEAR(date_day) AS year,
  MONTH(date_day) AS month,
  DAY(date_day) AS day,
  TO_CHAR(date_day, 'YYYY-MM') AS year_month,
  DAYOFWEEK(date_day) AS weekday
FROM dates