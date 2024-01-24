WITH 

    DATE_SOURCE AS (
        {{ dbt_date.get_date_dimension(start_date="2010-01-01", end_date="2023-01-01") }}
    ),

    DIM_DATE AS (
        SELECT 
        
            TO_NUMBER(TO_CHAR(DATE_DAY, 'YYYYMMDD')) AS DATE_KEY,
            EXTRACT (YEAR FROM DATE_DAY) AS YEAR ,

            LPAD(EXTRACT(MONTH FROM DATE_DAY), 2, '0') AS MONTH,

            LPAD(EXTRACT(DAY FROM DATE_DAY), 2, '0') AS DAY,

            QUARTER_OF_YEAR AS QUARTER,

            CASE
                WHEN QUARTER = 1 THEN 'First Quarter'
                WHEN QUARTER = 2 THEN 'Second Quarter'
                WHEN QUARTER = 3 THEN 'Third Quarter'
                WHEN QUARTER = 4 THEN 'Fourth Quarter'
                ELSE NULL
            END AS QUARTER_NAME
        FROM DATE_SOURCE
    ) 
    
{{ config(
  materialized='table',
  unique_key='date_key',
  description="This table represents a date dimension."
) }}

SELECT * FROM DIM_DATE
