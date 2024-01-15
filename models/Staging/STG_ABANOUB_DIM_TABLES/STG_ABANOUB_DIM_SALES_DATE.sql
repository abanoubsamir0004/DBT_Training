{# This code generates a dimension table named DIM_SALE_DATE by extracting and formatting date-related details from the STG_ABANOUB_NYC_SALES_CLEANEND source. 
The SALES_DATE_ID column serves as a unique identifier representing the sale date in YYYYMMDD format. 
The query retrieves all columns from the dimension table and orders the results by the sale year in ascending order.#}

{{ config(materialized='table') }}

WITH 

DIM_SALED_DATE AS (
    SELECT DISTINCT
        TO_NUMBER(
            TO_CHAR(SALE_DATE, 'YYYYMMDD')
        ) AS SALES_DATE_ID,
        SALE_DATE,
        EXTRACT (YEAR FROM SALE_DATE) AS SALE_YEAR ,
        LPAD(EXTRACT(MONTH FROM SALE_DATE), 2, '0') AS SALE_MONTH,
        LPAD(EXTRACT(DAY FROM SALE_DATE), 2, '0') AS SALE_DAY

    FROM {{ ref('STG_ABANOUB_NYC_SALES_CLEANEND') }} AS SOURCE 
)

SELECT *
FROM DIM_SALED_DATE
