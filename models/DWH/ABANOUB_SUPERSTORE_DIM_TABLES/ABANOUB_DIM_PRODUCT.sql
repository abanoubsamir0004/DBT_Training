WITH 

    SOURCE AS (
        SELECT 
        
            *
        
        FROM {{ ref('ABANOUB_SUPERSTORE') }}
    ),

    DIM_PRODUCT AS (
        SELECT DISTINCT

           PRODUCT_ID AS PRODUCT_KEY,
           PRODUCT_NAME,
           CATEGORY,
           SUB_CATEGORY

        FROM SOURCE
    ) 

{{ config(
  materialized='table',
  unique_key='PRODUCT_KEY',
  description="This table represents unique product data."
) }}


SELECT * FROM DIM_PRODUCT
