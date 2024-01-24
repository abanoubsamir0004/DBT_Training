WITH 

    SOURCE AS (
        SELECT 
        
            *
        
        FROM {{ ref('ABANOUB_SUPERSTORE') }}
    ),

    DIM_CUSTOMER AS (
        SELECT DISTINCT

           CUSTOMER_ID AS CUSTOMER_KEY,
           CUSTOMER_NAME,
           SEGMENT,
           POSTAL_CODE

        FROM SOURCE
    ) 

{{ config(
  materialized='table',
  unique_key='CUSTOMER_KEY',
  description="This table represents a Customer dimension."
) }}

SELECT * FROM DIM_CUSTOMER
