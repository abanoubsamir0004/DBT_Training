WITH 

    SOURCE AS (
        SELECT 
        
            *
        
        FROM {{ ref('SUPERSTORE') }}
    ),

    DIM_CUSTOMER AS (
        SELECT DISTINCT

           CUSTOMER_ID,
           CUSTOMER_NAME,
           SEGMENT
        FROM SOURCE
    ) 

{{ config(
  materialized='table',
  unique_key='CUSTOMER_ID',
  description="This table represents a Customer dimension."
) }}

SELECT * FROM DIM_CUSTOMER
