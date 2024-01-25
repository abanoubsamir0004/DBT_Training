WITH 

    SOURCE AS (
        SELECT 
        
            *
        
        FROM {{ ref('ABANOUB_SUPERSTORE') }}
    ),

    DIM_ORDER AS (
        SELECT DISTINCT

           ORDER_ID AS ORDER_KEY,
           SHIP_MODE

        FROM SOURCE
    ) 

{{ config(
  materialized='table',
  unique_key='ORDER_KEY',
  description="This table represents unique order data."
) }}

SELECT * FROM DIM_ORDER
