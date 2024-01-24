{{ config(
  materialized='table',
  unique_key='location_sk',
  description="This table represents unique locations with a surrogate key."
) }}

WITH 

    SOURCE AS (
        SELECT 
        
            *
        
        FROM {{ ref('ABANOUB_SUPERSTORE') }}
    ),

    UNIQUE_LOCATION AS (
        SELECT DISTINCT

            COUNTRY,
            STATE, 
            CITY, 
            REGION

        FROM SOURCE
    ),

    DIM_LOCATION AS (
        SELECT 

            ROW_NUMBER() OVER (ORDER BY (SELECT NULL))  AS LOCATION_SK,

            *

        FROM UNIQUE_LOCATION
    ) 

SELECT * FROM DIM_LOCATION
