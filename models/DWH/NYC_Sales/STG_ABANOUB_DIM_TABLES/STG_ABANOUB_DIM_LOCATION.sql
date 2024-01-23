{# This code creates a dimension table named DIM_LOCATION by extracting unique location-related information from the STG_ABANOUB_NYC_SALES_CLEANEND source. 
The LOCATION_ID column combines borough, neighborhood, and ZIP code details for a distinct representation. 
The final query retrieves all columns from the dimension table and orders the results by the location identifier in ascending order.#}

{{ config(materialized='table') }}

WITH 

    NYC_SALES_CLEANEND AS (
        SELECT * FROM {{ ref('STG_ABANOUB_NYC_SALES_CLEANEND') }}
    ), 


    DIM_LOCATION AS (
        SELECT 
            DISTINCT 
            DENSE_RANK () OVER (
                ORDER BY BOROUGH, BOROUGH_NAME, NEIGHBORHOOD, ZIP_CODE,TAX_BLOCK,TAX_LOT) AS LOCATION_ID,
            BOROUGH,
            BOROUGH_NAME,
            NEIGHBORHOOD,
            ZIP_CODE,
            TAX_BLOCK,
            TAX_LOT
        FROM NYC_SALES_CLEANEND
    )

SELECT *
FROM DIM_LOCATION
