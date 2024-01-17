{# Identify the top 5 most expensive buildings based on sale price.#}

WITH 

   FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_LOCATION  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_LOCATION') }}
    ),

    UNIQUE_BUILDINGS AS (

        SELECT DISTINCT 
        
        L.BOROUGH_NAME,
        L.NEIGHBORHOOD, 
        L.TAX_BLOCK, 
        L.TAX_LOT,
        F.SALE_PRICE

        FROM FACT_SALES F
        INNER JOIN DIM_LOCATION L
        ON F.LOCATION_ID = L.LOCATION_ID
    ),

    TOP_5_EXPENSIVE_BUILDINGS AS (
        SELECT 

        BOROUGH_NAME,
        NEIGHBORHOOD,
        TAX_BLOCK,
        TAX_LOT,
        MAX(SALE_PRICE) AS SALE_PRICE

        FROM UNIQUE_BUILDINGS 
        WHERE SALE_PRICE != 0 AND SALE_PRICE IS NOT NULL
        GROUP BY BOROUGH_NAME, NEIGHBORHOOD, TAX_BLOCK, TAX_LOT
        ORDER BY SALE_PRICE DESC 
        LIMIT 5

    )

SELECT * 
FROM TOP_5_EXPENSIVE_BUILDINGS