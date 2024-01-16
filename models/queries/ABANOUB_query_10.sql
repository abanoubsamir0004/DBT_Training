{# Implement a logic to find buildings sold multiple times and compare their sale price across each transaction.#}

WITH 

   FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_LOCATION  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_LOCATION') }}
    ),

    UNIQUE_BUILDINGS_SOLDED_MULITPLE_TIME AS (
        SELECT  
        COUNT(*) MULTIPLE_SOLDED_COUNT,
        L.BOROUGH_NAME,
        L.NEIGHBORHOOD, 
        F.TAX_BLOCK, 
        F.TAX_LOT

        FROM FACT_SALES F
        INNER JOIN DIM_LOCATION L
        ON F.LOCATION_ID = L.LOCATION_ID

        WHERE F.SALE_PRICE != 0 AND F.SALE_PRICE IS NOT NULL
        GROUP BY  L.BOROUGH_NAME,L.NEIGHBORHOOD,F.TAX_BLOCK, F.TAX_LOT
        HAVING MULTIPLE_SOLDED_COUNT > 1
    )

SELECT COUNT(*) 
FROM UNIQUE_BUILDINGS_SOLDED_MULITPLE_TIME
