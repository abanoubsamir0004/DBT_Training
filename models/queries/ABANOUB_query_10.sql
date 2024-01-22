{#10. Implement a logic to find buildings sold multiple times and compare their sale price across each transaction.#}

WITH 

    FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_LOCATION  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_LOCATION') }}
    ),

    DIM_SALES_DATE  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_SALES_DATE') }}
    ),

    UNIQUE_BUILDINGS_SOLDED_MULITPLE_TIME AS (

        SELECT  
            DISTINCT
            COUNT(*) MULTIPLE_SOLDED_COUNT,
            L.BOROUGH_NAME,
            L.NEIGHBORHOOD, 
            L.TAX_BLOCK, 
            L.TAX_LOT

        FROM FACT_SALES F
        INNER JOIN DIM_LOCATION L 
            ON F.LOCATION_ID = L.LOCATION_ID
        GROUP BY 
            L.BOROUGH_NAME, L.NEIGHBORHOOD, L.TAX_BLOCK, L.TAX_LOT
        HAVING 
            MULTIPLE_SOLDED_COUNT > 1
    ),

    FINAL AS (
        SELECT 
            UBSMT.*,
            D.SALE_DATE,
            F.SALE_PRICE AS CURRENT_SALE_PRICE,
            LAG(F.SALE_PRICE) OVER (
                PARTITION BY UBSMT.BOROUGH_NAME, UBSMT.NEIGHBORHOOD , UBSMT.TAX_BLOCK, UBSMT.TAX_LOT 
                    ORDER BY D.SALE_DATE) AS PREVIOUS_SALE_PRICE

        FROM 
            UNIQUE_BUILDINGS_SOLDED_MULITPLE_TIME UBSMT

        JOIN DIM_LOCATION L 
            ON UBSMT.BOROUGH_NAME = L.BOROUGH_NAME
            AND UBSMT.NEIGHBORHOOD = L.NEIGHBORHOOD
            AND UBSMT.TAX_BLOCK = L.TAX_BLOCK
            AND UBSMT.TAX_LOT = L.TAX_LOT
            
        JOIN 
            FACT_SALES F 
                ON F.LOCATION_ID = L.LOCATION_ID         

        JOIN DIM_SALES_DATE D 
            ON F.SALES_DATE_ID = D.SALES_DATE_ID

        WHERE
            F.SALE_PRICE != 0 OR SALE_PRICE IS NOT NULL
            
        ORDER BY 
            UBSMT.BOROUGH_NAME, UBSMT.NEIGHBORHOOD, UBSMT.TAX_BLOCK, UBSMT.TAX_LOT, D.SALE_DATE
    )

SELECT * 
FROM FINAL
