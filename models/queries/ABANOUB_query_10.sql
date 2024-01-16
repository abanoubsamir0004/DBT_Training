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

            COUNT(*) MULTIPLE_SOLDED_COUNT,
            L.BOROUGH_NAME,
            L.NEIGHBORHOOD, 
            F.TAX_BLOCK, 
            F.TAX_LOT

        FROM FACT_SALES F
        INNER JOIN DIM_LOCATION L 
            ON F.LOCATION_ID = L.LOCATION_ID

        WHERE F.SALE_PRICE != 0 AND F.SALE_PRICE IS NOT NULL
        GROUP BY 
            L.BOROUGH_NAME, L.NEIGHBORHOOD, F.TAX_BLOCK, F.TAX_LOT
        HAVING 
            MULTIPLE_SOLDED_COUNT > 1
    ),

    FINAL AS (
        SELECT 
            DISTINCT
            UBSMT.*,
            D.SALE_DATE,
            F.SALE_PRICE AS CURRENT_SALE_PRICE,
            LAG(F.SALE_PRICE) OVER (
                PARTITION BY F.TAX_BLOCK, F.TAX_LOT ORDER BY D.SALE_DATE) AS PREVIOUS_SALE_PRICE

        FROM 
            UNIQUE_BUILDINGS_SOLDED_MULITPLE_TIME UBSMT
        JOIN 
            FACT_SALES F 
                ON UBSMT.TAX_BLOCK = F.TAX_BLOCK 
                AND UBSMT.TAX_LOT = F.TAX_LOT
                
        JOIN 
            DIM_LOCATION L 
                ON F.LOCATION_ID = L.LOCATION_ID
                AND UBSMT.BOROUGH_NAME = L.BOROUGH_NAME
                AND UBSMT.NEIGHBORHOOD = L.NEIGHBORHOOD

        JOIN DIM_SALES_DATE D 
            ON F.SALES_DATE_ID = D.SALES_DATE_ID

        WHERE 
            F.SALE_PRICE != 0 AND F.SALE_PRICE IS NOT NULL
        ORDER BY 
            UBSMT.BOROUGH_NAME, UBSMT.NEIGHBORHOOD, UBSMT.TAX_BLOCK, UBSMT.TAX_LOT, D.SALE_DATE
    )

SELECT * 
FROM FINAL


