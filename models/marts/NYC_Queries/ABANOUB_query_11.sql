{# determine the building age category based on the year built, and then use it to analyze the relationship between building age and sale price.#}

WITH 
   FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_PROPERTY_AT_SALE  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_PROPERTY_AT_SALE') }}
    ),

    DIM_SALES_DATE  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_SALES_DATE') }}
    ),

    BUILDING_AGE_CATEGORY AS (
        SELECT

            FACT_SALES.*,
            DIM_SALES_DATE.SALE_YEAR,
            DIM_PROPERTY_AT_SALE.YEAR_BUILT,

            CASE
                WHEN DIM_PROPERTY_AT_SALE.YEAR_BUILT IS NOT NULL THEN DIM_SALES_DATE.SALE_YEAR - DIM_PROPERTY_AT_SALE.YEAR_BUILT
                ELSE NULL
            END AS BUILDING_AGE

        FROM  FACT_SALES
        LEFT JOIN DIM_PROPERTY_AT_SALE
            ON FACT_SALES.PROPERTY_AT_SALE_ID = DIM_PROPERTY_AT_SALE.PROPERTY_AT_SALE_ID

        LEFT JOIN DIM_SALES_DATE 
            ON FACT_SALES.SALES_DATE_ID = DIM_SALES_DATE.SALES_DATE_ID
        
        WHERE DIM_PROPERTY_AT_SALE.YEAR_BUILT NOT IN (2019,1111) AND DIM_PROPERTY_AT_SALE.YEAR_BUILT IS NOT NULL
    ),

    FINAL AS (
        SELECT

            YEAR_BUILT,
            SALE_YEAR,
            BUILDING_AGE,
            COUNT(*) AS NUM_SALES,
            MIN(SALE_PRICE) AS MIN_SALE_PRICE,
            MAX(SALE_PRICE) AS MAX_SALE_PRICE,
            AVG(SALE_PRICE) AS AVG_SALE_PRICE
            
        FROM
            BUILDING_AGE_CATEGORY
        GROUP BY SALE_YEAR, YEAR_BUILT, BUILDING_AGE
        ORDER BY BUILDING_AGE
    )

SELECT * 
FROM FINAL