{# Create a new column with the difference years between sale date year built. Group the data by this new column and analyze the distribution of sale price.#}

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

    SALES_WITH_AGE_DIFF AS (

        SELECT

            D.SALE_YEAR,
            P.YEAR_BUILT,
            D.SALE_YEAR - P.YEAR_BUILT AS AGE_DIFF,
            F.SALE_PRICE
        FROM
            FACT_SALES F
       
        LEFT JOIN DIM_PROPERTY_AT_SALE P
            ON F.PROPERTY_AT_SALE_ID = P.PROPERTY_AT_SALE_ID
       
        LEFT JOIN DIM_SALES_DATE D
            ON F.SALES_DATE_ID = D.SALES_DATE_ID
       
        WHERE 
            P.YEAR_BUILT IS NOT NULL AND P.YEAR_BUILT NOT IN (2019, 1111)
            AND F.SALE_PRICE != 0 AND F.SALE_PRICE IS NOT NULL 
    ),

    FINAL AS (
        SELECT

            AGE_DIFF,
            COUNT(*) AS NUMBER_OF_SALES,
            AVG(SALE_PRICE) AS AVERAGE_SALE_PRICE,
            MIN(SALE_PRICE) AS MIN_SALE_PRICE,
            MAX(SALE_PRICE) AS MAX_SALE_PRICE,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY SALE_PRICE) AS Q1_SALE_PRICE,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY SALE_PRICE) AS MEDIAN_SALE_PRICE,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY SALE_PRICE) AS Q3_SALE_PRICE

        FROM
            SALES_WITH_AGE_DIFF
        GROUP BY
            AGE_DIFF
        ORDER BY
            AGE_DIFF
    )

SELECT * 
FROM FINAL
