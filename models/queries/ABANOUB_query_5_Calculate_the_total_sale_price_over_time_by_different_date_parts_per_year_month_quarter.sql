WITH 

    FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_SALES_DATE  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_SALES_DATE') }}
    ),

    TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_YEAR AS (
        SELECT

            'YEAR' AS DIMENSION,
            D.SALE_YEAR,
            NULL AS SALE_MONTH,
            NULL AS SALE_QUARTER,
            SUM(F.SALE_PRICE) AS TOTAL_SALE_PRICE
        FROM
            FACT_SALES F
        INNER JOIN
            DIM_SALES_DATE D ON F.SALES_DATE_ID = D.SALES_DATE_ID
        GROUP BY
            D.SALE_YEAR
    ),

    TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_MONTH AS (
        SELECT

            'MONTH' AS DIMENSION,
            D.SALE_YEAR,
            D.SALE_MONTH,
            NULL AS SALE_QUARTER,
            SUM(F.SALE_PRICE) AS TOTAL_SALE_PRICE
        FROM
            FACT_SALES F
        INNER JOIN
            DIM_SALES_DATE D ON F.SALES_DATE_ID = D.SALES_DATE_ID
        GROUP BY
            D.SALE_YEAR, D.SALE_MONTH
    ),

    TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_QUARTER AS (
        SELECT

            'QUARTER' AS DIMENSION,
            D.SALE_YEAR,
            NULL AS SALE_MONTH,
            EXTRACT(QUARTER FROM D.SALE_DATE) AS SALE_QUARTER,
            SUM(F.SALE_PRICE) AS TOTAL_SALE_PRICE

        FROM
            FACT_SALES F
        INNER JOIN
            DIM_SALES_DATE D ON F.SALES_DATE_ID = D.SALES_DATE_ID
        GROUP BY
            D.SALE_YEAR, SALE_QUARTER
    ),

    FINAL AS (
        SELECT * 
        FROM TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_YEAR
        
        UNION ALL
        
        SELECT 
            DIMENSION,
            SALE_YEAR,
            SALE_MONTH,
            CASE
                WHEN SALE_QUARTER = 1 THEN 'First Quarter'
                WHEN SALE_QUARTER = 2 THEN 'Second Quarter'
                WHEN SALE_QUARTER = 3 THEN 'Third Quarter'
                WHEN SALE_QUARTER = 4 THEN 'Fourth Quarter'
                ELSE NULL
            END AS SALE_QUARTER,
            TOTAL_SALE_PRICE

        FROM TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_QUARTER
       
        UNION ALL

        SELECT * 
        FROM TOTAL_SALE_PRICE_BY_DATE_PARTS_PER_MONTH
    )

SELECT * 
FROM FINAL
ORDER BY DIMENSION, SALE_YEAR, SALE_MONTH, SALE_QUARTER
