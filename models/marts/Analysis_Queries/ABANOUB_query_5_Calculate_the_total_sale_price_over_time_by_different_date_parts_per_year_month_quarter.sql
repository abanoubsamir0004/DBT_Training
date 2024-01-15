WITH 

    FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_SALES_DATE  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_SALES_DATE') }}
    ),

    TOTAL_SALE_PRICE_BY_DATE_PARTS AS (
        SELECT
            D.SALE_YEAR,
            D.SALE_MONTH,
            EXTRACT(QUARTER FROM D.SALE_DATE) AS SALE_QUARTER,
            SUM(F.SALE_PRICE) AS TOTAL_SALE_PRICE
        FROM
            FACT_SALES F
        INNER JOIN
            DIM_SALES_DATE D ON F.SALES_DATE_ID = D.SALES_DATE_ID
        GROUP BY
            D.SALE_YEAR,
            D.SALE_MONTH,
            SALE_QUARTER
    ),

    FINAL AS (
        SELECT
            'MONTH' AS DIMENSION,
            SALE_YEAR,
            SALE_MONTH,
            NULL AS SALE_QUARTER,
            TOTAL_SALE_PRICE
        FROM
            TOTAL_SALE_PRICE_BY_DATE_PARTS

        UNION ALL

        SELECT
            'QUARTER' AS DIMENSION,
            SALE_YEAR,
            NULL AS SALE_MONTH,  
            CASE
                WHEN SALE_QUARTER = 1 THEN 'First Quarter'
                WHEN SALE_QUARTER = 2 THEN 'Second Quarter'
                WHEN SALE_QUARTER = 3 THEN 'Third Quarter'
                WHEN SALE_QUARTER = 4 THEN 'Fourth Quarter'
                ELSE NULL
            END AS SALE_QUARTER,
            TOTAL_SALE_PRICE
        FROM
            TOTAL_SALE_PRICE_BY_DATE_PARTS

        UNION ALL

        SELECT
            'YEAR' AS DIMENSION,
            SALE_YEAR,
            NULL AS SALE_MONTH,
            NULL AS SALE_QUARTER,
            TOTAL_SALE_PRICE
        FROM
            TOTAL_SALE_PRICE_BY_DATE_PARTS
        ORDER BY DIMENSION ,SALE_YEAR, SALE_MONTH,SALE_QUARTER
    )

SELECT * 
FROM FINAL