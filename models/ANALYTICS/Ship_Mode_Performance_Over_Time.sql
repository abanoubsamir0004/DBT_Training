-- Ship_Mode_Performance_Over_Time

WITH 
    FACT_SALES AS (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),

    DIM_DATE AS (
        SELECT * FROM {{ ref('DIM_DATE') }}
    ),

    DIM_ORDER AS (
        SELECT * FROM {{ ref('DIM_ORDER') }}
    ),

    SHIP_MODE_PERFORMANCE_OVER_TIME as (
        SELECT

            D.YEAR,
            D.MONTH,
            DIM_ORDER.SHIP_MODE,
            ROUND(SUM(SALES),3) AS TOTAL_SALES

        FROM FACT_SALES F
        LEFT JOIN DIM_DATE D 
            ON F.SHIP_DATE_KEY = D.DATE_KEY

        LEFT JOIN DIM_ORDER
            ON F.ORDER_KEY =DIM_ORDER.ORDER_KEY
            
        GROUP BY 
           D.YEAR,D.MONTH, DIM_ORDER.SHIP_MODE

        ORDER BY YEAR, MONTH, DIM_ORDER.SHIP_MODE
    )

SELECT *
FROM SHIP_MODE_PERFORMANCE_OVER_TIME


