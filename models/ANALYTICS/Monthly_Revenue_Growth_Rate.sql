-- Monthly Revenue Growth Rate

WITH 

    FACT_SALES AS (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),

    DIM_DATE AS (
        SELECT * FROM {{ ref('DIM_DATE') }}
    ),

    MONTHLY_REVENUE_GROWTH_RATE AS (
        
        SELECT
            
            D.YEAR, 
            D.MONTH,
            ROUND(SUM(SALES),3) AS TOTAL_SALES,
            LAG(TOTAL_SALES) OVER (ORDER BY D.YEAR, D.MONTH) AS PREVIOUS_MONTH_SALES,
            (TOTAL_SALES - LAG(TOTAL_SALES) OVER (
                ORDER BY D.YEAR, D.MONTH)) / LAG(TOTAL_SALES) OVER (ORDER BY D.YEAR, D.MONTH) * 100 AS GROWTH_RATE

        FROM FACT_SALES F
        LEFT JOIN DIM_DATE D 
            ON F.ORDER_DATE_KEY = D.DATE_KEY

        GROUP BY 
            D.YEAR, D.MONTH
        ORDER BY D.YEAR, D.MONTH
    )

SELECT * 
FROM MONTHLY_REVENUE_GROWTH_RATE
