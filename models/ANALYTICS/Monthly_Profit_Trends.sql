-- Monthly_Profit_Trends

WITH 
    DIM_DATE AS (
        SELECT * FROM {{ ref('DIM_DATE') }}
    ),

    FACT_SALES AS (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),

    MONTHLY_PROFITS AS (
        SELECT

            DIM_DATE.YEAR,
            DIM_DATE.MONTH,
            SUM(FACT_SALES.PROFIT) AS TOTAL_PROFIT

        FROM FACT_SALES
        LEFT JOIN DIM_DATE 
            ON FACT_SALES.ORDER_DATE_KEY = DIM_DATE.DATE_KEY
        GROUP BY  DIM_DATE.YEAR,  DIM_DATE.MONTH
    )

SELECT *
FROM MONTHLY_PROFITS
ORDER BY YEAR,MONTH
