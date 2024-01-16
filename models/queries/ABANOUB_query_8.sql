{# Use_a_window_function_to_calculate_the_running_total_of_Sales_price.#}

WITH 

   FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_SALES_DATE  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_SALES_DATE') }}
    ),

    RUNNING_SALES_PRICE_TOTAL AS (
        SELECT DISTINCT

        D.SALE_YEAR,
        D.SALE_MONTH,
        SUM (F.SALE_PRICE) OVER (
            ORDER BY D.SALE_YEAR, D.SALE_MONTH) AS RUNNING_TOTAL_SALES_PRICE

        FROM FACT_SALES F
        INNER JOIN DIM_SALES_DATE D
        ON F.SALES_DATE_ID = D.SALES_DATE_ID

    )

SELECT * 
FROM RUNNING_SALES_PRICE_TOTAL
ORDER BY SALE_YEAR, SALE_MONTH