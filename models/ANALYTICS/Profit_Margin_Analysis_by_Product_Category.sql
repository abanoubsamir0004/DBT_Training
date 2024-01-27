-- Profit_Margin_Analysis_by_Product_Category

WITH 

    DIM_PRODUCT AS (
        SELECT * FROM {{ ref('DIM_PRODUCT') }}
    ),


    FACT_SALES AS (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),


    PROFIT_MARGINS AS (
        SELECT

            DIM_PRODUCT.CATEGORY,
            ROUND(AVG(FACT_SALES.PROFIT / FACT_SALES.SALES),3) AS AVG_PROFIT_MARGIN

        FROM FACT_SALES
        LEFT JOIN DIM_PRODUCT 
            ON FACT_SALES.PRODUCT_KEY = DIM_PRODUCT.PRODUCT_KEY

        GROUP BY DIM_PRODUCT.CATEGORY
    )

SELECT *
FROM PROFIT_MARGINS
