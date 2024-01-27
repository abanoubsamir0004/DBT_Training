-- Top_10_Selling_Products

WITH 

    DIM_PRODUCT AS
    (
        SELECT * FROM {{ ref('DIM_PRODUCT') }}
    ),

    FACT_SALES AS
    (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),

    TOP_10_SELLING_PRODUCTS AS (
        SELECT 

            DIM_PRODUCT.PRODUCT_ID,
            DIM_PRODUCT.PRODUCT_NAME,
            SUM(FACT_SALES.QUANTITY) AS TOTAL_QUANTITY,
            ROUND(SUM(FACT_SALES.SALES),3) AS TOTAL_PRODUCT_SALES,
            RANK() OVER (ORDER BY TOTAL_PRODUCT_SALES DESC) AS PRODUCT_RANK

        FROM FACT_SALES

        LEFT JOIN DIM_PRODUCT
            ON FACT_SALES.PRODUCT_KEY = DIM_PRODUCT.PRODUCT_KEY

        GROUP BY DIM_PRODUCT.PRODUCT_ID, DIM_PRODUCT.PRODUCT_NAME

    )

SELECT *
FROM TOP_10_SELLING_PRODUCTS
WHERE PRODUCT_RANK <= 10
