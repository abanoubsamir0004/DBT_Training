-- Total product category and subcategory sales

WITH 

    DIM_PRODUCT AS
    (
        SELECT * FROM {{ ref('DIM_PRODUCT') }}
    ),

    FACT_SALES AS
    (
        SELECT * FROM {{ ref('FACT_SALES') }}
    ),

    CATEGORY_SUBCATEGORY_SALES AS(
        SELECT 

            DIM_PRODUCT.CATEGORY,
            DIM_PRODUCT.SUB_CATEGORY,
            ROUND(SUM(FACT_SALES.SALES),3) AS TOTAL_SALES

        FROM FACT_SALES

        LEFT JOIN DIM_PRODUCT
        ON FACT_SALES.PRODUCT_KEY = DIM_PRODUCT.PRODUCT_KEY

        GROUP BY DIM_PRODUCT.CATEGORY, DIM_PRODUCT.SUB_CATEGORY
        ORDER BY TOTAL_SALES DESC
    )

SELECT * 
FROM CATEGORY_SUBCATEGORY_SALES

