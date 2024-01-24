WITH 

    SOURCE AS (
        SELECT 
        
            *
        
        FROM {{ ref('ABANOUB_SUPERSTORE') }}
    ),

    DIM_PRODUCT AS (
        SELECT DISTINCT

           PRODUCT_ID AS PRODUCT_KEY,
           PRODUCT_NAME,
           CATEGORY,
           SUB_CATEGORY

        FROM SOURCE
    ) 

SELECT * FROM DIM_PRODUCT
