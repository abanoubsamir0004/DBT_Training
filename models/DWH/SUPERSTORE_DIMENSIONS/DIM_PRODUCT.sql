{# 
While exploring the product data, I noticed that there are 32 product IDs, each with 2 different product names.
So, I decided to take the product key to obtain distinct product IDs, product names, categories, and subcategories. 
This ensures that I don't lose any products and can create a unique product key for them, with the product ID serving as the natural key 
#}

WITH 

    SOURCE AS (
        SELECT 
            *
        FROM {{ ref('SUPERSTORE') }}
    ),
    
    UNIQUE_PRODUCTS AS (
        SELECT DISTINCT 
            PRODUCT_ID ,
            PRODUCT_NAME,
            CATEGORY,
            SUB_CATEGORY
        FROM SOURCE
    ),

    DIM_PRODUCT AS (
        SELECT 

            ROW_NUMBER() OVER (ORDER BY (SELECT NULL))  AS PRODUCT_KEY,
            
            * 
            
        FROM UNIQUE_PRODUCTS
    )

{{ config(
materialized='table',
unique_key='PRODUCT_KEY',
description="This table represents unique product data with ranking based on the number of rows."
) }}

SELECT *
FROM  DIM_PRODUCT
