with

    SOURCE AS (

        SELECT * 
        
        FROM {{ ref('SUPERSTORE') }}
    ),

    DIM_DATE AS (

        SELECT * 
        
        FROM {{ ref('DIM_DATE') }}

    ),

    DIM_LOCATION AS (

        SELECT * 
        
        FROM {{ ref('DIM_LOCATION') }}

    ),

    DIM_ORDER AS (

        SELECT * 
        
        FROM {{ ref('DIM_ORDER') }}

    ),

    DIM_PRODUCT AS (

        SELECT * 
        
        FROM {{ ref('DIM_PRODUCT') }}

    ),

    DIM_CUSTOMER AS (

        SELECT * 
        
        FROM {{ ref('DIM_CUSTOMER') }}

    ),



    FACT_SALES AS (

        SELECT 

            D1.DATE_KEY AS ORDER_DATE_KEY,

            D2.DATE_KEY AS SHIP_DATE_KEY,

            DIM_LOCATION.LOCATION_SK,

            DIM_PRODUCT.PRODUCT_KEY,

            DIM_CUSTOMER.CUSTOMER_ID AS CUSTOMER_KEY,

            DIM_ORDER.ORDER_KEY,

            SOURCE.SALES, 

            SOURCE.QUANTITY,

            SOURCE.DISCOUNT,

            SOURCE.PROFIT 
        
        FROM SOURCE  


        LEFT JOIN DIM_DATE D1
            ON SOURCE.ORDER_DATE = D1.FULL_DATE 

        LEFT JOIN DIM_DATE D2
            ON SOURCE.SHIP_DATE = D2.FULL_DATE 

        LEFT JOIN  DIM_LOCATION 
            ON SOURCE.COUNTRY = DIM_LOCATION.COUNTRY 
            AND SOURCE.STATE = DIM_LOCATION.STATE 
            AND SOURCE.CITY = DIM_LOCATION.CITY 
            AND SOURCE.REGION = DIM_LOCATION.REGION 
            AND SOURCE.POSTAL_CODE =DIM_LOCATION.POSTAL_CODE

        LEFT JOIN  DIM_CUSTOMER
            ON SOURCE.CUSTOMER_ID = DIM_CUSTOMER.CUSTOMER_ID 

        LEFT JOIN  DIM_PRODUCT 
            ON SOURCE.PRODUCT_ID = DIM_PRODUCT.PRODUCT_ID 
            AND SOURCE.PRODUCT_NAME = DIM_PRODUCT.PRODUCT_NAME 
            AND SOURCE.CATEGORY = DIM_PRODUCT.CATEGORY 
            AND SOURCE.SUB_CATEGORY = DIM_PRODUCT.SUB_CATEGORY 

        LEFT JOIN  DIM_ORDER
            ON SOURCE.ORDER_ID = DIM_ORDER.ORDER_KEY 
    )

{{ config(
  materialized='table',
  unique_key='ORDER_DATE_KEY, SHIP_DATE_KEY, LOCATION_SK, PRODUCT_KEY, CUSTOMER_KEY, ORDER_KEY',
  description="This table represents a fact table for sales."
) }}

SELECT *
FROM FACT_SALES