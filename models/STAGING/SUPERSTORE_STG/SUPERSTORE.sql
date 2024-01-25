WITH

    SOURCE AS (
        SELECT 
            ROW_ID ,
            ORDER_ID ,
            ORDER_DATE ,
            SHIP_DATE ,
            SHIP_MODE ,
            CUSTOMER_ID ,
            CUSTOMER_NAME ,
            SEGMENT ,
            COUNTRY ,
            CITY ,
            STATE,
            CAST(POSTAL_CODE AS INT) AS POSTAL_CODE,
            REGION ,
            PRODUCT_ID ,
            CATEGORY ,
            SUB_CATEGORY ,
            TRIM(PRODUCT_NAME) AS PRODUCT_NAME,
            SALES ,
            QUANTITY ,
            DISCOUNT ,
            PROFIT
        FROM 
            {{ source('SAMPLE_SUPERSTORE', 'SUPERSTORE') }}
    )

SELECT * 
FROM SOURCE