{# This code creates a dimension table called DIM_PROPERTY_AT_SALE by extracting unique property-related details from the STG_ABANOUB_NYC_SALES_CLEANEND source. 
The PROPERTY_AT_SALE_ID is generated using dense ranking based on building class category, building class at the time of sale, tax class at the time of sale, and year built.
The final query retrieves all columns from the dimension table. #}

WITH 

    NYC_SALES_CLEANEND AS (
        SELECT * FROM {{ ref('STG_ABANOUB_NYC_SALES_CLEANEND') }}
    ), 

    DIM_PROPERTY_AT_SALE AS(
        SELECT DISTINCT

            DENSE_RANK () OVER 
                (ORDER BY BUILDING_CLASS_CATEGORY,BUILDING_CLASS_AT_TIME_OF_SALE,TAX_CLASS_AT_TIME_OF_SALE,YEAR_BUILT)
                AS PROPERTY_AT_SALE_ID,

            BUILDING_CLASS_CATEGORY,
            
            BUILDING_CLASS_AT_TIME_OF_SALE,
            
            TAX_CLASS_AT_TIME_OF_SALE,

            YEAR_BUILT
            
        FROM NYC_SALES_CLEANEND
    )

SELECT * 
FROM DIM_PROPERTY_AT_SALE
