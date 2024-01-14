{# This code establishes a dimension table named DIM_PROPERTY_AT_PRESENT by capturing distinct property-related attributes from the STG_ABANOUB_NYC_SALES_CLEANEND source. 
The PROPERTY_AT_PRESENT_ID is assigned using dense ranking based on building class at present, tax class at present, and tax subclass at present. 
The final query retrieves all columns from the dimension table.#}

WITH 

    NYC_SALES_CLEANEND AS (
        SELECT * FROM {{ ref('STG_ABANOUB_NYC_SALES_CLEANEND') }}
    ), 

    DIM_PROPERTY_AT_PRESENT AS(
        SELECT DISTINCT

            DENSE_RANK () OVER 
                (ORDER BY BUILDING_CLASS_AT_PRESENT,TAX_CLASS_AT_PRESENT,TAX_SUBCLASS_AT_PRESENT)
                AS PROPERTY_AT_PRESENT_ID,

            BUILDING_CLASS_AT_PRESENT,
            
            TAX_CLASS_AT_PRESENT,
            
            TAX_SUBCLASS_AT_PRESENT
            
        FROM NYC_SALES_CLEANEND
    )

SELECT * 
FROM DIM_PROPERTY_AT_PRESENT
