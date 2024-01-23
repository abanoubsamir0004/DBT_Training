{# Count the number of buildings by different dimensions.#}
{#To obtain each unique building must retrieve distinct rows by using the combination of block and lot, along with the location dimension, to capture all unique buildings.#}
WITH 

   FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_LOCATION  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_LOCATION') }}
    ),

    UNIQUE_BUILDINGS AS (
        SELECT DISTINCT 
        
        L.BOROUGH_NAME,
        L.NEIGHBORHOOD, 
        L.TAX_BLOCK, 
        L.TAX_LOT

        FROM FACT_SALES F
        LEFT JOIN DIM_LOCATION L
        ON F.LOCATION_ID =L.LOCATION_ID
    ),


    COUNT_DIFF_BUILDING_PER_BOROHGH_NAME AS (

        SELECT 

        'BOROUGH_NAME' AS DIMENSION,
        BOROUGH_NAME,
        NULL AS NEIGHBORHOOD,
        NULL AS TAX_BLOCK,
        COUNT(*) AS BUILDING_COUNT

        FROM UNIQUE_BUILDINGS

        GROUP BY BOROUGH_NAME

    ),

    COUNT_DIFF_BUILDING_PER_BOROHGH_NEIGHBORHOOD AS (

        SELECT 

        'NEIGHBORHOOD' AS DIMENSION,
        BOROUGH_NAME,
        NEIGHBORHOOD,
        NULL AS TAX_BLOCK,
        COUNT(*) AS BUILDING_COUNT

        FROM UNIQUE_BUILDINGS

        GROUP BY BOROUGH_NAME, NEIGHBORHOOD

    ),

    
    COUNT_DIFF_BUILDING_PER_BOROHGH_TAX_BLOCK AS (

        SELECT 

        'TAX_BLOCK' AS DIMENSION,
        BOROUGH_NAME,
        NEIGHBORHOOD,
        TAX_BLOCK,
        COUNT(*) AS BUILDING_COUNT

        FROM UNIQUE_BUILDINGS

        GROUP BY BOROUGH_NAME, NEIGHBORHOOD,TAX_BLOCK

    ),

    FINAL AS (
        SELECT * FROM COUNT_DIFF_BUILDING_PER_BOROHGH_NAME
        UNION 
        
        SELECT * FROM COUNT_DIFF_BUILDING_PER_BOROHGH_NEIGHBORHOOD
        UNION 

        SELECT * FROM COUNT_DIFF_BUILDING_PER_BOROHGH_TAX_BLOCK

    )

SELECT * 
FROM FINAL
ORDER BY DIMENSION,BOROUGH_NAME, NEIGHBORHOOD,TAX_BLOCK