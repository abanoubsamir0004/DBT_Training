{# Identify the building class category with the highest average land square feet.#}

WITH 

   FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_PROPERTY_AT_PRESENT  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_PROPERTY_AT_PRESENT') }}
    ),

    AVG_LAND_SQUARE_FEET_BY_BUILDING_CLASS AS (
        SELECT
            P.BUILDING_CLASS_CATEGORY,
            AVG(F.LAND_SQUARE_FEET) AS AVERAGE_LAND_SQUARE_FEET
        FROM
            FACT_SALES F
        INNER JOIN
            DIM_PROPERTY_AT_PRESENT P ON F.PROPERTY_AT_PRESENT_ID = P.PROPERTY_AT_PRESENT_ID
            
        WHERE F.LAND_SQUARE_FEET IS NOT NULL
        GROUP BY
            P.BUILDING_CLASS_CATEGORY
    )

SELECT *
FROM
    AVG_LAND_SQUARE_FEET_BY_BUILDING_CLASS 
ORDER BY
    AVERAGE_LAND_SQUARE_FEET DESC
LIMIT 1
