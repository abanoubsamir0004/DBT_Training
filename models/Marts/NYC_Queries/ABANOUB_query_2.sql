{# Find the neighborhood with the most total units.#}

WITH 

    FACT_SALES AS (
        SELECT * FROM {{ ref('ABANOUB_FACT_SALES') }}
    ),

    DIM_LOCATION  AS (
        SELECT * FROM {{ ref('STG_ABANOUB_DIM_LOCATION') }}
    ),

    TOTAL_UNITS_BY_NEIGHBORHOOD AS (
        SELECT
            L.NEIGHBORHOOD,
            SUM(F.TOTAL_UNITS) AS TOTAL_UNITS
        FROM
            FACT_SALES F
        LEFT JOIN
            DIM_LOCATION L ON F.LOCATION_ID = L.LOCATION_ID
        WHERE F.TOTAL_UNITS IS NOT NULL
        GROUP BY
            L.NEIGHBORHOOD
    )

SELECT *
FROM
    TOTAL_UNITS_BY_NEIGHBORHOOD 
ORDER BY
    TOTAL_UNITS DESC
LIMIT 1