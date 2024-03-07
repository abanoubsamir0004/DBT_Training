WITH 

    STG_Precipitation as (
        SELECT *
        FROM {{ ref('STG_Precipitation') }}
    ),

    DIM_PRECIPITATION as (
        SELECT
            date,
            precipitation,
            precipitation_normal

        FROM STG_Precipitation 
    )

SELECT *
FROM DIM_PRECIPITATION