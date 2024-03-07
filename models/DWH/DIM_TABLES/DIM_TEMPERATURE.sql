WITH 

    STG_Temperature as (
        SELECT *
        FROM {{ ref('STG_Temperature') }}
    ),

    DIM_TEMPERATURE as (
        SELECT
            date,
            min_Temperature,
            max_Temperature,
            normal_min_Temperature,
            normal_max_Temperature

        FROM STG_Temperature 
    )

SELECT *
FROM DIM_TEMPERATURE