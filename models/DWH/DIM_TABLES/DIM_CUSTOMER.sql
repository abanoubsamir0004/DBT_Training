WITH 

    STG_Customer as (
        SELECT *
        FROM {{ ref('STG_Customer') }}
    ),

    DIM_CUSTOMER as (
        SELECT
            user_id,
            name,
            average_stars,
            review_count,
            fans

        FROM STG_Customer 
    )

SELECT *
FROM DIM_CUSTOMER

