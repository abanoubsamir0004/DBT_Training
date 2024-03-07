WITH 

    STG_Business as (
        SELECT *
        FROM {{ ref('STG_Business') }}
    ),

    STG_Location as (
        SELECT *
        FROM {{ ref('STG_Location') }}
    ),

    STG_CHECKIN as (
        SELECT *
        FROM {{ ref('STG_Checkin') }}
    ),

   
    DIM_BUSINESS as (
        SELECT
            B.business_id,
            B.name,
            B.is_open,
            B.stars,
            L.city,
            L.state,
            L.postal_code,
            STG_CHECKIN.date AS checkin_dates

        FROM STG_Business B 

        LEFT JOIN STG_Location L 
            on B.location_key =  L.location_key
            
        LEFT JOIN STG_CHECKIN  
            on B.business_id = STG_CHECKIN.business_id
    )

SELECT *
FROM DIM_BUSINESS