WITH
    STG_LOCATION as (
        SELECT *
        FROM {{ ref('STG_Location') }}
    ),

    Final as (
        
        SELECT DISTINCT
         
            B.JSON_DATA:business_id::VARCHAR as business_id,
            L.location_key,
            B.JSON_DATA:name::VARCHAR as name,
            B.JSON_DATA:is_open::VARCHAR as is_open,
            B.JSON_DATA:stars::DOUBLE as stars

        FROM {{ source('WEATHER_AND_RESTAURANT', 'BUSINESS') }} B

        JOIN STG_LOCATION L
            ON  B.JSON_DATA:address = L.address
            AND B.JSON_DATA:city = L.city
            AND B.JSON_DATA:state = L.state
            AND B.JSON_DATA:postal_code = L.postal_code
            AND B.JSON_DATA:latitude = L.latitude
            AND B.JSON_DATA:longitude = L.longitude
    )

SELECT *
FROM Final