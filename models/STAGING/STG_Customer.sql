SELECT 
    JSON_DATA:user_id::VARCHAR AS user_id,
    JSON_DATA:average_stars::DOUBLE AS average_stars,
    JSON_DATA:fans::NUMBER AS fans,
    JSON_DATA:review_count::NUMBER AS review_count,
    JSON_DATA:name::VARCHAR AS name
FROM {{ source('WEATHER_AND_RESTAURANT', 'CUSTOMER') }}