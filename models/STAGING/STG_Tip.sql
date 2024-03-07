SELECT DISTINCT
    JSON_DATA:business_id::VARCHAR AS business_id,
    JSON_DATA:user_id::VARCHAR AS user_id,
    JSON_DATA:date::DATE AS date,
    JSON_DATA:compliment_count::NUMBER AS compliment_count,
    JSON_DATA:text::STRING AS text
FROM {{ source('WEATHER_AND_RESTAURANT', 'TIP') }}    
