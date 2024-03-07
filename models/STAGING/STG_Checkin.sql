SELECT  DISTINCT
    JSON_DATA:business_id:: VARCHAR AS business_id,
    JSON_DATA:date::ARRAY AS date

FROM {{ source('WEATHER_AND_RESTAURANT', 'CHECKIN') }} 


