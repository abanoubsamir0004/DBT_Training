SELECT DISTINCT
    date,
    precipitation,
    precipitation_normal
FROM {{ source('WEATHER_AND_RESTAURANT', 'LV_PRECIPITATION') }}    
