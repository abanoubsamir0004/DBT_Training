SELECT DISTINCT
    date,
    min as min_Temperature,
    max as max_Temperature,
    normal_min as normal_min_Temperature,
    normal_max as normal_max_Temperature
FROM {{ source('WEATHER_AND_RESTAURANT', 'lv_temperature') }}    
