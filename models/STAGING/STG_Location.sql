WITH 
    DISTINCT_LOCATION AS (
        SELECT DISTINCT 
        {{ dbt_utils.generate_surrogate_key([
            'JSON_DATA:address::VARCHAR',
            'JSON_DATA:city::VARCHAR',
            'JSON_DATA:state::VARCHAR',
            'JSON_DATA:postal_code::VARCHAR',
            'JSON_DATA:latitude::VARCHAR',
            'JSON_DATA:longitude::VARCHAR']) }} as location_key,
            
        JSON_DATA:address::VARCHAR as address,
        JSON_DATA:city::VARCHAR as city,
        JSON_DATA:state::VARCHAR as state,
        JSON_DATA:postal_code::VARCHAR as postal_code,
        JSON_DATA:latitude::DOUBLE as latitude,
        JSON_DATA:longitude::DOUBLE as longitude
        
    FROM {{ source('WEATHER_AND_RESTAURANT', 'BUSINESS') }} 
    )

SELECT *  
FROM DISTINCT_LOCATION