SELECT DISTINCT
    JSON_DATA:business_id::VARCHAR AS business_id,
    JSON_DATA:"Call To Action enabled"::VARCHAR AS call_action,
    JSON_DATA:"Covid Banner"::VARCHAR AS covid_banner,
    JSON_DATA:"Grubhub enabled"::VARCHAR AS grubhub,
    JSON_DATA:"Request a Quote Enabled"::VARCHAR AS request_a_quote,
    JSON_DATA:"Temporary Closed Until"::VARCHAR AS temporary_closed,
    JSON_DATA:"Virtual Services Offered"::VARCHAR AS virtual_services,
    JSON_DATA:"delivery or takeout"::VARCHAR AS delivery_or_takeout,
    JSON_DATA:highlights::VARCHAR AS highlights

FROM {{ source('WEATHER_AND_RESTAURANT', 'YELP_COVID') }}