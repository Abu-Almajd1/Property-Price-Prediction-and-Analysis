{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('airflow_transfers', 'total_bayut') }}
)
SELECT 
    REGEXP_EXTRACT(url, r'(\d+)\.html$') AS id,
    title,
    SPLIT(location_address, ',')[SAFE_OFFSET(0)] AS specific_location,  
    SPLIT(location_address, ',')[SAFE_OFFSET(1)] AS general_location,
    CAST(REGEXP_EXTRACT(property_area, r'(\d+)') AS INT64) AS area_sqm,
    date_posted,
    type,
    CAST(REGEXP_EXTRACT(bedrooms, r'(\d+)') AS INT64) AS bedrooms,
    CAST(REGEXP_EXTRACT(bathrooms, r'(\d+)') AS INT64) AS bathrooms,
    description,
    price_amount AS price,
    furnishing_status,
    agent_name,
    completion_status,
    ownership,
    purpose,
    reference,
    url,
    amenities
FROM
    {{ source('airflow_transfers', 'total_bayut') }}