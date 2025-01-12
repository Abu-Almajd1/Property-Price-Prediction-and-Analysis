{{ config(materialized='table') }}

WITH raw_data AS (
    SELECT * FROM {{ source('airflow_transfers', 'total_propertyfinder') }}
)

SELECT 
    REGEXP_EXTRACT(url, r'(\d+)\.html$') AS id,
    title,
    SPLIT(location, ',')[SAFE_OFFSET(0)] AS specific_location,  
    SPLIT(location, ',')[SAFE_OFFSET(1)] AS general_location,
    CAST(REGEXP_EXTRACT(area, r'(\d+)\s*sqm') AS INT64) AS area_sqm,
    available_from,
    property_type AS type,
    bathrooms,
    bedrooms,
    description,
    price,
    amenities
FROM
    {{ source('airflow_transfers', 'total_propertyfinder') }}

    