{{ config(materialized='table') }}
SELECT      
    id,
    specific_location,
    general_location,
    area_sqm,
    date_posted,
    type,
    bedrooms,
    bathrooms,
    price,
    CAST(price / NULLIF(area_sqm, 0) AS FLOAT64) AS price_per_sqm,
    furnishing_status,
    agent_name,
    completion_status,
    ownership,
    purpose
FROM
    {{ ref('stg_total_bayut') }}
    

