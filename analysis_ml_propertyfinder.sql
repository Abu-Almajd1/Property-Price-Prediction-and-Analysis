{{ config(materialized='table') }}

SELECT
    id,
    specific_location,
    general_location,
    

FROM
    {{ ref('stg_total_propertyfinder') }}