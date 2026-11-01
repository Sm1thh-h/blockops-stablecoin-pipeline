
{{
  config(
    materialized='view',
    tags=['staging', 'arkham']
  )
}}

-- models/staging/stg_arkham_entities.sql

with source as (
    select * from {{ source('raw_arkham', 'entities_raw') }}
)

select
    entity_slug,
    JSON_EXTRACT_SCALAR(raw_json, '$.name')    as entity_name,
    JSON_EXTRACT_SCALAR(raw_json, '$.id')      as arkham_id,
    JSON_EXTRACT_SCALAR(raw_json, '$.type')    as entity_type,
    JSON_EXTRACT_SCALAR(raw_json, '$.website') as website,
    pulled_at
from source