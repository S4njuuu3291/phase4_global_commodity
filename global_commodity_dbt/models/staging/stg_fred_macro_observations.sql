{{config(materialized='view')}}

WITH source AS (
    SELECT *
    FROM {{ source('commodity', 'commodity_data') }}
),

cpi AS (
    SELECT
        'CPIAUCSL' AS series_id,
        obs.date,
        obs.value
    FROM source, UNNEST(macro.CPIAUCSL.observations) obs
),

dollar AS (
    SELECT
        'DTWEXBGS' AS series_id,
        obs.date,
        obs.value
    FROM source, UNNEST(macro.DTWEXBGS.observations) obs
),

bond AS (
    SELECT
        'DGS10' AS series_id,
        obs.date,
        obs.value
    FROM source, UNNEST(macro.DGS10.observations) obs
)

SELECT * FROM cpi
UNION ALL
SELECT * FROM dollar
UNION ALL
SELECT * FROM bond
