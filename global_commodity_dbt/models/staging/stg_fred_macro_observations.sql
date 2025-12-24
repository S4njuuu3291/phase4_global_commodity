{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('commodity', 'commodity_data') }}
),

-- Flatten masing-masing seri
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
),

-- Gabungkan semua seri
unioned AS (
    SELECT * FROM cpi
    UNION ALL
    SELECT * FROM dollar
    UNION ALL
    SELECT * FROM bond
),

-- Dedup per series_id + date
dedup AS (
    SELECT
        series_id,
        date,
        value,
        ROW_NUMBER() OVER (
            PARTITION BY series_id, date
            ORDER BY date
        ) AS rn
    FROM unioned
)

SELECT
    series_id,
    date,
    value
FROM dedup
WHERE rn = 1
