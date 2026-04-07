{{ config(materialized='view') }}

WITH source AS (
    SELECT *
    FROM {{ source('commodity', 'commodity_data') }}
),

macro_parsed AS (
    SELECT
        *,
        SAFE.PARSE_JSON(macro) AS macro_json
    FROM source
),

-- Flatten masing-masing seri dari JSON, filter invalid values
cpi AS (
    SELECT
        'CPIAUCSL' AS series_id,
        JSON_VALUE(obs, '$.date') AS date,
        SAFE_CAST(JSON_VALUE(obs, '$.value') AS FLOAT64) AS value
    FROM macro_parsed,
    UNNEST(IFNULL(JSON_QUERY_ARRAY(macro_json, '$.CPIAUCSL.observations'), [])) AS obs
    WHERE SAFE_CAST(JSON_VALUE(obs, '$.value') AS FLOAT64) IS NOT NULL
),

dollar AS (
    SELECT
        'DTWEXBGS' AS series_id,
        JSON_VALUE(obs, '$.date') AS date,
        SAFE_CAST(JSON_VALUE(obs, '$.value') AS FLOAT64) AS value
    FROM macro_parsed,
    UNNEST(IFNULL(JSON_QUERY_ARRAY(macro_json, '$.DTWEXBGS.observations'), [])) AS obs
    WHERE SAFE_CAST(JSON_VALUE(obs, '$.value') AS FLOAT64) IS NOT NULL
),

bond AS (
    SELECT
        'DGS10' AS series_id,
        JSON_VALUE(obs, '$.date') AS date,
        SAFE_CAST(JSON_VALUE(obs, '$.value') AS FLOAT64) AS value
    FROM macro_parsed,
    UNNEST(IFNULL(JSON_QUERY_ARRAY(macro_json, '$.DGS10.observations'), [])) AS obs
    WHERE SAFE_CAST(JSON_VALUE(obs, '$.value') AS FLOAT64) IS NOT NULL
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
