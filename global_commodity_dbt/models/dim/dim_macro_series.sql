{{ config(materialized='table', on_schema_change='sync_all_columns') }}

SELECT DISTINCT
    series_id,
    CASE series_id
        WHEN 'CPIAUCSL' THEN 'Consumer Price Index'
        WHEN 'DTWEXBGS' THEN 'US Dollar Index'
        WHEN 'DGS10' THEN '10-Year Treasury Yield'
        ELSE 'Unknown Series'
    END AS series_name,
    'daily' AS frequency,
    'raw_value' AS units,
    'FRED' AS source
FROM {{ ref('stg_fred_macro_observations') }}
