{{ config(
    materialized='incremental',
    unique_key='series_id || "-" || date_str',
    partition_by={'field': 'date_col', 'data_type': 'date'},
    cluster_by=['series_id']
) }}

SELECT
    PARSE_DATE('%Y-%m-%d', date) AS date_col,
    date AS date_str,
    series_id,
    CAST(value AS FLOAT64) AS value
FROM {{ ref('stg_fred_macro_observations') }}
{% if is_incremental() %}
WHERE PARSE_DATE('%Y-%m-%d', date) > (SELECT MAX(date_col) FROM {{ this }})
{% endif %}
