{{ config(
    materialized='incremental',
    unique_key='series_id || "-" || date',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['series_id']
) }}

SELECT
    date,
    series_id,
    value
FROM {{ ref('stg_fred_macro_observations') }}
{% if is_incremental() %}
WHERE date > (SELECT MAX(date) FROM {{ this }})
{% endif %}
