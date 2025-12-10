{{ config(
    materialized='incremental',
    unique_key='date || "-" || base_currency || "-" || target_currency',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['base_currency']
) }}

SELECT
    DATE(currency_date) AS date,
    currency_base AS base_currency,
    target_currency,
    rate
FROM {{ ref('stg_currency_exchange_rates') }}
UNPIVOT(rate FOR target_currency IN (
    currency_idr AS 'IDR',
    currency_eur AS 'EUR',
    currency_jpy AS 'JPY',
    currency_cny AS 'CNY'
))
{% if is_incremental() %}
WHERE DATE(currency_date) > (SELECT MAX(date) FROM {{ this }})
{% endif %}
