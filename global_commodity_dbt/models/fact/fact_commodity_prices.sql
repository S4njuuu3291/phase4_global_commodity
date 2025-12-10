{{ config(
    materialized='incremental',
    unique_key='commodity_id || "-" || date',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['commodity_id']
) }}

SELECT
    DATE(metal_timestamp) AS date,
    commodity_id,
    price,
    currency_base
FROM {{ ref('stg_commodity_prices') }}
UNPIVOT(price FOR commodity_id IN (
    gold_price AS 'gold',
    silver_price AS 'silver',
    platinum_price AS 'platinum',
    nickel_price AS 'nickel',
    copper_price AS 'copper'
))
{% if is_incremental() %}
WHERE DATE(metal_timestamp) > (SELECT MAX(date) FROM {{ this }})
{% endif %}
