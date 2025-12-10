{{ config(
    materialized='incremental',
    unique_key='commodity_id || "-" || date',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['commodity_id']
) }}

SELECT
    DATE(news_timestamp) AS date,
    commodity_id,
    news_count
FROM {{ ref('stg_commodity_prices') }}
UNPIVOT(news_count FOR commodity_id IN (
    gold_news_count AS 'gold',
    silver_news_count AS 'silver',
    platinum_news_count AS 'platinum',
    nickel_news_count AS 'nickel',
    copper_news_count AS 'copper'
))
{% if is_incremental() %}
WHERE DATE(news_timestamp) > (SELECT MAX(date) FROM {{ this }})
{% endif %}
