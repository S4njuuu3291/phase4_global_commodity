{{ config(
    materialized='table',
    format='parquet',
    partitioned_by=['trade_date']
) }}

with metal as (
    select * from {{ ref('stg_metal_prices') }}
),
currency as (
    select * from {{ ref('stg_currency_rates') }}
    where currency_code = 'IDR'
),
news as (
    select 
        CAST(SUBSTRING(CAST(event_timestamp AS VARCHAR), 1, 10) AS DATE) as news_date,
        topics,
        sum(total_mentions) as daily_mentions
    from {{ ref('stg_news_data') }}
    group by 1, 2
),
-- Ambil data FRED terbaru untuk setiap indikator
fred_latest as (
    select 
        series_id,
        observation_values,
        row_number() over (partition by series_id order by observation_date desc) as rn
    from {{ ref('stg_fred_data') }}
),
-- PIVOT: Ubah baris jadi kolom agar tetap 1 baris per commodity
fred_pivoted as (
    select
        max(case when series_id = 'DGS10' then observation_values end) as yield_10y,
        max(case when series_id = 'DTWEXBGS' then observation_values end) as usd_index,
        max(case when series_id = 'CPIAUCSL' then observation_values end) as cpi_index
    from fred_latest
    where rn = 1
)

select
    m.commodity_code,
    m.price_usd as price_usd_per_gr,
    c.exchange_rate as usd_to_idr,
    (m.price_usd * c.exchange_rate) as price_idr_per_gr,
    coalesce(n.daily_mentions, 0) as news_hype_count,
    -- Data Ekonomi Lengkap (Pivoted)
    f.yield_10y,
    f.usd_index,
    f.cpi_index,
    m.trade_date
from metal m
left join currency c on m.trade_date = c.rate_date
left join news n on m.trade_date = n.news_date and m.commodity_code = n.topics
-- CROSS JOIN karena fred_pivoted cuma berisi tepat 1 baris sakti
cross join fred_pivoted f