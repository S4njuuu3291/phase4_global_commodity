{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['commodity_id']
) }}

WITH p AS (
    SELECT *
    FROM {{ ref('fact_commodity_prices') }}
),
n AS (
    SELECT *
    FROM {{ ref('fact_commodity_news') }}
),
-- ✅ PERBAIKAN 1: Filter hanya untuk kurs USD ke IDR
fx_idr AS (
    SELECT
        date,
        rate
    FROM {{ ref('fact_currency_exchange_rates') }}
    -- Ganti 'USD' dan 'IDR' dengan nama kolom yang sesuai di tabel Anda jika berbeda.
    -- Misalnya, jika ada kolom 'currency_pair', filternya mungkin WHERE currency_pair = 'USD/IDR'
    WHERE base_currency = 'USD' AND target_currency = 'IDR'
),
m AS (
    SELECT *
    FROM {{ ref('fact_macro_observations') }}
),

macro_pivot AS (
    SELECT
        date,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi,
        MAX(CASE WHEN series_id = 'DTWEXBGS' THEN value END) AS usd_index,
        MAX(CASE WHEN series_id = 'DGS10' THEN value END) AS treasury_yield
    FROM m
    GROUP BY date
)

SELECT
    p.date,
    p.commodity_id,
    p.price,
    n.news_count,
    -- ✅ PERBAIKAN 2: Menggunakan alias yang sudah difilter
    fx.rate AS usd_to_idr,
    macro.cpi,
    macro.usd_index,
    macro.treasury_yield
FROM p
LEFT JOIN n USING (date, commodity_id)
-- ✅ PERBAIKAN 3: Menggabungkan dengan alias 'fx_idr' yang sudah difilter
LEFT JOIN fx_idr AS fx ON fx.date = p.date
LEFT JOIN macro_pivot AS macro ON macro.date = p.date
