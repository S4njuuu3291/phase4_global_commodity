{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['commodity_id']
) }}

WITH
-- 1. Definisikan Tabel Sumber
p AS (
    SELECT * FROM {{ ref('fact_commodity_prices') }}
),
n AS (
    SELECT * FROM {{ ref('fact_commodity_news') }}
),
fx AS (
    SELECT * FROM {{ ref('fact_currency_exchange_rates') }}
),
m AS (
    SELECT
        date_col AS date,
        date_str,
        series_id,
        CAST(value AS NUMERIC) AS macro_value -- Pastikan nilai sudah CAST
    FROM {{ ref('fact_macro_observations') }}
),

-- ✅ KOREKSI 1: Membersihkan dan Memilih Satu Nilai Kurs (Mengatasi Duplikasi)
fx_cleaned AS (
    SELECT
        date,
        rate,
        target_currency,
        -- Beri peringkat. Ambil 1 nilai kurs per tanggal, per target mata uang.
        ROW_NUMBER() OVER(
            PARTITION BY date, base_currency, target_currency
            ORDER BY rate DESC -- Jika ada duplikasi, pilih nilai tertinggi
        ) AS rn
    FROM fx
    -- Asumsi base_currency selalu 'USD'
    WHERE base_currency = 'USD' AND target_currency IN ('IDR', 'JPY', 'CNY', 'EUR')
),

-- ✅ KOREKSI 2: Melakukan PIVOT Kurs Mata Uang (Menggabungkan beberapa kurs per tanggal)
currency_pivot AS (
    SELECT
        date,
        MAX(CASE WHEN target_currency = 'IDR' THEN rate END) AS rate_to_idr,
        MAX(CASE WHEN target_currency = 'JPY' THEN rate END) AS rate_to_jpy,
        MAX(CASE WHEN target_currency = 'CNY' THEN rate END) AS rate_to_cny,
        MAX(CASE WHEN target_currency = 'EUR' THEN rate END) AS rate_to_eur
    FROM fx_cleaned
    WHERE rn = 1 -- Hanya gunakan data yang sudah dibersihkan (tanpa duplikasi)
    GROUP BY date
),

-- CTE Makro: CPI
macro_cpi AS (
    SELECT
        m.date AS macro_date,
        m.macro_value,
        p.date AS commodity_date,
        ROW_NUMBER() OVER(
            PARTITION BY p.date
            ORDER BY m.date DESC
        ) AS rn
    FROM p
    CROSS JOIN m
    WHERE m.series_id = 'CPIAUCSL'
    AND m.date <= p.date
),

-- CTE Makro: USD Index
macro_usd_index AS (
    SELECT
        m.date AS macro_date,
        m.macro_value,
        p.date AS commodity_date,
        ROW_NUMBER() OVER(
            PARTITION BY p.date
            ORDER BY m.date DESC
        ) AS rn
    FROM p
    CROSS JOIN m
    WHERE m.series_id = 'DTWEXBGS'
    AND m.date <= p.date
),

-- CTE Makro: Treasury Yield
macro_treasury_yield AS (
    SELECT
        m.date AS macro_date,
        m.macro_value,
        p.date AS commodity_date,
        ROW_NUMBER() OVER(
            PARTITION BY p.date
            ORDER BY m.date DESC
        ) AS rn
    FROM p
    CROSS JOIN m
    WHERE m.series_id = 'DGS10'
    AND m.date <= p.date
)

-- 3. Final SELECT (Gabungkan semua CTE ke dalam data Komoditas)
SELECT
    p.date,
    p.commodity_id,
    p.price,
    n.news_count,

    -- ✅ KOREKSI 3: Perhitungan Harga dalam Mata Uang Lokal
    (p.price * cp.rate_to_idr) AS idr_price,
    (p.price * cp.rate_to_jpy) AS jpy_price,
    (p.price * cp.rate_to_cny) AS cny_price,
    (p.price * cp.rate_to_eur) AS eur_price,

    -- CPI
    cpi.macro_value AS cpi_value,
    cpi.macro_date AS cpi_date,

    -- USD Index
    usd.macro_value AS usd_index_value,
    usd.macro_date AS usd_index_date,

    -- Treasury Yield
    ty.macro_value AS treasury_yield_value,
    ty.macro_date AS treasury_yield_date

FROM p
LEFT JOIN n USING (date, commodity_id)
-- ✅ KOREKSI 4: JOIN ke CTE Pivot Kurs yang sudah dibersihkan
LEFT JOIN currency_pivot AS cp ON cp.date = p.date

-- Gabungkan CTE Makro (LOCF)
LEFT JOIN macro_cpi AS cpi ON cpi.commodity_date = p.date AND cpi.rn = 1
LEFT JOIN macro_usd_index AS usd ON usd.commodity_date = p.date AND usd.rn = 1
LEFT JOIN macro_treasury_yield AS ty ON ty.commodity_date = p.date AND ty.rn = 1
