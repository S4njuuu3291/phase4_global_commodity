
-- -- ============================================================
-- -- NEWS DATA TABLE (Silver Layer)
-- -- Source: fetch_news() → transform_news()
-- -- Model: NewsCountModel
-- -- ============================================================
-- CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.news_data (
--   news_timestamp STRING,
--   keywords STRING,
--   total_mentions INT,
--   status STRING
-- )
-- PARTITIONED BY (ingested_at STRING)
-- STORED AS PARQUET
-- LOCATION 's3://global-commodity-data-lake-commo-dev/silver/news/'
-- TBLPROPERTIES (
--   "parquet.compression"="SNAPPY",
--   "projection.enabled"="true",
--   "projection.ingested_at.type"="date",
--   "projection.ingested_at.format"="yyyy-MM-dd",
--   "projection.ingested_at.range"="2026-01-01,NOW",
--   "projection.ingested_at.interval"="1",
--   "projection.ingested_at.interval.unit"="DAYS"
-- );

with source as (
    select * from {{ source('athena_silver', 'news_data') }}
)
select
    -- Ambil 10 karakter pertama saja (YYYY-MM-DD) baru di-cast ke TIMESTAMP, sisanya ambil 19 karakter termasuk jam:menit:detik
    CAST(SUBSTRING(CAST(news_timestamp AS VARCHAR), 1, 19) AS TIMESTAMP) as news_timestamp,
    keywords as topics,
    total_mentions,
    status,
    CAST(SUBSTRING(ingested_at, 1, 10) AS DATE) as event_timestamp
from source