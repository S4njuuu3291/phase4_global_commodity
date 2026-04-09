-- CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.metal_prices (
--   event_timestamp STRING,
--   metal_symbol STRING,
--   price_usd DOUBLE,
--   currency_base STRING,
--   unit STRING
-- )
-- PARTITIONED BY (ingested_at STRING)
-- STORED AS PARQUET
-- LOCATION 's3://global-commodity-data-lake-commo-dev/silver/metal/'
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
    select * from {{ source('athena_silver', 'metal_prices') }}
)
select
    -- Ambil 10 karakter pertama saja (YYYY-MM-DD) baru di-cast ke DATE
    CAST(SUBSTRING(ingested_at, 1, 10) AS DATE) as trade_date,
    metal_symbol as commodity_code,
    CAST(price_usd AS DOUBLE) as price_usd,
    unit,
    CAST(SUBSTRING(CAST(event_timestamp AS VARCHAR), 1, 19) AS TIMESTAMP) as event_timestamp
from source