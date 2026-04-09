-- CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.currency_rates (
--   rate_date STRING,
--   currency_code STRING,
--   exchange_rate DOUBLE,
--   base_currency STRING
-- )
-- PARTITIONED BY (ingested_at STRING)
-- STORED AS PARQUET
-- LOCATION 's3://global-commodity-data-lake-commo-dev/silver/currency/'
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
    select * from {{ source('athena_silver', 'currency_rates') }}
)
select
    -- Ambil 10 karakter pertama saja (YYYY-MM-DD) baru di-cast ke DATE
    CAST(SUBSTRING(rate_date, 1, 10) AS DATE) as rate_date,
    currency_code,
    CAST(exchange_rate AS DOUBLE) as exchange_rate,
    base_currency,
    CAST(SUBSTRING(ingested_at, 1, 10) AS DATE) as event_timestamp
from source