-- ============================================================
-- FRED DATA TABLE (Silver Layer)
-- Source: fetch_fred_data() → transform_fred_data()
-- Model: FredDataModel
-- ============================================================
-- CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.fred_data (
--   series_id STRING,
--   observation_date STRING,
--   observation_values DOUBLE,
--   units STRING
-- )
-- PARTITIONED BY (ingested_at STRING)
-- STORED AS PARQUET
-- LOCATION 's3://global-commodity-data-lake-commo-dev/silver/fred/'
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
    select * from {{ source('athena_silver', 'fred_data') }}
)
select
    series_id,
    -- Ambil 10 karakter pertama saja (YYYY-MM-DD) baru di-cast ke DATE
    CAST(SUBSTRING(observation_date, 1, 10) AS DATE) as observation_date,
    CAST(observation_values AS DOUBLE) as observation_values,
    units,
    CAST(SUBSTRING(ingested_at, 1, 10) AS DATE) as event_timestamp
from source