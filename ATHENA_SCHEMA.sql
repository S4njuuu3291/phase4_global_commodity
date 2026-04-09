-- ============================================================
-- ATHENA EXTERNAL TABLES FOR COMMODITY DATA LAKE
-- Database: ds_commodity_staged
-- Format: Parquet with Hive-style partitioning
-- ============================================================

-- ============================================================
-- METAL PRICES TABLE (Silver Layer)
-- Source: fetch_metal_prices() → transform_metal_prices()
-- Model: ProcessedMetalModel
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.metal_prices (
  event_timestamp STRING,
  metal_symbol STRING,
  price_usd DOUBLE,
  currency_base STRING,
  unit STRING
)
PARTITIONED BY (ingested_at STRING)
STORED AS PARQUET
LOCATION 's3://global-commodity-data-lake-commo-dev/silver/metal/'
TBLPROPERTIES (
  "parquet.compression"="SNAPPY",
  "projection.enabled"="true",
  "projection.ingested_at.type"="date",
  "projection.ingested_at.format"="yyyy-MM-dd",
  "projection.ingested_at.range"="2026-01-01,NOW",
  "projection.ingested_at.interval"="1",
  "projection.ingested_at.interval.unit"="DAYS"
);

-- ============================================================
-- CURRENCY RATES TABLE (Silver Layer)
-- Source: fetch_currency_rate() → transform_currency_rates()
-- Model: CurrencyRateModel
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.currency_rates (
  rate_date STRING,
  currency_code STRING,
  exchange_rate DOUBLE,
  base_currency STRING
)
PARTITIONED BY (ingested_at STRING)
STORED AS PARQUET
LOCATION 's3://global-commodity-data-lake-commo-dev/silver/currency/'
TBLPROPERTIES (
  "parquet.compression"="SNAPPY",
  "projection.enabled"="true",
  "projection.ingested_at.type"="date",
  "projection.ingested_at.format"="yyyy-MM-dd",
  "projection.ingested_at.range"="2026-01-01,NOW",
  "projection.ingested_at.interval"="1",
  "projection.ingested_at.interval.unit"="DAYS"
);

-- ============================================================
-- FRED DATA TABLE (Silver Layer)
-- Source: fetch_fred_data() → transform_fred_data()
-- Model: FredDataModel
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.fred_data (
  series_id STRING,
  observation_date STRING,
  observation_values DOUBLE,
  units STRING
)
PARTITIONED BY (ingested_at STRING)
STORED AS PARQUET
LOCATION 's3://global-commodity-data-lake-commo-dev/silver/fred/'
TBLPROPERTIES (
  "parquet.compression"="SNAPPY",
  "projection.enabled"="true",
  "projection.ingested_at.type"="date",
  "projection.ingested_at.format"="yyyy-MM-dd",
  "projection.ingested_at.range"="2026-01-01,NOW",
  "projection.ingested_at.interval"="1",
  "projection.ingested_at.interval.unit"="DAYS"
);

-- ============================================================
-- NEWS DATA TABLE (Silver Layer)
-- Source: fetch_news() → transform_news()
-- Model: NewsCountModel
-- ============================================================
CREATE EXTERNAL TABLE IF NOT EXISTS ds_commodity_staged.news_data (
  news_timestamp STRING,
  keywords STRING,
  total_mentions INT,
  status STRING
)
PARTITIONED BY (ingested_at STRING)
STORED AS PARQUET
LOCATION 's3://global-commodity-data-lake-commo-dev/silver/news/'
TBLPROPERTIES (
  "parquet.compression"="SNAPPY",
  "projection.enabled"="true",
  "projection.ingested_at.type"="date",
  "projection.ingested_at.format"="yyyy-MM-dd",
  "projection.ingested_at.range"="2026-01-01,NOW",
  "projection.ingested_at.interval"="1",
  "projection.ingested_at.interval.unit"="DAYS"
);

-- ============================================================
-- MSCK REPAIR TABLE SCRIPTS
-- ============================================================
-- MSCK REPAIR TABLE ds_commodity_staged.metal_prices;
-- MSCK REPAIR TABLE ds_commodity_staged.currency_rates;
-- MSCK REPAIR TABLE ds_commodity_staged.fred_data;
-- MSCK REPAIR TABLE ds_commodity_staged.news_data;