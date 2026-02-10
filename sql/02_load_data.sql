-- Phase 2: load scraped output into Snowflake
-- Assumes raw_data.csv is available at the referenced stage location.

USE DATABASE STOCK_PIPELINE_DB;
USE SCHEMA RAW;

-- 1) Named CSV file format for scraper output
CREATE OR REPLACE FILE FORMAT STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_CSV_FF
  TYPE = CSV
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  TRIM_SPACE = TRUE
  EMPTY_FIELD_AS_NULL = TRUE;

-- 2) Internal stage (upload raw_data.csv here)
CREATE OR REPLACE STAGE STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_STAGE
  FILE_FORMAT = STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_CSV_FF;

-- Example upload command from local machine:
-- PUT file:///absolute/path/to/raw_data.csv @STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- 3) Load CSV rows into raw ingestion table
COPY INTO STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_RAW
(
  source_file_name,
  source_row_number,
  symbol,
  entity_name,
  category,
  location,
  exchange,
  currency,
  price,
  market_cap,
  volume,
  avg_volume_3m,
  source_url,
  scraped_at
)
FROM
(
  SELECT
    METADATA$FILENAME::STRING,
    METADATA$FILE_ROW_NUMBER::NUMBER(38,0),
    $1::STRING,
    $2::STRING,
    $3::STRING,
    $4::STRING,
    $5::STRING,
    $6::STRING,
    $7::STRING,
    $8::STRING,
    $9::STRING,
    $10::STRING,
    $11::STRING,
    $12::STRING
  FROM @STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_STAGE
)
FILE_FORMAT = (FORMAT_NAME = STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_CSV_FF)
ON_ERROR = ABORT_STATEMENT;

-- Strict validation checks after raw load:
--  - verify total rows loaded
--  - verify required identity fields are present
--  - verify numeric fields can be parsed
SELECT
  COUNT(*) AS raw_row_count,
  COUNT_IF(symbol IS NULL OR TRIM(symbol) = '') AS missing_symbol_rows,
  COUNT_IF(entity_name IS NULL OR TRIM(entity_name) = '') AS missing_entity_name_rows,
  COUNT_IF(TRY_TO_DECIMAL(price, 18, 4) IS NULL) AS invalid_price_rows,
  COUNT_IF(TRY_TO_NUMBER(market_cap) IS NULL) AS invalid_market_cap_rows,
  COUNT_IF(TRY_TO_NUMBER(volume) IS NULL) AS invalid_volume_rows
FROM STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_RAW;

-- 4) Normalize and upsert into clean table
USE SCHEMA ANALYTICS;

MERGE INTO STOCK_PIPELINE_DB.ANALYTICS.MOST_ACTIVES_CLEAN AS tgt
USING (
  SELECT
    SHA2(CONCAT_WS('|',
      COALESCE(TRIM(symbol), ''),
      COALESCE(TRIM(entity_name), ''),
      COALESCE(TRIM(exchange), '')
    ), 256) AS entity_sk,
    SHA2(CONCAT_WS('|',
      COALESCE(TRIM(symbol), ''),
      COALESCE(TRIM(entity_name), ''),
      COALESCE(TRIM(exchange), ''),
      COALESCE(TRIM(scraped_at), '')
    ), 256) AS observation_sk,

    NULLIF(TRIM(symbol), '') AS symbol,
    NULLIF(TRIM(entity_name), '') AS entity_name,
    NULLIF(TRIM(category), '') AS category,
    NULLIF(TRIM(location), '') AS location,
    NULLIF(TRIM(exchange), '') AS exchange,
    NULLIF(TRIM(currency), '') AS currency,

    TRY_TO_DECIMAL(price, 18, 4) AS price,
    TRY_TO_NUMBER(market_cap) AS market_cap,
    TRY_TO_NUMBER(volume) AS volume,
    TRY_TO_NUMBER(avg_volume_3m) AS avg_volume_3m,

    source_url,
    TRY_TO_TIMESTAMP_TZ(scraped_at) AS scraped_at,
    TO_DATE(TRY_TO_TIMESTAMP_TZ(scraped_at)) AS scraped_date
  FROM STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_RAW
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol, entity_name, exchange, scraped_at
    ORDER BY ingested_at DESC
  ) = 1
) AS src
ON tgt.observation_sk = src.observation_sk
WHEN MATCHED THEN UPDATE SET
  tgt.entity_sk = src.entity_sk,
  tgt.symbol = src.symbol,
  tgt.entity_name = src.entity_name,
  tgt.category = src.category,
  tgt.location = src.location,
  tgt.exchange = src.exchange,
  tgt.currency = src.currency,
  tgt.price = src.price,
  tgt.market_cap = src.market_cap,
  tgt.volume = src.volume,
  tgt.avg_volume_3m = src.avg_volume_3m,
  tgt.source_url = src.source_url,
  tgt.scraped_at = src.scraped_at,
  tgt.scraped_date = src.scraped_date,
  tgt.loaded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  observation_sk,
  entity_sk,
  symbol,
  entity_name,
  category,
  location,
  exchange,
  currency,
  price,
  market_cap,
  volume,
  avg_volume_3m,
  source_url,
  scraped_at,
  scraped_date,
  loaded_at
) VALUES (
  src.observation_sk,
  src.entity_sk,
  src.symbol,
  src.entity_name,
  src.category,
  src.location,
  src.exchange,
  src.currency,
  src.price,
  src.market_cap,
  src.volume,
  src.avg_volume_3m,
  src.source_url,
  src.scraped_at,
  src.scraped_date,
  CURRENT_TIMESTAMP()
);

-- Strict validation checks after clean merge:
--  - no duplicate observation hash keys
--  - ensure entity hash key coverage
SELECT
  observation_sk,
  COUNT(*) AS duplicate_count
FROM STOCK_PIPELINE_DB.ANALYTICS.MOST_ACTIVES_CLEAN
GROUP BY observation_sk
HAVING COUNT(*) > 1;

SELECT
  COUNT(*) AS clean_row_count,
  COUNT_IF(entity_sk IS NULL OR TRIM(entity_sk) = '') AS missing_entity_sk_rows,
  COUNT_IF(observation_sk IS NULL OR TRIM(observation_sk) = '') AS missing_observation_sk_rows,
  COUNT_IF(scraped_date IS NULL) AS missing_scraped_date_rows
FROM STOCK_PIPELINE_DB.ANALYTICS.MOST_ACTIVES_CLEAN;
