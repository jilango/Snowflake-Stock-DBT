-- Phase 2: Snowflake schema and table design
-- Raw ingestion table + clean analytics table with hash key strategy.

-- Optional role/warehouse context (adjust per environment)
-- USE ROLE <your_role>;
-- USE WAREHOUSE <your_warehouse>;

CREATE DATABASE IF NOT EXISTS STOCK_PIPELINE_DB;
CREATE SCHEMA IF NOT EXISTS STOCK_PIPELINE_DB.RAW;
CREATE SCHEMA IF NOT EXISTS STOCK_PIPELINE_DB.ANALYTICS;

-- Raw ingestion layer (file-shaped, permissive datatypes)
CREATE OR REPLACE TABLE STOCK_PIPELINE_DB.RAW.MOST_ACTIVES_RAW (
    load_id STRING DEFAULT UUID_STRING(),
    source_file_name STRING,
    source_row_number NUMBER(38, 0),
    ingested_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    symbol STRING,
    entity_name STRING,
    category STRING,
    location STRING,
    exchange STRING,
    currency STRING,
    price STRING,
    market_cap STRING,
    volume STRING,
    avg_volume_3m STRING,
    source_url STRING,
    scraped_at STRING
);

-- Clean analytics-friendly layer (typed + dedupe-ready keys)
CREATE OR REPLACE TABLE STOCK_PIPELINE_DB.ANALYTICS.MOST_ACTIVES_CLEAN (
    observation_sk STRING,                    -- Hash key: entity + scraped_at timestamp
    entity_sk STRING,                         -- Hash key: symbol + entity_name + exchange

    symbol STRING NOT NULL,
    entity_name STRING NOT NULL,
    category STRING,
    location STRING,
    exchange STRING,
    currency STRING,

    price NUMBER(18, 4),
    market_cap NUMBER(38, 0),
    volume NUMBER(38, 0),
    avg_volume_3m NUMBER(38, 0),

    source_url STRING,
    scraped_at TIMESTAMP_TZ,
    scraped_date DATE,
    loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),

    -- Note: uniqueness in Snowflake should still be validated in QA queries.
    CONSTRAINT uq_observation_sk UNIQUE (observation_sk)
);
