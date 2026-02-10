-- Phase 4: analytics-ready stakeholder queries
-- Uses dbt models:
--   STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY
--   STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS

/* 1) Top 10 entities by chosen metric (market cap) */
SELECT
    d.symbol,
    d.entity_name,
    d.category,
    d.location,
    MAX(f.market_cap) AS latest_market_cap
FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS AS f
JOIN STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY AS d
    ON f.entity_sk = d.entity_sk
GROUP BY
    d.symbol,
    d.entity_name,
    d.category,
    d.location
ORDER BY latest_market_cap DESC
LIMIT 10;

/* 2) Metric trend over time (daily average price and volume) */
SELECT
    f.observation_date,
    AVG(f.price) AS avg_price,
    AVG(f.volume) AS avg_volume,
    COUNT(DISTINCT f.entity_sk) AS active_entities
FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS AS f
GROUP BY f.observation_date
ORDER BY f.observation_date;

/* 3) Outlier detection using z-score on daily volume */
WITH volume_stats AS (
    SELECT
        AVG(volume) AS mean_volume,
        STDDEV(volume) AS stddev_volume
    FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS
    WHERE volume IS NOT NULL
),
scored AS (
    SELECT
        f.observation_date,
        d.symbol,
        d.entity_name,
        d.category,
        f.volume,
        (f.volume - s.mean_volume) / NULLIF(s.stddev_volume, 0) AS volume_z_score
    FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS AS f
    JOIN STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY AS d
        ON f.entity_sk = d.entity_sk
    CROSS JOIN volume_stats AS s
    WHERE f.volume IS NOT NULL
)
SELECT
    observation_date,
    symbol,
    entity_name,
    category,
    volume,
    volume_z_score
FROM scored
WHERE ABS(volume_z_score) >= 3
ORDER BY ABS(volume_z_score) DESC;

/* 4) Geographic breakdown by country/region */
SELECT
    COALESCE(d.location, 'Unknown') AS location,
    COUNT(DISTINCT f.entity_sk) AS entity_count,
    AVG(f.price) AS avg_price,
    AVG(f.market_cap) AS avg_market_cap,
    SUM(f.volume) AS total_volume
FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS AS f
JOIN STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY AS d
    ON f.entity_sk = d.entity_sk
GROUP BY COALESCE(d.location, 'Unknown')
ORDER BY total_volume DESC;

/* 5) Stakeholder query: sector risk watchlist by volume spikes with weak price momentum */
WITH base AS (
    SELECT
        f.observation_date,
        f.entity_sk,
        d.symbol,
        d.entity_name,
        d.category,
        f.price,
        f.volume
    FROM STOCK_PIPELINE_DB.ANALYTICS.FCT_OBSERVATIONS AS f
    JOIN STOCK_PIPELINE_DB.ANALYTICS.DIM_ENTITY AS d
        ON f.entity_sk = d.entity_sk
),
windowed AS (
    SELECT
        observation_date,
        entity_sk,
        symbol,
        entity_name,
        category,
        price,
        volume,
        AVG(volume) OVER (
            PARTITION BY entity_sk
            ORDER BY observation_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS volume_7d_avg,
        LAG(price, 7) OVER (
            PARTITION BY entity_sk
            ORDER BY observation_date
        ) AS price_7d_ago
    FROM base
),
flags AS (
    SELECT
        observation_date,
        symbol,
        entity_name,
        category,
        volume,
        volume_7d_avg,
        price,
        price_7d_ago,
        CASE
            WHEN volume_7d_avg IS NULL OR volume_7d_avg = 0 THEN NULL
            ELSE volume / volume_7d_avg
        END AS volume_spike_ratio,
        CASE
            WHEN price_7d_ago IS NULL OR price_7d_ago = 0 THEN NULL
            ELSE (price - price_7d_ago) / price_7d_ago
        END AS price_momentum_7d
    FROM windowed
)
SELECT
    observation_date,
    category,
    symbol,
    entity_name,
    volume,
    volume_7d_avg,
    volume_spike_ratio,
    price,
    price_momentum_7d
FROM flags
WHERE volume_spike_ratio >= 1.5
  AND price_momentum_7d <= 0
ORDER BY volume_spike_ratio DESC, price_momentum_7d ASC
LIMIT 50;
