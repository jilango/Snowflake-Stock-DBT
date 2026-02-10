with staged as (
    select *
    from {{ ref('stg_most_actives') }}
),
fact_rows as (
    select
        observation_sk,
        entity_sk,
        symbol,
        scraped_date as observation_date,
        scraped_at,
        price,
        market_cap,
        volume,
        avg_volume_3m,
        source_url
    from staged
)
select
    observation_sk,
    entity_sk,
    symbol,
    observation_date,
    scraped_at,
    price,
    market_cap,
    volume,
    avg_volume_3m,
    source_url
from fact_rows
