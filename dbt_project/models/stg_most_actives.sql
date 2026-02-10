with source_data as (
    select *
    from {{ source('raw', 'most_actives_raw') }}
),
standardized as (
    select
        nullif(trim(symbol), '') as symbol,
        nullif(trim(entity_name), '') as entity_name,
        coalesce(nullif(trim(category), ''), 'Unknown') as category,
        coalesce(nullif(trim(location), ''), 'Unknown') as location,
        nullif(trim(exchange), '') as exchange,
        nullif(trim(currency), '') as currency,

        try_to_decimal(price, 18, 4) as price,
        try_to_number(market_cap) as market_cap,
        try_to_number(volume) as volume,
        try_to_number(avg_volume_3m) as avg_volume_3m,

        source_url,
        try_to_timestamp_tz(scraped_at) as scraped_at,
        to_date(try_to_timestamp_tz(scraped_at)) as scraped_date,
        ingested_at,
        source_file_name,
        source_row_number
    from source_data
),
deduped as (
    select
        sha2(
            concat_ws(
                '|',
                coalesce(symbol, ''),
                coalesce(entity_name, ''),
                coalesce(exchange, '')
            ),
            256
        ) as entity_sk,
        sha2(
            concat_ws(
                '|',
                coalesce(symbol, ''),
                coalesce(entity_name, ''),
                coalesce(exchange, ''),
                coalesce(to_varchar(scraped_at), '')
            ),
            256
        ) as observation_sk,
        *
    from standardized
    qualify row_number() over (
        partition by symbol, entity_name, exchange, scraped_at
        order by ingested_at desc nulls last
    ) = 1
)
select *
from deduped
