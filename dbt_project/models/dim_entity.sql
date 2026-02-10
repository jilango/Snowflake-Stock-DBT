with staged as (
    select *
    from {{ ref('stg_most_actives') }}
),
ranked as (
    select
        entity_sk,
        symbol,
        entity_name,
        category,
        location,
        exchange,
        currency,
        scraped_at,
        row_number() over (
            partition by entity_sk
            order by scraped_at desc nulls last
        ) as rn
    from staged
)
select
    entity_sk,
    symbol,
    entity_name,
    category,
    location,
    exchange,
    currency,
    scraped_at as last_seen_scraped_at
from ranked
where rn = 1
