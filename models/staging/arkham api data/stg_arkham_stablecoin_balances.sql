{{ config(materialized='view') }}

{#-
    Flattens Arkham's /balances/entity/{entity} response into one row per
    entity x blockchain x stablecoin. USDT and USDC only.

    Source JSON shape (per entity):
      { "totalBalance": {...},
        "balances": { "<chain>": [ {id, symbol, name, balance, usd, price, quoteTime, ...}, ... ] } }

    Why the Jinja loop: the chain names are JSON *keys*, not array items, and
    BigQuery needs literal JSON paths, so we generate one block per chain.
    If Arkham adds a chain you care about, add it to this list.
-#}
{% set chains = [
    'arbitrum_one', 'avalanche', 'base', 'bsc', 'ethereum', 'flare',
    'hyperevm', 'optimism', 'polygon', 'solana', 'tron'
] %}

with source as (

    select entity_slug, raw_json, pulled_at
    from {{ source('raw_arkham', 'entity_endpoints_raw') }}
    where endpoint = 'balances'

),

-- Keep only the most recent snapshot per entity so balances are not double counted.
-- (BigQuery requires a WHERE/GROUP BY/HAVING alongside QUALIFY, hence "where true".)
latest as (

    select *
    from source
    where true
    qualify row_number() over (partition by entity_slug order by pulled_at desc) = 1

),

tokens as (

{% for chain in chains %}
    select
        entity_slug,
        pulled_at,
        '{{ chain }}' as blockchain,
        token
    from latest,
        unnest(json_query_array(raw_json, '$.balances.{{ chain }}')) as token
    {% if not loop.last %}union all{% endif %}
{% endfor %}

)

select
    entity_slug,
    blockchain,
    json_value(token, '$.id')                                   as arkham_token_id,
    json_value(token, '$.symbol')                               as stablecoin,
    case json_value(token, '$.id')
        when 'tether'   then 'Tether'
        when 'usd-coin' then 'Circle'
    end                                                         as issuer,
    safe_cast(json_value(token, '$.balance')   as float64)      as balance_native,
    safe_cast(json_value(token, '$.usd')       as float64)      as balance_usd,
    safe_cast(json_value(token, '$.price')     as float64)      as price_usd,
    safe_cast(json_value(token, '$.quoteTime') as timestamp)    as price_quoted_at,
    pulled_at,
    'Arkham /balances/entity'                                   as data_source
from tokens
-- Filter on Arkham's token id, not the symbol: anyone can deploy a fake token
-- with the symbol "USDT", but the id identifies the real asset.
where json_value(token, '$.id') in ('tether', 'usd-coin')

{{ config(materialized='view') }}

{#-
    Flattens Arkham's /balances/entity/{entity} response into one row per
    entity x blockchain x token, for every token listed in the stablecoin_registry seed.

    Source JSON shape (per entity):
      { "totalBalance": {...},
        "balances": { "<chain>": [ {id, symbol, name, balance, usd, price, quoteTime, ...}, ... ] } }

    Why the Jinja loop: the chain names are JSON *keys*, not array items, and
    BigQuery needs literal JSON paths, so we generate one block per chain.
    If Arkham adds a chain you care about, add it to this list.

    Which tokens count is decided by the registry seed, not by SQL. Add a row to
    seeds/stablecoin_registry.csv to bring a new token in. Joining on Arkham's token id
    (not the symbol) keeps fake tokens that copy a symbol like "USDT" out.

    CAUTION: balance_usd now includes several kinds of token. Do not sum it blindly.
    Filter on include_in_stablecoin_total, or group by reporting_group.
-#}
{% set chains = [
    'arbitrum_one', 'avalanche', 'base', 'bsc', 'ethereum', 'flare',
    'hyperevm', 'optimism', 'polygon', 'solana', 'tron'
] %}

with source as (

    select entity_slug, raw_json, pulled_at
    from {{ source('raw_arkham', 'entity_endpoints_raw') }}
    where endpoint = 'balances'

),

-- Keep only the most recent snapshot per entity so balances are not double counted.
-- (BigQuery requires a WHERE/GROUP BY/HAVING alongside QUALIFY, hence "where true".)
latest as (

    select *
    from source
    where true
    qualify row_number() over (partition by entity_slug order by pulled_at desc) = 1

),

tokens as (

{% for chain in chains %}
    select
        entity_slug,
        pulled_at,
        '{{ chain }}' as blockchain,
        token
    from latest,
        unnest(json_query_array(raw_json, '$.balances.{{ chain }}')) as token
    {% if not loop.last %}union all{% endif %}
{% endfor %}

)

select
    tokens.entity_slug,
    tokens.blockchain,
    json_value(tokens.token, '$.id')                                as arkham_token_id,
    json_value(tokens.token, '$.symbol')                            as stablecoin,        -- symbol as Arkham reports it
    reg.symbol                                                      as registry_symbol,
    reg.issuer,
    reg.issuer_origin,
    reg.peg_currency,
    reg.category,
    reg.reporting_group,
    reg.include_in_stablecoin_total,
    safe_cast(json_value(tokens.token, '$.balance')   as float64)   as balance_native,
    safe_cast(json_value(tokens.token, '$.usd')       as float64)   as balance_usd,
    safe_cast(json_value(tokens.token, '$.price')     as float64)   as price_usd,
    safe_cast(json_value(tokens.token, '$.quoteTime') as timestamp) as price_quoted_at,
    tokens.pulled_at,
    'Arkham /balances/entity'                                       as data_source
from tokens
inner join {{ ref('stablecoin_registry') }} as reg
    on reg.arkham_token_id = json_value(tokens.token, '$.id')