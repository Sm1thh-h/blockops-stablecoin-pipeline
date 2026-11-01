-- models/staging/stg_arkham_histogram.sql
{{ config(materialized='view') }}

with src as (
    select
        entity_slug,
        chain,
        token_symbol,
        flow,
        raw_json,
        pulled_at
    from {{ source('raw_arkham', 'entity_transfers_raw') }}
    where endpoint = 'histogram'
),

unnested as (
    select
        entity_slug,
        chain,
        token_symbol as stablecoin,
        flow,
        date(timestamp(json_value(d, '$.time')))           as activity_date,
        cast(json_value(d, '$.count') as int64)            as transfer_count,
        cast(json_value(d, '$.usd') as float64)            as raw_volume_usd,
        pulled_at
    from src,
        unnest(json_extract_array(raw_json)) as d
),

deduped as (
    select *,
        row_number() over (
            partition by entity_slug, chain, stablecoin, flow, activity_date
            order by pulled_at desc
        ) as rn
    from unnested
)

select
    entity_slug,
    chain,
	case
		when chain in ('ethereum', 'base', 'bsc', 'arbitrum_one', 'avalanche', 'polygon') then 'evm'
		when chain = 'tron' then 'tron'
		when chain = 'solana' then 'solana'
    end as chain_family,
    stablecoin,
    flow,
    activity_date,
    transfer_count,
    raw_volume_usd,
    pulled_at,
    'arkham' as data_source
from deduped
where rn = 1