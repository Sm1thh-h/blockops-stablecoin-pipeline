{{ config(materialized='table') }}

-- Grain: one row per entity, chain, stablecoin and day
-- Pivots the flow column (in / out / self) into separate columns.
-- No joins and no judgments here; those belong in the mart.

select
    entity_slug,
    chain,
    chain_family,
    stablecoin,
    activity_date,

    -- volumes (USD)
    sum(if(flow = 'in',   raw_volume_usd, 0)) as inflow_usd,
    sum(if(flow = 'out',  raw_volume_usd, 0)) as outflow_usd,
    sum(if(flow = 'self', raw_volume_usd, 0)) as internal_usd,

    -- transfer counts
    sum(if(flow = 'in',   transfer_count, 0)) as inflow_count,
    sum(if(flow = 'out',  transfer_count, 0)) as outflow_count,
    sum(if(flow = 'self', transfer_count, 0)) as internal_count,

    -- derived
    sum(if(flow in ('in', 'out'), raw_volume_usd, 0)) as external_volume_usd,
    sum(raw_volume_usd)                               as gross_volume_usd,

    -- flags for the data-quality checks we discussed
    sum(if(flow = 'self', raw_volume_usd, 0))
      > sum(if(flow = 'in', raw_volume_usd, 0))       as internal_exceeds_inflow,

    max(data_source) as data_source
from {{ ref('stg_arkham_histogram') }}
group by 1, 2, 3, 4, 5