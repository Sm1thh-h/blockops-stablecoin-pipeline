-- Every entity must have exactly one primary layer in entity_layers,
-- and that layer must match entity_registry.value_chain_layer.
-- Returns one row per problem entity, with the details needed to fix it.
-- Checks both directions: registry entities missing a primary layer, and
-- primary-layer rows whose entity is not in the registry.
with primary_layers as (
    select
        entity_slug,
        count(*)                                                       as n_primary,
        string_agg(value_chain_layer, ', ' order by value_chain_layer) as primary_layers
    from {{ ref('entity_layers') }}
    where is_primary = true
    group by entity_slug
)

select
    coalesce(r.entity_slug, p.entity_slug) as entity_slug,
    r.value_chain_layer                    as registry_layer,
    p.primary_layers                       as layers_marked_primary,
    coalesce(p.n_primary, 0)               as n_primary
from {{ ref('entity_registry') }} as r
full outer join primary_layers as p
    on r.entity_slug = p.entity_slug
where r.entity_slug is null                         -- primary layer for an entity not in the registry
   or p.entity_slug is null                         -- registry entity with no primary layer
   or p.n_primary != 1                              -- more than one primary layer
   or p.primary_layers != r.value_chain_layer       -- primary layer disagrees with the registry