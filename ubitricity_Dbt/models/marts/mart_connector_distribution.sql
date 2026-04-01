with exploded as (
    select
        state,
        operator,
        trim(connector_type.value)  as connector_type
    from {{ ref('stg_ev_stations') }}
    lateral view explode(split(connector_types, '\\|')) connector_type as value
)

select
    connector_type,
    state                           as region,
    count(*)                        as charger_count
from exploded
where connector_type != 'Unknown'
group by connector_type, state
order by charger_count desc