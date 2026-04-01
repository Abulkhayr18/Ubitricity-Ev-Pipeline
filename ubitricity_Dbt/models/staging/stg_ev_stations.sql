with source as (
    select * from delta.`/Volumes/workspace/ev-energy/ev-insight/silver/ev_stations`
)

select
    id,
    title,
    address,
    town,
    state,
    postcode,
    country,
    lat,
    lon,
    operator,
    status,
    num_connectors,
    connector_types,
    ingested_at,
    cleaned_at
from source