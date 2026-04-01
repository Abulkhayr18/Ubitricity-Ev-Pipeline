with base as (
    select *,
        case
            when lower(operator) like '%ubitricity%' then 'Ubitricity'
            else 'Competitor'
        end as operator_type
    from {{ ref('stg_ev_stations') }}
)

select
    operator,
    operator_type,
    state                           as region,
    count(*)                        as total_chargers,
    sum(num_connectors)             as total_connectors
from base
group by operator, operator_type, state
order by total_chargers desc