with base as (
    select * from {{ ref('stg_ev_stations') }}
)

select
    state                                                               as region,
    town,
    count(*)                                                            as total_chargers,
    sum(num_connectors)                                                 as total_connectors,
    sum(case when status = 'OPERATIONAL' then 1 else 0 end)            as active_chargers,
    round(sum(case when status = 'OPERATIONAL' then 1 else 0 end)
          / count(*) * 100, 2)                                          as coverage_score
from base
group by state, town
order by total_chargers asc