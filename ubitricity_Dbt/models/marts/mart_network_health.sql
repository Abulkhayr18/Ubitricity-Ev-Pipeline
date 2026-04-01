with base as (
    select * from {{ ref('stg_ev_stations') }}
)

select
    state                                                               as region,
    count(*)                                                            as total_chargers,
    sum(case when status = 'OPERATIONAL' then 1 else 0 end)            as operational,
    sum(case when status = 'FAULTED'     then 1 else 0 end)            as faulted,
    sum(case when status = 'PLANNED'     then 1 else 0 end)            as planned,
    round(sum(case when status = 'OPERATIONAL' then 1 else 0 end)
          / count(*) * 100, 2)                                          as operational_pct,
    round(sum(case when status = 'FAULTED' then 1 else 0 end)
          / count(*) * 100, 2)                                          as fault_rate_pct
from base
group by state
order by total_chargers desc