select
  action,
  count(*) as n
from {{ ref('stg_raw_events') }}
group by action
order by n desc
