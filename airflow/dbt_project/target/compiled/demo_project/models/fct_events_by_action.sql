select
  action,
  count(*) as n
from "demo"."public"."stg_raw_events"
group by action
order by n desc