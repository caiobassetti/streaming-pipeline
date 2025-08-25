
  
    

  create  table "demo"."public"."fct_events_by_action__dbt_tmp"
  
  
    as
  
  (
    select
  action,
  count(*) as n
from "demo"."public"."stg_raw_events"
group by action
order by n desc
  );
  