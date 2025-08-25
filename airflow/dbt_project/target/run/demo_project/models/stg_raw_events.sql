
  
    

  create  table "demo"."public"."stg_raw_events__dbt_tmp"
  
  
    as
  
  (
    select
  event_id::uuid as event_id,
  ts::timestamptz as ts,
  user_id::int as user_id,
  campaign_id::int as campaign_id,
  action::text as action,
  page::text as page
from public.raw_events
  );
  