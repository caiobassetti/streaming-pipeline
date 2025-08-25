select
  event_id::uuid               as event_id,
  user_id::int                 as user_id,
  session_id::text             as session_id,
  action::text                 as action,
  metadata::jsonb              as metadata,
  (event_time at time zone 'UTC')::timestamptz as ts

from public.raw_events
