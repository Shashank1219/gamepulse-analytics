-- Fails if any session spans more than 24 hours.
-- Uses window functions to calculate min and max event timestamp
-- per session_id and flags sessions exceeding the threshold.
-- A session lasting over 24 hours indicates UUID collision or
-- incorrect session assignment in the event generator.

with session_spans as (
    select
        session_id,
        user_id,
        min(event_timestamp)                        as session_start,
        max(event_timestamp)                        as session_end,
        datediff(second,
            min(event_timestamp),
            max(event_timestamp))                   as duration_seconds,
        count(distinct event_type)                  as distinct_event_types,
        count(event_id)                             as total_events
    from {{ ref('stg_game_events') }}
    group by session_id, user_id
)

select
    session_id,
    user_id,
    session_start,
    session_end,
    duration_seconds,
    round(duration_seconds / 3600.0, 2)             as duration_hours,
    total_events
from session_spans
where duration_seconds > 86400