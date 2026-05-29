-- Fails if any session occurs before the user's install date.
-- Joins session events back to the earliest install_date recorded
-- for that user across all session_start events.
-- An event_date before install_date is a logical impossibility
-- and indicates a data generation or join error.

with user_install_dates as (
    select
        user_id,
        min(install_date)                           as install_date
    from {{ ref('stg_game_events') }}
    where event_type = 'session_start'
      and install_date is not null
    group by user_id
),

session_dates as (
    select
        session_id,
        user_id,
        event_date,
        event_type,
        min(event_date) over (
            partition by session_id
        )                                           as earliest_event_in_session
    from {{ ref('stg_game_events') }}
),

violations as (
    select
        s.session_id,
        s.user_id,
        s.earliest_event_in_session             as session_date,
        u.install_date,
        datediff(day,
            s.earliest_event_in_session,
            u.install_date)                         as days_before_install
    from session_dates s
    inner join user_install_dates u
        on s.user_id = u.user_id
    where s.earliest_event_in_session < u.install_date
)

select distinct
    session_id,
    user_id,
    session_date,
    install_date,
    days_before_install
from violations