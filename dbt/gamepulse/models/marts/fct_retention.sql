with first_sessions as (
    select
        user_id,
        min(event_date)                     as first_seen_date,
        date_trunc('week', min(event_date)) as cohort_week
    from {{ ref('int_sessions') }}
    group by user_id
),

all_sessions as (
    select distinct
        user_id,
        event_date
    from {{ ref('int_sessions') }}
),

joined as (
    select
        f.user_id,
        f.cohort_week,
        f.first_seen_date,
        a.event_date,
        datediff(day, f.first_seen_date, a.event_date) as days_since_install
    from first_sessions f
    inner join all_sessions a on f.user_id = a.user_id
),

retention as (
    select
        cohort_week,
        days_since_install,
        count(distinct user_id)             as retained_users
    from joined
    where days_since_install in (0, 1, 7, 14, 30)
    group by cohort_week, days_since_install
),

cohort_sizes as (
    select
        cohort_week,
        count(distinct user_id)             as cohort_size
    from first_sessions
    group by cohort_week
)

select
    r.cohort_week,
    r.days_since_install,
    c.cohort_size,
    r.retained_users,
    round(r.retained_users / c.cohort_size * 100, 2) as retention_rate_pct
from retention r
inner join cohort_sizes c on r.cohort_week = c.cohort_week
order by cohort_week, days_since_install