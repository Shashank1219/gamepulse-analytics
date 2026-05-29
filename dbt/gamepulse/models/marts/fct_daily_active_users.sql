with sessions as (
    select * from {{ ref('int_sessions') }}
),

daily as (
    select
        event_date                          as date,
        country_code,
        count(distinct user_id)             as dau,
        count(distinct session_id)          as total_sessions,
        count(distinct case when had_purchase then user_id end) as paid_users,
        round(avg(session_duration_seconds), 2) as avg_session_duration_seconds,
        sum(session_revenue_usd)            as total_revenue_usd,
        sum(ad_revenue_usd)                 as total_ad_revenue_usd
    from sessions
    group by event_date, country_code
)

select * from daily
order by date desc, country_code