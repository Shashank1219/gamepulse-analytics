with sessions as (
    select * from {{ ref('int_sessions') }}
),

session_agg as (
    select
        user_id,
        segment_id,
        ab_test_group,
        country_code,
        platform,
        device_type,
        min(event_date)                     as first_seen_date,
        max(event_date)                     as last_seen_date,
        count(distinct session_id)          as total_sessions,
        sum(session_duration_seconds)       as total_time_seconds,
        sum(session_revenue_usd)            as lifetime_spend_usd,
        max(had_purchase::int)              as is_payer
    from sessions
    group by user_id, segment_id, ab_test_group,
             country_code, platform, device_type
),

first_purchase as (
    select
        user_id,
        min(event_date)                     as first_purchase_date
    from {{ ref('stg_game_events') }}
    where event_type = 'purchase_made'
    group by user_id
),

install_dates as (
    select
        user_id,
        install_date,
        acquisition_source,
        acquisition_campaign_id
    from {{ ref('stg_game_events') }}
    where event_type = 'session_start'
      and install_date is not null
    qualify row_number() over (partition by user_id order by event_timestamp) = 1
)

select
    s.user_id,
    s.segment_id,
    s.ab_test_group,
    s.country_code,
    s.platform,
    s.device_type,
    i.install_date,
    i.acquisition_source,
    i.acquisition_campaign_id,
    s.first_seen_date,
    s.last_seen_date,
    s.total_sessions,
    s.total_time_seconds,
    round(s.lifetime_spend_usd, 2)          as lifetime_spend_usd,
    case when s.is_payer = 1
         then true else false end           as is_payer,
    p.first_purchase_date
from session_agg s
left join first_purchase p on s.user_id = p.user_id
left join install_dates i on s.user_id = i.user_id