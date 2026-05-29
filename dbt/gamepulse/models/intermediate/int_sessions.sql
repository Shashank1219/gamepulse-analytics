with events as (
    select * from {{ ref('stg_game_events') }}
    where event_type in ('session_start', 'level_complete', 'purchase_made',
                         'powerup_used', 'ad_watched')
),

session_metrics as (
    select
        session_id,
        user_id,
        segment_id,
        ab_test_group,
        country_code,
        platform,
        device_type,

        -- Deriving event_date from session start, not from individual events
        -- This prevents midnight-spanning sessions from creating duplicate rows
        to_date(min(event_timestamp))               as event_date,

        min(event_timestamp)                        as session_start_time,
        max(event_timestamp)                        as session_end_time,
        datediff(second,
            min(event_timestamp),
            max(event_timestamp))                   as session_duration_seconds,
        count(event_id)                             as total_events,
        count(case when event_type = 'level_complete'
                   then event_id end)               as levels_completed,
        count(case when event_type = 'purchase_made'
                   then event_id end)               as purchases_in_session,
        coalesce(sum(case when event_type = 'purchase_made'
                          then price_usd end), 0)   as session_revenue_usd,
        coalesce(sum(case when event_type = 'ad_watched'
                          then revenue_usd end), 0) as ad_revenue_usd,
        count(case when event_type = 'ad_watched'
                   then event_id end)               as ads_watched,
        case when count(case when event_type = 'purchase_made'
                             then event_id end) > 0
             then true else false end               as had_purchase

    from events
    group by
        session_id, user_id, segment_id, ab_test_group,
        country_code, platform, device_type
)

select * from session_metrics