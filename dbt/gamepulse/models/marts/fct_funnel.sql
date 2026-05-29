with levels as (
    select
        level_number,
        level_id,
        count(event_id)                     as total_attempts,
        count(distinct user_id)             as unique_players,
        round(avg(attempt_number), 2)       as avg_attempts_to_complete,
        round(avg(time_to_complete_seconds), 2) as avg_time_seconds,
        round(avg(stars_earned), 2)         as avg_stars,
        sum(case when boosters_purchased_mid_level
                 then 1 else 0 end)         as mid_level_purchases,
        round(sum(case when boosters_purchased_mid_level
                       then 1 else 0 end) /
              nullif(count(event_id), 0) * 100, 2) as booster_purchase_rate_pct
    from {{ ref('stg_game_events') }}
    where event_type = 'level_complete'
    group by level_number, level_id
)

select * from levels
order by level_number