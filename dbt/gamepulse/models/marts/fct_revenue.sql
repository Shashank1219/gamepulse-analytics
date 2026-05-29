with sessions as (
    select * from {{ ref('int_sessions') }}
),

purchases as (
    select
        event_date,
        user_id,
        count(event_id)                     as purchase_count,
        sum(price_usd)                      as revenue_usd
    from {{ ref('stg_game_events') }}
    where event_type = 'purchase_made'
    group by event_date, user_id
),

daily_revenue as (
    select
        event_date                          as date,
        count(distinct user_id)             as paying_users,
        sum(purchase_count)                 as transaction_count,
        round(sum(revenue_usd), 2)          as total_revenue_usd,
        round(sum(revenue_usd) /
              nullif(count(distinct user_id), 0), 4) as arppu,
        count(distinct case when purchase_count = 1
                            and revenue_usd > 0
                            then user_id end) as new_payers
    from purchases
    group by event_date
),

dau as (
    select
        date,
        sum(dau)                            as total_dau
    from {{ ref('fct_daily_active_users') }}
    group by date
)

select
    r.date,
    r.paying_users,
    r.transaction_count,
    r.total_revenue_usd,
    r.arppu,
    r.new_payers,
    round(r.total_revenue_usd /
          nullif(d.total_dau, 0), 4)        as arpu
from daily_revenue r
left join dau d on r.date = d.date
order by date desc