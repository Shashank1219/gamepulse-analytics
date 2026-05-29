with assignments as (
    select *
    from {{ ref('int_experiment_assignments') }}
    where has_leakage = false
),

purchases as (
    select
        user_id,
        count(event_id)                     as purchase_count,
        sum(price_usd)                      as total_spend_usd
    from {{ ref('stg_game_events') }}
    where event_type = 'purchase_made'
    group by user_id
),

joined as (
    select
        a.ab_test_group,
        a.user_id,
        a.has_leakage,
        coalesce(p.purchase_count, 0)       as purchase_count,
        coalesce(p.total_spend_usd, 0)      as total_spend_usd,
        case when p.purchase_count > 0
             then true else false end       as converted
    from assignments a
    left join purchases p on a.user_id = p.user_id
),

results as (
    select
        ab_test_group,
        count(distinct user_id)             as total_users,
        count(distinct case when converted
                            then user_id end) as converted_users,
        round(count(distinct case when converted
                                  then user_id end) /
              nullif(count(distinct user_id), 0) * 100, 4) as conversion_rate_pct,
        round(sum(total_spend_usd) /
              nullif(count(distinct user_id), 0), 4) as avg_revenue_per_user,
        round(sum(total_spend_usd), 2)      as total_revenue_usd
    from joined
    group by ab_test_group
),

leakage_summary as (
    select count(distinct user_id) as leakage_users
    from {{ ref('int_experiment_assignments') }}
    where has_leakage = true
)

select
    r.*,
    l.leakage_users
from results r
cross join leakage_summary l