with base as (
    select
        user_id,
        ab_test_group,
        segment_id,
        min(event_date) as first_seen_in_experiment
    from {{ ref('stg_game_events') }}
    where ab_test_group is not null
    group by user_id, ab_test_group, segment_id
),

-- Flag users who appear in both control and treatment (variant leakage)
leakage_check as (
    select
        user_id,
        count(distinct ab_test_group) as group_count
    from base
    group by user_id
),

final as (
    select
        b.user_id,
        b.ab_test_group,
        b.segment_id,
        b.first_seen_in_experiment,
        case when l.group_count > 1 then true else false end as has_leakage
    from base b
    left join leakage_check l on b.user_id = l.user_id
)

select * from final