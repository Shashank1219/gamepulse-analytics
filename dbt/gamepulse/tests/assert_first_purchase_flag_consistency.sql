-- Fails if is_first_purchase is inconsistent with purchase order.
-- Two failure modes are detected:
--   1. A user has is_first_purchase = true on more than one purchase
--   2. A user's chronologically first purchase does not have the flag set
-- Uses ROW_NUMBER() partitioned by user_id ordered by event_timestamp
-- to identify the true first purchase and compare against the flag.

with ranked_purchases as (
    select
        event_id,
        user_id,
        transaction_id,
        event_timestamp,
        is_first_purchase,
        row_number() over (
            partition by user_id
            order by event_timestamp asc
        )                                           as purchase_rank,
        count(*) over (
            partition by user_id
        )                                           as total_purchases,
        sum(case when is_first_purchase then 1 else 0 end) over (
            partition by user_id
        )                                           as first_purchase_flag_count
    from {{ ref('stg_game_events') }}
    where event_type = 'purchase_made'
),

violations as (
    select
        event_id,
        user_id,
        transaction_id,
        event_timestamp,
        is_first_purchase,
        purchase_rank,
        total_purchases,
        first_purchase_flag_count,
        case
            when first_purchase_flag_count > 1
                then 'multiple_first_purchase_flags'
            when purchase_rank = 1 and is_first_purchase = false
                then 'first_purchase_not_flagged'
        end                                         as violation_type
    from ranked_purchases
    where first_purchase_flag_count > 1
       or (purchase_rank = 1 and is_first_purchase = false)
)

select * from violations