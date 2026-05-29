with source as (
    select * from raw.game_events
),

renamed as (
    select
        -- identity fields
        event_id,
        event_type,
        cast(event_timestamp as timestamp)   as event_timestamp,
        cast(ingestion_timestamp as timestamp) as ingestion_timestamp,
        event_date,
        session_id,
        user_id,
        device_type,
        platform,
        country_code,
        app_version,
        os_version,
        segment_id,
        ab_test_group,

        -- session_start fields
        session_number,
        days_since_install,
        days_since_last_session,
        acquisition_source,
        acquisition_campaign_id,
        player_level,
        in_game_currency_balance,
        cast(lifetime_spend_usd as double)   as lifetime_spend_usd,
        is_payer,
        cast(install_date as date)           as install_date,
        connection_type,

        -- level_complete fields
        level_id,
        level_number,
        attempt_number,
        time_to_complete_seconds,
        score,
        stars_earned,
        boosters_used,
        boosters_purchased_mid_level,
        coins_earned,
        xp_earned,
        player_level_before,
        player_level_after,
        difficulty_modifier,

        -- purchase_made fields
        transaction_id,
        product_id,
        product_name,
        product_category,
        cast(price_usd as double)            as price_usd,
        cast(price_local_currency as double) as price_local_currency,
        local_currency_code,
        payment_method,
        is_first_purchase,
        purchase_context,
        discount_applied,
        cast(discount_percentage as double)  as discount_percentage,
        promo_id,
        in_game_currency_before,
        in_game_currency_after,
        lifetime_purchase_count,

        -- powerup_used fields
        powerup_id,
        powerup_name,
        powerup_source,
        powerup_cost_coins,
        level_attempt_number,
        used_at_seconds,
        outcome_after_use,
        inventory_before,
        inventory_after,
        is_ab_test_powerup,

        -- ad_watched fields
        ad_id,
        ad_network,
        ad_format,
        ad_duration_seconds,
        watch_duration_seconds,
        completed,
        reward_granted,
        reward_type,
        reward_value,
        placement_context,
        is_opted_in,
        cast(revenue_usd as double)          as revenue_usd,
        currency_before,
        currency_after

    from source
    where event_id is not null
      and user_id is not null
      and session_id is not null
      and event_timestamp is not null
)

select * from renamed