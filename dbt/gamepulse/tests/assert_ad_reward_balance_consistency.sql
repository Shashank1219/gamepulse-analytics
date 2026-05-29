-- Fails if a completed ad with a reward granted shows currency_after less than currency_before.

select
    event_id,
    user_id,
    currency_before,
    currency_after,
    reward_value,
    reward_granted
from {{ ref('stg_game_events') }}
where event_type = 'ad_watched'
  and completed = true
  and reward_granted = true
  and currency_after < currency_before