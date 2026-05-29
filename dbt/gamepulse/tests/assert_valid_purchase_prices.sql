-- Fails if any purchase has a zero or negative price in any currency.
-- A discount can reduce price but never to zero or below.

select
    transaction_id,
    price_local_currency,
    local_currency_code,
    discount_percentage
from {{ ref('stg_game_events') }}
where event_type = 'purchase_made'
and price_local_currency <= 0