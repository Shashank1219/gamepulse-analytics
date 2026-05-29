-- Fails if a user's country and local_currency_code are mismatched.
-- For example a DE user should always have EUR, not USD or GBP.

select
    transaction_id,
    country_code,
    local_currency_code
from {{ ref('stg_game_events') }}
where event_type = 'purchase_made'
  and not (
      (country_code = 'DE' and local_currency_code = 'EUR') or
      (country_code = 'FR' and local_currency_code = 'EUR') or
      (country_code = 'US' and local_currency_code = 'USD') or
      (country_code = 'GB' and local_currency_code = 'GBP') or
      (country_code = 'IN' and local_currency_code = 'INR') or
      (country_code = 'BR' and local_currency_code = 'BRL') or
      (country_code = 'JP' and local_currency_code = 'JPY') or
      (country_code = 'CA' and local_currency_code = 'CAD') or
      (country_code = 'AU' and local_currency_code = 'AUD') or
      (country_code = 'MX' and local_currency_code = 'MXN')
  )