"""
GamePulse Analytics Platform
Simulates player events for 50,000 users across ~200 days.

Output:
  - Local JSON: data/raw/events/date=YYYY-MM-DD/events_YYYYMMDD.json  (for debugging)
  - S3 Parquet:  s3://gamepulse-raw/events/date=YYYY-MM-DD/events_YYYYMMDD.parquet

Run:
  python3 data_generator/generate_events.py

  Optional flags:
    --start 2026-01-01     override start date
    --end   2026-05-27     override end date
    --users 10000          override user count (for quick test runs)
    --local-only           skip S3 upload (useful when testing without AWS)
    --date  2026-05-27     generate a single day only (for daily incremental runs)
"""

import argparse
import json
import os
import io
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from faker import Faker

load_dotenv()
fake = Faker()
Faker.seed(42)
random.seed(42)


DEFAULT_START        = date(2025, 11, 1)
DEFAULT_END          = date(2026, 5, 29)
DEFAULT_USER_COUNT   = 50_000
S3_BUCKET            = os.getenv("S3_BUCKET_NAME", "gamepulse-raw")
S3_PREFIX            = "events"
LOCAL_OUTPUT_DIR     = Path("data/raw/events")

SEGMENTS = {
    "casual":   0.50,
    "mid-core": 0.30,
    "whale":    0.10,
    "churned":  0.10,
}

PLATFORMS    = ["iOS", "Android", "web"]
DEVICE_TYPES = ["mobile", "tablet", "desktop"]
COUNTRIES    = ["DE", "US", "GB", "FR", "IN", "BR", "JP", "CA", "AU", "MX"]
AD_NETWORKS  = ["Meta Audience Network", "AdMob", "Unity Ads"]
AD_FORMATS   = ["rewarded_video", "interstitial", "banner"]
POWERUP_NAMES = ["Bomb Bubble", "Rainbow Bubble", "Fireball", "Line Blaster", "Color Splash"]
PAYMENT_METHODS = ["apple_pay", "google_pay", "credit_card"]
COUNTRY_CURRENCY_MAP = {
    "DE": ("EUR", 0.92),
    "FR": ("EUR", 0.92),
    "US": ("USD", 1.00),
    "GB": ("GBP", 0.79),
    "IN": ("INR", 83.50),
    "BR": ("BRL", 5.05),
    "JP": ("JPY", 149.50),
    "CA": ("CAD", 1.36),
    "AU": ("AUD", 1.53),
    "MX": ("MXN", 17.15),
}
PRODUCT_CATALOG = [
    {"product_id": "coin_pack_100",  "product_name": "100 Coin Bundle",   "product_category": "currency_pack",  "price_usd": 0.99},
    {"product_id": "coin_pack_500",  "product_name": "500 Coin Bundle",   "product_category": "currency_pack",  "price_usd": 3.99},
    {"product_id": "coin_pack_2000", "product_name": "2000 Coin Bundle",  "product_category": "currency_pack",  "price_usd": 9.99},
    {"product_id": "booster_pack_s", "product_name": "Starter Booster",   "product_category": "booster_pack",   "price_usd": 1.99},
    {"product_id": "booster_pack_m", "product_name": "Mega Booster Pack", "product_category": "booster_pack",   "price_usd": 4.99},
    {"product_id": "cosmetic_hat",   "product_name": "Party Hat Skin",    "product_category": "cosmetic",       "price_usd": 2.49},
    {"product_id": "subscription_w", "product_name": "Weekly Pass",       "product_category": "subscription",   "price_usd": 1.49},
    {"product_id": "subscription_m", "product_name": "Monthly Pass",      "product_category": "subscription",   "price_usd": 4.99},
]


# USER POPULATION BUILDER
def build_user_population(user_count: int, start_date: date) -> list[dict]:
    """
    Creates a stable population of users with fixed attributes.
    Called once and reused across all days so user_id, segment,
    install_date etc. stay consistent throughout the dataset.
    """
    print(f"Building population of {user_count:,} users...")
    users = []
    segment_names = list(SEGMENTS.keys())
    segment_weights = list(SEGMENTS.values())

    for _ in range(user_count):
        segment = random.choices(segment_names, weights=segment_weights, k=1)[0]
        install_offset = random.randint(0, 30)
        install_date = start_date - timedelta(days=install_offset)

        is_payer = segment == "whale" or (segment == "mid-core" and random.random() < 0.25)
        lifetime_spend = round(random.uniform(5, 300), 2) if segment == "whale" \
            else round(random.uniform(0, 30), 2) if is_payer \
            else 0.0

        users.append({
            "user_id":            str(uuid.uuid4()),
            "segment_id":         segment,
            "platform":           random.choice(PLATFORMS),
            "device_type":        random.choice(DEVICE_TYPES),
            "country_code":       random.choices(COUNTRIES, weights=[25,20,10,8,8,7,6,6,5,5], k=1)[0],
            "app_version":        random.choice(["3.1.0", "3.2.0", "3.2.1", "3.3.0"]),
            "os_version":         fake.random_element(["iOS 16.6", "iOS 17.2", "Android 13", "Android 14"]),
            "install_date":       install_date.isoformat(),
            "is_payer":           is_payer,
            "lifetime_spend_usd": lifetime_spend,
            "ab_test_group":      random.choice(["control", "treatment"]),
            "lifetime_purchases": random.randint(1, 20) if is_payer else 0,
            "currency_balance":   random.randint(0, 5000),
            "player_level":       random.randint(1, 100),
            "session_count":      0,
        })

    print(f"Population built: {sum(1 for u in users if u['segment_id']=='whale'):,} whales, "
          f"{sum(1 for u in users if u['segment_id']=='mid-core'):,} mid-core, "
          f"{sum(1 for u in users if u['segment_id']=='casual'):,} casual, "
          f"{sum(1 for u in users if u['segment_id']=='churned'):,} churned")
    return users


# SHARED IDENTITY FIELDS
def build_base_event(user: dict, event_type: str, session_id: str, event_ts: datetime) -> dict:
    """
    Returns the 13 shared identity fields present on every event.
    Every specific event builder calls this first, then merges on top.
    """
    return {
        "event_id":            str(uuid.uuid4()),
        "event_type":          event_type,
        "event_timestamp":     event_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "ingestion_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "session_id":          session_id,
        "user_id":             user["user_id"],
        "device_type":         user["device_type"],
        "platform":            user["platform"],
        "country_code":        user["country_code"],
        "app_version":         user["app_version"],
        "os_version":          user["os_version"],
        "segment_id":          user["segment_id"],
        "ab_test_group":       user["ab_test_group"],
    }


# EVENT BUILDERS
def build_session_start(user: dict, session_id: str, event_ts: datetime, install_date: date) -> dict:
    days_since_install = (event_ts.date() - install_date).days
    base = build_base_event(user, "session_start", session_id, event_ts)
    base.update({
        "session_number":           user["session_count"] + 1,
        "days_since_install":       max(days_since_install, 0),
        "days_since_last_session":  random.randint(0, 7) if user["session_count"] > 0 else 0,
        "acquisition_source":       random.choices(
                                        ["organic", "paid_social", "referral"],
                                        weights=[50, 35, 15], k=1)[0],
        "acquisition_campaign_id":  f"camp_{random.randint(100, 999)}",
        "player_level":             user["player_level"],
        "in_game_currency_balance": user["currency_balance"],
        "lifetime_spend_usd":       user["lifetime_spend_usd"],
        "is_payer":                 user["is_payer"],
        "install_date":             user["install_date"],
        "connection_type":          random.choices(["wifi", "cellular"], weights=[70, 30], k=1)[0],
    })
    return base


def build_level_complete(user: dict, session_id: str, event_ts: datetime) -> dict:
    level_num = user["player_level"] + random.randint(0, 2)
    attempt   = random.choices([1, 2, 3, 4, 5], weights=[40, 25, 18, 10, 7], k=1)[0]
    boosters  = random.sample(POWERUP_NAMES, k=random.randint(0, 2))
    coins     = random.randint(10, 200)

    base = build_base_event(user, "level_complete", session_id, event_ts)
    base.update({
        "level_id":                  f"world_{level_num // 10 + 1}_level_{level_num}",
        "level_number":              level_num,
        "attempt_number":            attempt,
        "time_to_complete_seconds":  random.randint(30, 600),
        "score":                     random.randint(1000, 99999),
        "stars_earned":              random.choices([1, 2, 3], weights=[20, 45, 35], k=1)[0],
        "boosters_used":             boosters,
        "boosters_purchased_mid_level": random.random() < 0.08,
        "coins_earned":              coins,
        "xp_earned":                 random.randint(50, 500),
        "player_level_before":       user["player_level"],
        "player_level_after":        user["player_level"] + (1 if random.random() < 0.3 else 0),
        "difficulty_modifier":       random.choices(["easy", "normal", "hard"],
                                                    weights=[20, 60, 20], k=1)[0],
    })
    return base


def build_purchase_made(user: dict, session_id: str, event_ts: datetime) -> dict:
    product  = random.choice(PRODUCT_CATALOG)
    discount = random.random() < 0.20
    discount_pct = round(random.uniform(0.10, 0.40), 2) if discount else 0.0
    final_price  = round(product["price_usd"] * (1 - discount_pct), 2)
    currency_before = user["currency_balance"]
    reward_coins    = int(final_price * 100)
    currency_code, fx_rate = COUNTRY_CURRENCY_MAP.get(
        user["country_code"], ("USD", 1.00)
    )

    base = build_base_event(user, "purchase_made", session_id, event_ts)
    base.update({
        "transaction_id":           str(uuid.uuid4()),
        "product_id":               product["product_id"],
        "product_name":             product["product_name"],
        "product_category":         product["product_category"],
        "price_usd":                final_price,
        "price_local_currency":     round(final_price * fx_rate, 2),
        "local_currency_code":      currency_code,
        "payment_method":           random.choice(PAYMENT_METHODS),
        "is_first_purchase":        user["lifetime_purchases"] == 0,
        "purchase_context":         random.choices(
                                        ["level_fail_prompt", "shop", "ad_reward_followup"],
                                        weights=[40, 45, 15], k=1)[0],
        "discount_applied":         discount,
        "discount_percentage":      discount_pct,
        "promo_id":                 f"promo_{random.randint(10, 99)}" if discount else None,
        "in_game_currency_before":  currency_before,
        "in_game_currency_after":   currency_before + reward_coins,
        "lifetime_purchase_count":  user["lifetime_purchases"] + 1,
    })
    return base


def build_powerup_used(user: dict, session_id: str, event_ts: datetime) -> dict:
    powerup_name   = random.choice(POWERUP_NAMES)
    powerup_source = random.choices(
        ["purchased", "earned", "gifted", "ad_reward"],
        weights=[30, 40, 10, 20], k=1)[0]
    inv_before = random.randint(1, 10)
    level_num  = user["player_level"] + random.randint(0, 2)

    base = build_base_event(user, "powerup_used", session_id, event_ts)
    base.update({
        "powerup_id":           powerup_name.lower().replace(" ", "_"),
        "powerup_name":         powerup_name,
        "powerup_source":       powerup_source,
        "powerup_cost_coins":   random.randint(10, 100) if powerup_source == "purchased" else 0,
        "level_id":             f"world_{level_num // 10 + 1}_level_{level_num}",
        "level_attempt_number": random.randint(1, 5),
        "used_at_seconds":      random.randint(5, 300),
        "outcome_after_use":    random.choices(
                                    ["level_complete", "level_fail", "still_playing"],
                                    weights=[50, 20, 30], k=1)[0],
        "inventory_before":     inv_before,
        "inventory_after":      max(inv_before - 1, 0),
        "is_ab_test_powerup":   user["ab_test_group"] == "treatment" and random.random() < 0.3,
    })
    return base


def build_ad_watched(user: dict, session_id: str, event_ts: datetime) -> dict:
    ad_format     = random.choice(AD_FORMATS)
    duration      = random.randint(15, 30)
    completed     = random.random() < 0.75
    watch_dur     = duration if completed else random.randint(5, duration - 1)
    reward_type   = random.choices(["coins", "extra_lives", "powerup"], weights=[60, 25, 15], k=1)[0]
    reward_val    = random.randint(10, 100) if reward_type == "coins" \
                    else random.randint(1, 3) if reward_type == "extra_lives" \
                    else 1
    currency_before = user["currency_balance"]

    base = build_base_event(user, "ad_watched", session_id, event_ts)
    base.update({
        "ad_id":               f"ad_{str(uuid.uuid4())[:8]}",
        "ad_network":          random.choice(AD_NETWORKS),
        "ad_format":           ad_format,
        "ad_duration_seconds": duration,
        "watch_duration_seconds": watch_dur,
        "completed":           completed,
        "reward_granted":      completed,
        "reward_type":         reward_type if completed else None,
        "reward_value":        reward_val if completed else 0,
        "placement_context":   random.choices(
                                   ["level_end", "home_screen", "shop_prompt"],
                                   weights=[50, 30, 20], k=1)[0],
        "is_opted_in":         ad_format == "rewarded_video",
        "revenue_usd":         round(random.uniform(0.001, 0.05), 4),
        "currency_before":     currency_before,
        "currency_after":      currency_before + reward_val if completed else currency_before,
    })
    return base


# DAILY SESSION SIMULATOR
def should_user_play(user: dict, current_date: date) -> bool:
    """
    Decides if a user plays on a given day based on their segment.
    Churned users have very low probability after day 14.
    """
    install_date = date.fromisoformat(user["install_date"])
    days_since   = (current_date - install_date).days

    if days_since < 0:
        return False

    probs = {
        "whale":    0.90,
        "mid-core": 0.55,
        "casual":   0.20,
        "churned":  0.02 if days_since > 14 else 0.40,
    }
    return random.random() < probs[user["segment_id"]]


def simulate_day(users: list[dict], current_date: date) -> list[dict]:
    """
    Runs one day of simulation. For each active user, generates
    1-3 sessions with realistic event sequences inside each session.
    Returns a flat list of all events for that day.
    """
    events     = []
    active     = [u for u in users if should_user_play(u, current_date)]
    install_d  = date.fromisoformat

    for user in active:
        install_date  = install_d(user["install_date"])
        num_sessions  = random.choices(
            [1, 2, 3],
            weights=[60, 30, 10] if user["segment_id"] in ("casual", "churned") else [30, 45, 25],
            k=1)[0]

        day_start_hour = random.randint(7, 22)

        for s in range(num_sessions):
            session_id    = str(uuid.uuid4())
            session_hour  = min(day_start_hour + s * random.randint(1, 3), 23)
            session_start = datetime(
                current_date.year, current_date.month, current_date.day,
                session_hour, random.randint(0, 59), random.randint(0, 59)
            )
            cursor = session_start

            # 1. session_start is always first
            events.append(build_session_start(user, session_id, cursor, install_date))
            user["session_count"] += 1
            cursor += timedelta(seconds=random.randint(2, 10))

            # 2. Level completions (1-5 per session based on segment)
            num_levels = random.choices(
                [1, 2, 3, 4, 5],
                weights=[10, 25, 35, 20, 10] if user["segment_id"] == "whale" else [30, 35, 20, 10, 5],
                k=1)[0]

            for _ in range(num_levels):
                events.append(build_level_complete(user, session_id, cursor))
                cursor += timedelta(seconds=random.randint(60, 600))

                # Ad after level (60% chance)
                if random.random() < 0.60:
                    events.append(build_ad_watched(user, session_id, cursor))
                    cursor += timedelta(seconds=random.randint(15, 35))
                    # Update currency balance if ad reward given
                    user["currency_balance"] = max(0, user["currency_balance"] + random.randint(0, 50))

                # Powerup used (40% chance per level)
                if random.random() < 0.40:
                    events.append(build_powerup_used(user, session_id, cursor))
                    cursor += timedelta(seconds=random.randint(5, 30))

            # 3. Purchase (probability depends on segment and ab_test_group)
            purchase_prob = {
                "whale":    0.60,
                "mid-core": 0.10,
                "casual":   0.02,
                "churned":  0.005,
            }[user["segment_id"]]

            # Treatment group has a 20% uplift in purchase probability
            if user["ab_test_group"] == "treatment":
                purchase_prob *= 1.20

            if random.random() < purchase_prob:
                events.append(build_purchase_made(user, session_id, cursor))
                user["lifetime_purchases"] += 1
                user["is_payer"] = True
                user["lifetime_spend_usd"] = round(
                    user["lifetime_spend_usd"] + random.choice(PRODUCT_CATALOG)["price_usd"], 2)
                cursor += timedelta(seconds=random.randint(5, 30))

            # Update player level occasionally
            if random.random() < 0.15:
                user["player_level"] = min(user["player_level"] + 1, 500)

    return events


# WRITE FUNCTIONS
def write_local_json(events: list[dict], current_date: date) -> Path:
    """Writes events as a JSON file locally for debugging."""
    date_str  = current_date.strftime("%Y%m%d")
    out_dir   = LOCAL_OUTPUT_DIR / f"date={current_date.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path  = out_dir / f"events_{date_str}.json"

    with open(out_path, "w") as f:
        json.dump(events, f, indent=2, default=str)

    return out_path


def write_s3_parquet(events: list[dict], current_date: date, s3_client) -> str:
    """Converts events to a Parquet file and uploads to S3."""
    date_str  = current_date.strftime("%Y%m%d")
    s3_key    = f"{S3_PREFIX}/date={current_date.isoformat()}/events_{date_str}.parquet"

    df    = pd.DataFrame(events)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Write Parquet to an in-memory buffer then upload to S3
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )

    return f"s3://{S3_BUCKET}/{s3_key}"


def parse_args():
    parser = argparse.ArgumentParser(description="GamePulse event generator")
    parser.add_argument("--start",      type=str,  default=None,  help="Start date YYYY-MM-DD")
    parser.add_argument("--end",        type=str,  default=None,  help="End date YYYY-MM-DD")
    parser.add_argument("--users",      type=int,  default=DEFAULT_USER_COUNT)
    parser.add_argument("--local-only", action="store_true", help="Skip S3 upload")
    parser.add_argument("--date",       type=str,  default=None,  help="Generate single day YYYY-MM-DD")
    return parser.parse_args()


def main():
    args = parse_args()

    # Single-day mode (for daily incremental runs)
    if args.date:
        start_date = date.fromisoformat(args.date)
        end_date   = start_date
    else:
        start_date = date.fromisoformat(args.start) if args.start else DEFAULT_START
        end_date   = date.fromisoformat(args.end)   if args.end   else DEFAULT_END

    user_count  = args.users
    local_only  = args.local_only

    print(f"\nGamePulse Event Generator")
    print(f"  Date range : {start_date} to {end_date}")
    print(f"  Users      : {user_count:,}")
    print(f"  S3 bucket  : {S3_BUCKET if not local_only else 'skipped (local-only mode)'}")
    print(f"  Local JSON : {LOCAL_OUTPUT_DIR}\n")

    # Build S3 client
    s3_client = None
    if not local_only:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "eu-central-1"),
        )
        # Verify bucket is reachable before starting
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET)
            print(f"S3 connection verified: s3://{S3_BUCKET}\n")
        except Exception as e:
            print(f"ERROR: Cannot reach S3 bucket '{S3_BUCKET}': {e}")
            print("Tip: run with --local-only to skip S3 and generate locally only.")
            return

    # Build user population once, reuse across all days
    users = build_user_population(user_count, start_date)

    # Generate day by day
    current_date = start_date
    total_events = 0
    days_processed = 0

    while current_date <= end_date:
        events = simulate_day(users, current_date)
        count  = len(events)

        if count == 0:
            current_date += timedelta(days=1)
            continue

        # Write local JSON (always)
        local_path = write_local_json(events, current_date)

        # Write Parquet to S3 (unless local-only)
        s3_path = None
        if not local_only and s3_client:
            try:
                s3_path = write_s3_parquet(events, current_date, s3_client)
            except Exception as e:
                print(f"  WARNING: S3 upload failed for {current_date}: {e}")

        total_events   += count
        days_processed += 1

        if days_processed % 10 == 0 or current_date == end_date:
            print(f"  [{current_date}] {count:>7,} events | local: {local_path.name}"
                  + (f" | s3: {s3_path}" if s3_path else ""))

        current_date += timedelta(days=1)

    print(f"\nDone.")
    print(f"  Days processed : {days_processed}")
    print(f"  Total events   : {total_events:,}")
    print(f"  Avg per day    : {total_events // max(days_processed, 1):,}")


if __name__ == "__main__":
    main()