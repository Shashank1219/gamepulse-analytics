# Databricks notebook source
# MAGIC %md
# MAGIC <h3>GamePulse: Raw Ingestion</h3>
# MAGIC <br/>Reads daily Parquet files from S3, validates schema,<br/>deduplicates on event_id, and writes to raw.game_events Delta table.
# MAGIC
# MAGIC Parameters:
# MAGIC <br/>`run_date`: the partition date to ingest (YYYY-MM-DD)
# MAGIC <br/>`mode`: `backfill` to process all partitions, `daily` for single date

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>AWS Config</h5>

# COMMAND ----------

# Config
S3_BUCKET   = "gamepulse-raw"
S3_PREFIX   = "events"
DELTA_TABLE = "raw.game_events"

# S3 access is handled via Unity Catalog external location:
# 'db_s3_external_databricks-s3-ingest-25def' -> s3://gamepulse-raw/
# No credentials needed in code. Databricks assumes the IAM role at runtime.

S3_PATH = f"s3://{S3_BUCKET}/{S3_PREFIX}/"

print(f"External location : db_s3_external_databricks-s3-ingest-25def")
print(f"S3 path           : {S3_PATH}")
print(f"Target Delta table: {DELTA_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Schema Definition</h5>

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS raw")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Read Parquet files from S3</h5>

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(S3_PATH)
)
 
print(f"Raw records read from S3 : {raw_df.count():,}")
print(f"Columns inferred         : {len(raw_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC Cast columns to correct types

# COMMAND ----------

integer_cols = [
    "session_number", "days_since_install", "days_since_last_session",
    "player_level", "in_game_currency_balance",                 
    "level_number", "attempt_number", "time_to_complete_seconds",
    "score", "stars_earned", "coins_earned", "xp_earned",
    "player_level_before", "player_level_after",
    "in_game_currency_before", "in_game_currency_after",
    "lifetime_purchase_count", "powerup_cost_coins",
    "level_attempt_number", "used_at_seconds",
    "inventory_before", "inventory_after",
    "ad_duration_seconds", "watch_duration_seconds",
    "reward_value", "currency_before", "currency_after",
]
 
float_cols = [
    "lifetime_spend_usd", "price_usd", "price_local_currency",
    "discount_percentage", "revenue_usd",
]
 
boolean_cols = [
    "is_payer", "is_first_purchase", "discount_applied",
    "boosters_purchased_mid_level", "reward_granted",
    "completed", "is_opted_in", "is_ab_test_powerup",
]
 
typed_df = raw_df
 
for col in integer_cols:
    if col in typed_df.columns:
        typed_df = typed_df.withColumn(col, F.col(col).cast("integer"))
 
for col in float_cols:
    if col in typed_df.columns:
        typed_df = typed_df.withColumn(col, F.col(col).cast("float"))
 
for col in boolean_cols:
    if col in typed_df.columns:
        typed_df = typed_df.withColumn(col, F.col(col).cast("boolean"))
 
print("Type casting complete")
print(f"Total columns: {len(typed_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Validate</h5>

# COMMAND ----------

validated_df = typed_df.filter(
    F.col("event_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("session_id").isNotNull() &
    F.col("event_timestamp").isNotNull() &
    F.col("event_type").isin([
        "session_start", "level_complete",
        "purchase_made", "powerup_used", "ad_watched"
    ])
)
 
total_raw       = typed_df.count()
total_validated = validated_df.count()
dropped         = total_raw - total_validated
 
print(f"Total rows read          : {total_raw:,}")
print(f"Rows passing validation  : {total_validated:,}")
print(f"Rows dropped             : {dropped:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Deduplicate</h5>

# COMMAND ----------

from pyspark.sql.window import Window
 
window = Window.partitionBy("event_id").orderBy("ingestion_timestamp")
 
deduped_df = (
    validated_df
    .withColumn("row_num", F.row_number().over(window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
 
dedup_count = deduped_df.count()
dupes_removed = total_validated - dedup_count
 
print(f"Rows after dedup : {dedup_count:,}")
print(f"Duplicates removed: {dupes_removed:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Cast timestamps and add event_date partition column</h5>

# COMMAND ----------

final_df = (
    deduped_df
    .withColumn(
        "event_timestamp",
        F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    .withColumn(
        "ingestion_timestamp",
        F.to_timestamp("ingestion_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )
    # Add date column used for Delta partitioning
    .withColumn("event_date", F.to_date("event_timestamp"))
)
 
print(f"Final row count  : {final_df.count():,}")
print(f"Final column count: {len(final_df.columns)}")
print("\nSample schema:")
final_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Write to Delta table</h5>

# COMMAND ----------

(
    final_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("event_date", "event_type")
    .option("overwriteSchema", "true")
    .saveAsTable(DELTA_TABLE)
)
 
print(f"Successfully written to Delta table: {DELTA_TABLE}")
print(f"Partitioned by: event_date, event_type")

# COMMAND ----------

# MAGIC %md
# MAGIC <h5>Verify Delta table write</h5>

# COMMAND ----------

verify_df = spark.table(DELTA_TABLE)
 
print(f"Total rows in {DELTA_TABLE}: {verify_df.count():,}")
print()
 
print("Row count by event_type:")
display(
    verify_df
    .groupBy("event_type")
    .count()
    .orderBy("event_type")
)
 
print("\nDate range in table:")
display(
    verify_df.selectExpr(
        "min(event_date) as earliest_date",
        "max(event_date) as latest_date",
        "count(distinct event_date) as total_days"
    )
)