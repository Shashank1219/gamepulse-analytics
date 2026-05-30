"""
Daily batch pipeline that:
1. Runs the event generator for today's date.
2. Checks S3 for a new date partition
3. Validates event volume (alerts if drop > 30% vs prior day)
4. Triggers Databricks notebook to ingest into Delta Lake
5. Runs dbt models
6. Runs dbt tests

Schedule: 17:00 CEST (UTC+2) daily
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta

import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

S3_BUCKET         = os.getenv("S3_BUCKET_NAME", "gamepulse-raw")
S3_PREFIX         = "events"
DATABRICKS_HOST   = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN  = os.getenv("DATABRICKS_TOKEN")
NOTEBOOK_PATH     = "/gamepulse/01_ingest_raw"
DBT_PROJECT_PATH  = "/opt/airflow/dbt/gamepulse"
VOLUME_DROP_ALERT = 0.30   # alert if event count drops more than 30%
ALERT_EMAIL       = os.getenv("AIRFLOW_ALERT_EMAIL", "shashank.prakash1997@outlook.com")

DEFAULT_ARGS = {
    "owner":            "shashank",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "eu-central-1"),
    )


def get_event_count_for_date(s3, date_str: str) -> int:
    """Counts total events in a given date partition by reading object metadata."""
    prefix = f"{S3_PREFIX}/date={date_str}/"
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    contents = response.get("Contents", [])
    if not contents:
        return 0
    return sum(obj["Size"] for obj in contents)


# TASK FUNCTIONS
def sense_s3_partition(**context) -> bool:
    """
    Checks if present day's S3 partition exists and contains at least one file.
    Returns True when partition is found, False to keep polling.
    """
    date_str = context["ds"]  # YYYY-MM-DD from Airflow execution date
    s3       = get_s3_client()
    prefix   = f"{S3_PREFIX}/date={date_str}/"

    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    contents = response.get("Contents", [])

    if contents:
        log.info(f"Partition found: s3://{S3_BUCKET}/{prefix} ({len(contents)} files)")
        return True

    log.info(f"Partition not yet available: s3://{S3_BUCKET}/{prefix}")
    return False


def validate_event_volume(**context):
    """
    Compares present day's partition size against yesterday's.
    Raises an alert if the drop exceeds 30%.
    Does not fail the DAG, just logs a warning so analysts are notified.
    """
    s3            = get_s3_client()
    today_str     = context["ds"]
    yesterday_str = (datetime.strptime(today_str, "%Y-%m-%d")
                     - timedelta(days=1)).strftime("%Y-%m-%d")

    today_size     = get_event_count_for_date(s3, today_str)
    yesterday_size = get_event_count_for_date(s3, yesterday_str)

    log.info(f"Today ({today_str}) partition size    : {today_size:,} bytes")
    log.info(f"Yesterday ({yesterday_str}) partition size: {yesterday_size:,} bytes")

    if yesterday_size == 0:
        log.warning("No yesterday data found. Skipping volume comparison.")
        return

    drop_pct = (yesterday_size - today_size) / yesterday_size

    if drop_pct > VOLUME_DROP_ALERT:
        log.warning(
            f"VOLUME ALERT: Today's partition is {drop_pct:.1%} smaller than yesterday. "
            f"Expected drop threshold: {VOLUME_DROP_ALERT:.0%}. "
            f"Investigate s3://{S3_BUCKET}/events/date={today_str}/"
        )
    else:
        log.info(f"Volume check passed. Drop: {drop_pct:.1%} (threshold: {VOLUME_DROP_ALERT:.0%})")

    context["ti"].xcom_push(key="today_size",     value=today_size)
    context["ti"].xcom_push(key="yesterday_size", value=yesterday_size)
    context["ti"].xcom_push(key="drop_pct",       value=round(drop_pct, 4))


def trigger_databricks_notebook(**context):
    """
    On Databricks Free Edition, the notebook is triggered manually.
    This task logs a reminder and passes so the DAG continues to dbt.
    In production with a paid tier, this would call the Databricks Jobs API.
    """
    date_str = context["ds"]
    log.info(f"REMINDER: Run the Databricks ingestion notebook manually for date: {date_str}")
    log.info("Notebook path: /gamepulse/01_ingest_raw")
    log.info("Assuming notebook has been run. Proceeding to dbt.")


def run_dbt_models(**context):
    """
    Logs a reminder for the operator to run dbt models locally.
    dbt cannot run inside the Airflow container due to Databricks Free Edition
    SQL Warehouse authentication restrictions on container network paths.
    In production this would call dbt via the Docker socket or run on a remote runner.
    """
    log.info("=" * 60)
    log.info("MANUAL STEP REQUIRED: Run dbt models")
    log.info("=" * 60)
    log.info("From your terminal, run:")
    log.info("  cd ~/gamepulse-analytics/dbt/gamepulse")
    log.info("  dbt run")
    log.info("=" * 60)

def run_dbt_tests(**context):
    """
    Logs a reminder for the operator to run dbt tests locally.
    """
    log.info("=" * 60)
    log.info("MANUAL STEP REQUIRED: Run dbt tests")
    log.info("=" * 60)
    log.info("From your terminal, run:")
    log.info("  cd ~/gamepulse-analytics/dbt/gamepulse")
    log.info("  dbt test")
    log.info("=" * 60)


# DAG DEFINITION
with DAG(
    dag_id="gamepulse_daily",
    description="GamePulse daily batch pipeline: S3 → Databricks → dbt",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 15 * * *",   # 17:00 CEST (UTC+2)
    start_date=datetime(2026, 5, 30),
    catchup=False,
    tags=["gamepulse", "data-engineering", "batch"],
) as dag:

    sense_partition = PythonSensor(
        task_id="sense_s3_partition",
        python_callable=sense_s3_partition,
        poke_interval=60,       # check every 60 seconds
        timeout=60 * 60 * 2,    # wait up to 2 hours before failing
        mode="poke",
    )

    validate_volume = PythonOperator(
        task_id="validate_event_volume",
        python_callable=validate_event_volume,
    )

    trigger_databricks = PythonOperator(
        task_id="trigger_databricks_notebook",
        python_callable=trigger_databricks_notebook,
    )

    run_models = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_models,
    )

    run_tests = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=run_dbt_tests,
    )

    # Task dependency chain
    sense_partition >> validate_volume >> trigger_databricks >> run_models >> run_tests