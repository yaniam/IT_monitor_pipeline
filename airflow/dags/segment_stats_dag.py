from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT_STR = str(PROJECT_ROOT)
if PROJECT_ROOT_STR not in sys.path:
    sys.path.append(PROJECT_ROOT_STR)

API_URL = os.getenv("SEGMENT_STATS_API_URL", "http://127.0.0.1:8000/segment-stats")
OUTPUT_DIR = PROJECT_ROOT / "data_collection"
SIM_MINUTES_ENV = os.getenv("SEGMENT_STATS_SIM_MINUTES")
SIM_STATE_ENV = os.getenv("SEGMENT_STATS_SIM_STATE_FILE")
SIM_MINUTES = int(SIM_MINUTES_ENV) if SIM_MINUTES_ENV else None
SIM_STATE_FILE = (
    Path(SIM_STATE_ENV) if SIM_STATE_ENV else OUTPUT_DIR / "simulation_clock.txt"
)


def run_ingestion_task(**context) -> str:
    from data_collection.fake_api_collector import run_ingestion

    execution_time = (
        context["data_interval_end"].to_datetime_string()
        if SIM_MINUTES is None
        else None
    )
    csv_path = run_ingestion(
        api_url=API_URL,
        output_dir=OUTPUT_DIR,
        current_time=execution_time,
        window_minutes=60,
        frequency_minutes=15,
        simulate_minutes=SIM_MINUTES,
        simulation_state_file=SIM_STATE_FILE,
    )
    return str(csv_path)


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="segment_stats_ingestion",
    default_args=default_args,
    description="Ingests synthetic segment stats via the fake REST API (supports accelerated simulations).",
    schedule_interval="* * * * *",
    start_date=datetime(2025, 11, 16, 0, 0),
    catchup=False,
    max_active_runs=1,
    tags=["synthetic", "segments"],
) as dag:
    ingest = PythonOperator(
        task_id="ingest_segment_stats",
        python_callable=run_ingestion_task,
        provide_context=True,
    )

