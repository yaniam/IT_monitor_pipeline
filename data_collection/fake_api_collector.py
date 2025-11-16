#!/usr/bin/env python3
"""
Ingestion script that calls the fake REST API, stores the JSON response,
and produces a flattened CSV snapshot.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import requests

from common.segment_utils import ensure_output_dirs, flatten_segments

SIM_STATE_DEFAULT = Path("data_collection") / "simulation_clock.txt"
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect data from the fake REST API.")
    parser.add_argument(
        "--api-url",
        default="http://127.0.0.1:8000/segment-stats",
        help="Endpoint for the fake REST API.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data_collection"),
        help="Base directory for JSON/CSV outputs.",
    )
    parser.add_argument(
        "--current-time",
        help="Optional timestamp override for the API (YYYY-MM-DD HH:MM:SS).",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=60,
        help="Window size to request from the API.",
    )
    parser.add_argument(
        "--frequency-minutes",
        type=int,
        default=15,
        help="Frequency metadata to send to the API.",
    )
    parser.add_argument(
        "--update-probability",
        type=float,
        default=0.10,
        help="Probability hint passed to the API for timestamp refresh.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--simulate-minutes",
        type=int,
        help="Advance a simulated clock by this many minutes per run. Useful for fast testing.",
    )
    parser.add_argument(
        "--simulation-state-file",
        type=Path,
        default=SIM_STATE_DEFAULT,
        help="Where to persist the simulated clock (default: data_collection/simulation_clock.txt).",
    )
    return parser.parse_args()


def call_api(
    api_url: str,
    payload: Dict[str, object],
    timeout: int,
) -> Dict[str, object]:
    response = requests.post(api_url, json=payload, timeout=timeout)
    response.raise_for_status()
    return response.json()


def persist_outputs(payload: Dict[str, object], output_dir: Path) -> Path:
    json_dir, csv_dir = ensure_output_dirs(output_dir)
    timestamp = payload["generated_at"].replace(" ", "").replace(":", "").replace("-", "")
    json_path = json_dir / f"segment_stats_{timestamp}.json"
    csv_path = csv_dir / f"segment_stats_{timestamp}.csv"

    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    fieldnames = [
        "segment",
        "count",
        "dim1_name",
        "dim1_value",
        "dim2_name",
        "dim2_value",
        "dim3_name",
        "dim3_value",
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        for row in flatten_segments(payload):
            writer.writerow(row)
    return csv_path


def load_simulated_time(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, TIME_FORMAT)
    except ValueError:
        return None


def save_simulated_time(path: Path, value: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value.strftime(TIME_FORMAT), encoding="utf-8")


def determine_current_time(
    explicit_time: Optional[str],
    simulate_minutes: Optional[int],
    state_file: Path,
) -> Optional[str]:
    if explicit_time:
        return explicit_time
    if simulate_minutes is None:
        return None

    last_time = load_simulated_time(state_file)
    if last_time is None:
        last_time = datetime.utcnow()
    next_time = last_time + timedelta(minutes=simulate_minutes)
    save_simulated_time(state_file, next_time)
    return next_time.strftime(TIME_FORMAT)


def run_ingestion(
    api_url: str,
    output_dir: Path,
    current_time: Optional[str] = None,
    window_minutes: int = 60,
    frequency_minutes: int = 15,
    update_probability: float = 0.10,
    timeout: int = 30,
    simulate_minutes: Optional[int] = None,
    simulation_state_file: Path = SIM_STATE_DEFAULT,
) -> Path:
    effective_current_time = determine_current_time(current_time, simulate_minutes, simulation_state_file)
    api_payload = {
        "current_time": effective_current_time,
        "window_minutes": window_minutes,
        "frequency_minutes": frequency_minutes,
        "update_probability": update_probability,
    }
    payload = call_api(api_url, api_payload, timeout=timeout)
    csv_path = persist_outputs(payload, output_dir)
    print(f"Ingestion finished for timestamp {payload['generated_at']}")
    return csv_path


def main() -> None:
    args = parse_args()
    run_ingestion(
        api_url=args.api_url,
        output_dir=args.output_dir,
        current_time=args.current_time,
        window_minutes=args.window_minutes,
        frequency_minutes=args.frequency_minutes,
        update_probability=args.update_probability,
        timeout=args.timeout,
        simulate_minutes=args.simulate_minutes,
        simulation_state_file=args.simulation_state_file,
    )


if __name__ == "__main__":
    main()

