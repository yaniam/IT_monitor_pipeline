from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

SEGMENTS: Tuple[Tuple[str, Tuple[str, ...]], ...] = (
    ("State", ("state",)),
    ("Type", ("machine_type",)),
    ("Model", ("machine_model",)),
    ("Supplier", ("supplier",)),
    ("Model and Supplier", ("machine_model", "supplier")),
    ("OS", ("operating_system",)),
    ("OS_Version", ("os_version",)),
    ("Type and OS and OS_version", ("machine_type", "operating_system", "os_version")),
)

MAX_DIM_COLUMNS = 3
UPDATE_WINDOW_MINUTES = 15

DEVICE_FIELDNAMES: Tuple[str, ...] = (
    "device_code",
    "acquisition_sequence",
    "branch_id",
    "state",
    "has_error",
    "machine_type",
    "machine_model",
    "supplier",
    "network_address",
    "operating_system",
    "os_version",
    "acquisition_date",
    "last_connection_ts",
)

DATASET_PATH = Path("data_raw") / "synthetic_devices.csv"


def ensure_output_dirs(base_dir: Path) -> Tuple[Path, Path]:
    json_dir = base_dir / "json"
    csv_dir = base_dir / "csv"
    json_dir.mkdir(parents=True, exist_ok=True)
    csv_dir.mkdir(parents=True, exist_ok=True)
    return json_dir, csv_dir


def load_devices(path: Path = DATASET_PATH) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found at {path}. Generate data first.")

    with path.open("r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        devices: List[Dict[str, str]] = []
        for row in reader:
            if "has_error" not in row or row["has_error"] in (None, ""):
                row["has_error"] = "False"
            devices.append(row)
        return devices


def write_devices(path: Path, devices: Sequence[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=DEVICE_FIELDNAMES)
        writer.writeheader()
        for row in devices:
            normalized = {field: row.get(field, "") for field in DEVICE_FIELDNAMES}
            writer.writerow(normalized)


def parse_timestamp(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def filter_recent(devices: Iterable[Dict[str, str]], now: datetime, window_minutes: int) -> List[Dict[str, str]]:
    cutoff = now - timedelta(minutes=window_minutes)
    recent: List[Dict[str, str]] = []
    for row in devices:
        ts = parse_timestamp(row.get("last_connection_ts", ""))
        if ts and cutoff <= ts <= now:
            recent.append(row)
    return recent


def aggregate_segment(devices: Iterable[Dict[str, str]], dimensions: Sequence[str]) -> List[Dict[str, object]]:
    counter: Dict[Tuple[str, ...], int] = {}

    for row in devices:
        key = tuple(row[dimension] for dimension in dimensions)
        counter[key] = counter.get(key, 0) + 1

    rows = []
    for key in sorted(counter.keys()):
        dim_map = {dimension: value for dimension, value in zip(dimensions, key)}
        rows.append({"dimensions": dim_map, "count": counter[key]})
    return rows


def parse_current_time(value: Optional[str]) -> datetime:
    if not value:
        return datetime.utcnow()

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError('Invalid timestamp format. Use "YYYY-MM-DD HH:MM:SS".')


def to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def random_timestamp_between(start: datetime, end: datetime) -> datetime:
    if start > end:
        start, end = end, start
    total_seconds = int((end - start).total_seconds())
    if total_seconds <= 0:
        return start
    offset = random.randint(0, total_seconds)
    return start + timedelta(seconds=offset)


def maybe_update_last_connections(
    devices: List[Dict[str, str]],
    now: datetime,
    update_probability: float,
) -> None:
    window_start = now - timedelta(minutes=UPDATE_WINDOW_MINUTES)
    for row in devices:
        if to_bool(row.get("has_error", "False")):
            continue
        if random.random() <= update_probability:
            ts = random_timestamp_between(window_start, now)
            row["last_connection_ts"] = ts.strftime("%Y-%m-%d %H:%M:%S")


def build_payload(devices: List[Dict[str, str]], now: datetime, window_minutes: int, frequency_minutes: int) -> Dict[str, object]:
    segments_payload = []
    for name, dimensions in SEGMENTS:
        rows = aggregate_segment(devices, dimensions)
        segments_payload.append({"name": name, "dimensions": list(dimensions), "rows": rows})

    return {
        "generated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "window_minutes": window_minutes,
        "frequency_minutes": frequency_minutes,
        "device_count_in_window": len(devices),
        "segments": segments_payload,
    }


def flatten_segments(payload: Dict[str, object]) -> Iterator[Dict[str, object]]:
    segments = payload.get("segments", [])
    for segment in segments:
        name = segment["name"]
        dimensions: List[str] = segment["dimensions"]
        rows = segment["rows"]

        for row in rows:
            dim_values: Dict[str, str] = row["dimensions"]
            flat_row: Dict[str, object] = {
                "segment": name,
                "count": row["count"],
            }

            for idx in range(MAX_DIM_COLUMNS):
                name_key = f"dim{idx + 1}_name"
                value_key = f"dim{idx + 1}_value"
                if idx < len(dimensions):
                    dim_name = dimensions[idx]
                    flat_row[name_key] = dim_name
                    flat_row[value_key] = dim_values.get(dim_name, "")
                else:
                    flat_row[name_key] = ""
                    flat_row[value_key] = ""

            yield flat_row

