#!/usr/bin/env python3
"""
Store flattened segment CSV rows as time-series entries in SQLite.
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

DEFAULT_DB_PATH = Path("data_collection") / "segment_timeseries.db"
FILENAME_PATTERN = re.compile(r"segment_stats_(\d{12})\.csv$")
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist segment CSV rows into SQLite.")
    parser.add_argument("--csv-path", type=Path, required=True, help="Path to the flattened CSV file.")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite output path (default: {DEFAULT_DB_PATH}).",
    )
    parser.add_argument(
        "--timestamp",
        help="Override the timestamp for this batch (default: derived from filename or current time).",
    )
    return parser.parse_args()


def extract_timestamp(csv_path: Path, override: Optional[str]) -> str:
    if override:
        return override
    match = FILENAME_PATTERN.search(csv_path.name)
    if match:
        dt = datetime.strptime(match.group(1), "%Y%m%d%H%M")
        return dt.strftime(TIMESTAMP_FORMAT)
    return datetime.utcnow().strftime(TIMESTAMP_FORMAT)


def read_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return [row for row in reader]


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS segment_timeseries (
            ts TEXT NOT NULL,
            segment TEXT NOT NULL,
            count INTEGER NOT NULL,
            dim1_name TEXT,
            dim1_value TEXT,
            dim2_name TEXT,
            dim2_value TEXT,
            dim3_name TEXT,
            dim3_value TEXT,
            PRIMARY KEY (ts, segment, dim1_name, dim1_value, dim2_name, dim2_value, dim3_name, dim3_value)
        )
        """
    )


def store_rows(csv_path: Path, db_path: Path, timestamp: str) -> int:
    rows = read_rows(csv_path)
    if not rows:
        return 0

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        ensure_table(conn)
        insert_sql = """
            INSERT OR REPLACE INTO segment_timeseries (
                ts,
                segment,
                count,
                dim1_name,
                dim1_value,
                dim2_name,
                dim2_value,
                dim3_name,
                dim3_value
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        conn.executemany(
            insert_sql,
            [
                (
                    timestamp,
                    row.get("segment", ""),
                    int(row.get("count", 0) or 0),
                    row.get("dim1_name"),
                    row.get("dim1_value"),
                    row.get("dim2_name"),
                    row.get("dim2_value"),
                    row.get("dim3_name"),
                    row.get("dim3_value"),
                )
                for row in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    timestamp = extract_timestamp(args.csv_path, args.timestamp)
    inserted = store_rows(args.csv_path, args.db_path, timestamp)
    print(f"Stored {inserted} rows for ts={timestamp} into {args.db_path}")


if __name__ == "__main__":
    main()

