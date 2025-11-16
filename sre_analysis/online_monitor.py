from __future__ import annotations

import argparse
import csv
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import List, Sequence, Tuple

DEFAULT_DB_PATH = Path("data_collection") / "segment_timeseries.db"
DEFAULT_DATASET_PATH = Path("data_raw") / "synthetic_devices.csv"
DEFAULT_ALERT_LOG = Path("sre_analysis") / "alerts.log"
DEFAULT_SEGMENT = "Type"
DEFAULT_HISTORY = 4
DEFAULT_DROP_THRESHOLD = 0.15  # 15% drop


@dataclass(frozen=True)
class OnlineSnapshot:
    """Represents a single ingestion timestamp and its online share."""

    timestamp: str
    total_online: int
    percentage: float


@dataclass(frozen=True)
class MonitorResult:
    """Outcome of evaluating the latest snapshot."""

    snapshot: OnlineSnapshot | None
    baseline_percentage: float | None
    drop_fraction: float | None
    alert_triggered: bool
    message: str


class OnlineMonitor:
    """Calculate online percentages and detect significant drops."""

    def __init__(
        self,
        db_path: Path = DEFAULT_DB_PATH,
        dataset_path: Path = DEFAULT_DATASET_PATH,
        segment: str = DEFAULT_SEGMENT,
        history: int = DEFAULT_HISTORY,
        drop_threshold: float = DEFAULT_DROP_THRESHOLD,
        alert_log: Path = DEFAULT_ALERT_LOG,
        enable_alert_log: bool = True,
    ) -> None:
        self.db_path = Path(db_path)
        self.dataset_path = Path(dataset_path)
        self.segment = segment
        self.history = max(1, history)
        self.drop_threshold = max(0.0, drop_threshold)
        self.alert_log = Path(alert_log)
        self.enable_alert_log = enable_alert_log

    # -----------------------
    # Public API
    # -----------------------
    def load_history(self) -> List[OnlineSnapshot]:
        totals = self._fetch_online_totals()
        if not totals:
            return []
        total_devices = self._count_total_devices()
        return [
            OnlineSnapshot(ts, total, self._as_percentage(total, total_devices))
            for ts, total in totals
        ]

    def evaluate_latest(self) -> MonitorResult:
        snapshots = self.load_history()
        if not snapshots:
            return MonitorResult(
                snapshot=None,
                baseline_percentage=None,
                drop_fraction=None,
                alert_triggered=False,
                message="No ingestion snapshots found in the time-series database.",
            )

        latest = snapshots[-1]
        baseline_samples = snapshots[:-1][-self.history :] if len(snapshots) > 1 else []
        baseline_pct = mean(s.percentage for s in baseline_samples) if baseline_samples else None
        drop_fraction = None
        alert_triggered = False

        if baseline_pct and baseline_pct > 0:
            drop_fraction = max(0.0, (baseline_pct - latest.percentage) / baseline_pct)
            alert_triggered = drop_fraction >= self.drop_threshold

        if alert_triggered and self.enable_alert_log:
            self._write_alert(latest, baseline_pct or 0.0, drop_fraction or 0.0)

        message = self._compose_message(latest, baseline_pct, drop_fraction, alert_triggered)

        return MonitorResult(
            snapshot=latest,
            baseline_percentage=baseline_pct,
            drop_fraction=drop_fraction,
            alert_triggered=alert_triggered,
            message=message,
        )

    # -----------------------
    # Internal helpers
    # -----------------------
    def _fetch_online_totals(self) -> List[Tuple[str, int]]:
        if not self.db_path.exists():
            return []

        query = """
            SELECT ts, SUM(count) AS total_online
            FROM segment_timeseries
            WHERE segment = ?
            GROUP BY ts
            ORDER BY ts ASC
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(query, (self.segment,))
            rows = cursor.fetchall()
            return [(ts, int(total)) for ts, total in rows]
        except sqlite3.OperationalError:
            return []
        finally:
            conn.close()

    def _count_total_devices(self) -> int:
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found at {self.dataset_path}")

        with self.dataset_path.open("r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp)
            total = sum(1 for _ in reader)

        if total <= 0:
            raise ValueError(f"No device records found in {self.dataset_path}")
        return total

    @staticmethod
    def _as_percentage(numerator: int, denominator: int) -> float:
        if denominator <= 0:
            raise ValueError("Denominator must be greater than zero.")
        return (numerator / denominator) * 100.0

    def _write_alert(
        self,
        snapshot: OnlineSnapshot,
        baseline_percentage: float,
        drop_fraction: float,
    ) -> None:
        timestamp = datetime.now(timezone.utc).isoformat()
        self.alert_log.parent.mkdir(parents=True, exist_ok=True)
        line = (
            f"{timestamp} | ts={snapshot.timestamp} | "
            f"online={snapshot.total_online} | pct={snapshot.percentage:.2f} | "
            f"baseline={baseline_percentage:.2f} | drop={drop_fraction:.2%}\n"
        )
        with self.alert_log.open("a", encoding="utf-8") as fp:
            fp.write(line)

    @staticmethod
    def _compose_message(
        latest: OnlineSnapshot,
        baseline_percentage: float | None,
        drop_fraction: float | None,
        alert_triggered: bool,
    ) -> str:
        parts = [
            f"[SRE] Latest snapshot @ {latest.timestamp}: "
            f"{latest.total_online} machines online "
            f"({latest.percentage:.2f}%)."
        ]
        if baseline_percentage is not None:
            parts.append(f"Rolling baseline: {baseline_percentage:.2f}%.")
        if drop_fraction is not None:
            parts.append(f"Drop vs. baseline: {drop_fraction:.2%}.")
        if alert_triggered:
            parts.append("ALERT: online percentage fell beyond the allowed threshold.")
        return " ".join(parts)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate the percentage of machines online in the last hour."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite path with ingestion snapshots (default: {DEFAULT_DB_PATH}).",
    )
    parser.add_argument(
        "--dataset-path",
        type=Path,
        default=DEFAULT_DATASET_PATH,
        help=f"CSV containing the device inventory (default: {DEFAULT_DATASET_PATH}).",
    )
    parser.add_argument(
        "--segment",
        default=DEFAULT_SEGMENT,
        help="Segment to aggregate for total counts (default: Type).",
    )
    parser.add_argument(
        "--history",
        type=int,
        default=DEFAULT_HISTORY,
        help="How many prior snapshots to average when computing the baseline.",
    )
    parser.add_argument(
        "--drop-threshold",
        type=float,
        default=DEFAULT_DROP_THRESHOLD,
        help="Alert when the drop fraction exceeds this value (0.15 = 15%%).",
    )
    parser.add_argument(
        "--alert-log",
        type=Path,
        default=DEFAULT_ALERT_LOG,
        help="Optional log file for alerts (default: sre_analysis/alerts.log).",
    )
    parser.add_argument(
        "--disable-alert-log",
        action="store_true",
        help="Skip writing alert entries to disk.",
    )
    return parser.parse_args(argv)


def run_post_ingestion_monitor(
    db_path: Path = DEFAULT_DB_PATH,
    dataset_path: Path = DEFAULT_DATASET_PATH,
    history: int = DEFAULT_HISTORY,
    drop_threshold: float = DEFAULT_DROP_THRESHOLD,
) -> MonitorResult:
    monitor = OnlineMonitor(
        db_path=db_path,
        dataset_path=dataset_path,
        history=history,
        drop_threshold=drop_threshold,
    )
    return monitor.evaluate_latest()


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    monitor = OnlineMonitor(
        db_path=args.db_path,
        dataset_path=args.dataset_path,
        segment=args.segment,
        history=args.history,
        drop_threshold=args.drop_threshold,
        alert_log=args.alert_log,
        enable_alert_log=not args.disable_alert_log,
    )
    result = monitor.evaluate_latest()
    print(result.message)


if __name__ == "__main__":
    main()

