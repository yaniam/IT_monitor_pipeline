#!/usr/bin/env python3
"""
Synthetic device data generator.

This script produces a CSV file with realistic-looking metadata for different
machine types (ATM, CC, PD, PC) based on the variable dictionary provided by
the business. It enforces:
  - Machine identifiers prefixed by the two-letter type code followed by six digits
  - Composite acquisition codes built from branch identifiers plus acquisition dates
  - Valid combinations of models, suppliers, operating systems, and OS versions
  - Brazilian state attribution and error flag per record
  - Last-connection timestamps within the past hour

Example:
    python data_raw/generate_synthetic_data.py -n 250 -o data_raw/devices.csv --seed 42
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

# -----------------------------
# Reference data/configuration
# -----------------------------

MACHINE_TYPES = ("ATM", "CC", "PD", "PC")

TYPE_PREFIX: Dict[str, str] = {
    "ATM": "AT",
    "CC": "CC",
    "PD": "PD",
    "PC": "PC",
}

MODELS: Dict[str, Tuple[str, ...]] = {
    "ATM": ("ATMv17", "ATMv18", "ATMRv18", "ATMv22", "ATMRv22"),
    "CC": ("STR", "TRL", "MFR"),
    "PD": ("GH", "GHF", "TRH", "TRHF"),
    "PC": ("Motto2018", "Motto2019", "Bell2020", "Bell2021", "Bell2022"),
}

SUPPLIERS: Dict[str, Tuple[str, ...]] = {
    "ATM": ("Cool IT Supplier", "Best Supplier"),
    "CC": ("Cool IT supplier", "Lespeed", "Meganet"),
    "PD": ("Best Supplier", "Lespeed"),
    "PC": ("Motto", "Bell"),
}

OPERATING_SYSTEM: Dict[str, str] = {
    "ATM": "Ubuntu",
    "CC": "Ubuntu",
    "PD": "Ubuntu",
    "PC": "Windows",
}

OS_VERSION_RANGE: Dict[str, Tuple[float, float]] = {
    "ATM": (17.1, 22.5),
    "CC": (17.1, 22.6),
    "PD": (17.1, 22.7),
    "PC": (35.0, 57.8),
}

BRAZILIAN_STATES: Tuple[str, ...] = (
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
)

ERROR_PROBABILITY = 0.05

# -----------------------------
# Data structures/helpers
# -----------------------------


@dataclass(frozen=True)
class DeviceRecord:
    device_code: str
    acquisition_sequence: str
    branch_id: str
    state: str
    has_error: bool
    machine_type: str
    machine_model: str
    supplier: str
    network_address: str
    operating_system: str
    os_version: str
    acquisition_date: str
    last_connection_ts: str

    def to_row(self) -> List[str]:
        return [
            self.device_code,
            self.acquisition_sequence,
            self.branch_id,
            self.state,
            "True" if self.has_error else "False",
            self.machine_type,
            self.machine_model,
            self.supplier,
            self.network_address,
            self.operating_system,
            self.os_version,
            self.acquisition_date,
            self.last_connection_ts,
        ]


def random_branch_id() -> str:
    """Return a 5-digit branch identifier as a zero-padded string."""
    return f"{random.randint(10000, 99999)}"


def random_acquisition_date() -> datetime:
    """Return a random acquisition date between 2021-01-01 and 2025-11-01."""
    start = datetime(2021, 1, 1)
    end = datetime(2025, 11, 1)
    days_range = (end - start).days
    return start + timedelta(days=random.randint(0, days_range))


def build_acquisition_sequence(branch_id: str, acq_date: datetime) -> str:
    """
    Build the 13-digit sequence: first five digits are branch id,
    followed by the YYYYMMDD acquisition date (8 digits).
    """
    return f"{branch_id}{acq_date:%Y%m%d}"


def random_device_code(machine_type: str) -> str:
    """Generate a device code with a two-letter prefix followed by six digits."""
    prefix = TYPE_PREFIX[machine_type]
    return f"{prefix}{random.randint(0, 999999):06d}"


def random_ipv4() -> str:
    """Generate a private IPv4 (10.x.x.x) address."""
    return f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def random_os_version(machine_type: str) -> str:
    """Generate a version string with one decimal place within the allowed range."""
    low, high = OS_VERSION_RANGE[machine_type]
    value = random.uniform(low, high)
    return f"{value:.1f}"


def random_recent_timestamp() -> str:
    """Return a timestamp between now and one hour ago (UTC)."""
    end = datetime.utcnow()
    start = end - timedelta(hours=1)
    seconds_offset = random.uniform(0, (end - start).total_seconds())
    ts = start + timedelta(seconds=seconds_offset)
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def build_record() -> DeviceRecord:
    machine_type = random.choice(MACHINE_TYPES)
    model = random.choice(MODELS[machine_type])
    supplier = random.choice(SUPPLIERS[machine_type])
    branch_id = random_branch_id()
    acq_date = random_acquisition_date()
    has_error = random.random() < ERROR_PROBABILITY

    return DeviceRecord(
        device_code=random_device_code(machine_type),
        acquisition_sequence=build_acquisition_sequence(branch_id, acq_date),
        branch_id=branch_id,
        state=random.choice(BRAZILIAN_STATES),
        has_error=has_error,
        machine_type=machine_type,
        machine_model=model,
        supplier=supplier,
        network_address=random_ipv4(),
        operating_system=OPERATING_SYSTEM[machine_type],
        os_version=random_os_version(machine_type),
        acquisition_date=acq_date.strftime("%Y-%m-%d"),
        last_connection_ts=random_recent_timestamp(),
    )


def generate_records(total: int) -> Iterable[DeviceRecord]:
    for _ in range(total):
        yield build_record()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic device metadata.")
    parser.add_argument(
        "-n",
        "--rows",
        type=int,
        default=500,
        help="Number of rows to generate (default: 500).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("data_raw") / "synthetic_devices.csv",
        help="Output CSV path (default: data_raw/synthetic_devices.csv).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Optional random seed for reproducibility.",
    )
    return parser.parse_args()


def write_csv(records: Iterable[DeviceRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    header = [
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
    ]

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        for record in records:
            writer.writerow(record.to_row())


def main() -> None:
    args = parse_args()
    if args.rows <= 0:
        raise SystemExit("Number of rows must be a positive integer.")

    if args.seed is not None:
        random.seed(args.seed)

    write_csv(generate_records(args.rows), args.output)
    print(f"Created {args.rows} synthetic rows at {args.output.resolve()}")


if __name__ == "__main__":
    main()

