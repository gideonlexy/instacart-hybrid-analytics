"""Validate local Instacart raw CSV files against the source contract.

This script validates file presence, header order, and row counts without
loading the full dataset into memory. It is intentionally closer to a warehouse
source contract check than an exploratory pandas profiling notebook.

Run from the repository root:
    .venv/bin/python scripts/validate_raw_data.py
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.raw_contract import RAW_FILE_CONTRACTS, RawFileContract  # noqa: E402


class RawDataValidationError(Exception):
    """Raised when raw source files do not match the expected contract."""


def count_data_rows(path: Path) -> int:
    """Count rows after the header using streaming file iteration."""
    with path.open("r", encoding="utf-8", newline="") as file:
        next(file, None)
        return sum(1 for _ in file)


def read_header(path: Path) -> tuple[str, ...]:
    """Read a CSV header as a tuple of column names."""
    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.reader(file)
        return tuple(next(reader, ()))


def validate_contract(
    contract: RawFileContract,
    *,
    raw_dir: Path,
    check_row_counts: bool = True,
) -> tuple[str, int | None]:
    """Validate one raw file and return its filename plus observed row count."""
    path = raw_dir / contract.filename

    if not path.exists():
        raise RawDataValidationError(f"Missing required raw file: {path}")

    header = read_header(path)
    if header != contract.columns:
        raise RawDataValidationError(
            f"{path} has unexpected columns.\n"
            f"Expected: {contract.columns}\n"
            f"Observed: {header}"
        )

    if not check_row_counts:
        return contract.filename, None

    row_count = count_data_rows(path)
    if row_count != contract.expected_rows:
        raise RawDataValidationError(
            f"{path} has unexpected row count. "
            f"Expected {contract.expected_rows:,}, observed {row_count:,}."
        )

    return contract.filename, row_count


def validate_raw_data(
    *,
    raw_dir: Path,
    check_row_counts: bool = True,
) -> list[tuple[str, int | None]]:
    """Validate all raw files and return observed row counts."""
    return [
        validate_contract(
            contract,
            raw_dir=raw_dir,
            check_row_counts=check_row_counts,
        )
        for contract in RAW_FILE_CONTRACTS
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
        help="Directory containing the raw Instacart CSV files.",
    )
    parser.add_argument(
        "--skip-row-counts",
        action="store_true",
        help="Only validate file presence and headers.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        results = validate_raw_data(
            raw_dir=args.raw_dir,
            check_row_counts=not args.skip_row_counts,
        )
    except RawDataValidationError as exc:
        print(f"[RAW DATA] Validation failed: {exc}", file=sys.stderr)
        return 1

    print("[RAW DATA] Validation passed")
    for filename, row_count in results:
        if row_count is None:
            print(f"  - {filename}: header ok")
        else:
            print(f"  - {filename}: {row_count:,} rows")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
