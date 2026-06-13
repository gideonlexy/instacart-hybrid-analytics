"""Tests for raw data contract validation."""

from pathlib import Path

import pytest

from scripts.validate_raw_data import (
    RawDataValidationError,
    validate_contract,
)
from src.data.raw_contract import RawFileContract, contract_by_filename


def write_csv(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_contract_contains_required_instacart_files():
    contracts = contract_by_filename()

    assert set(contracts) == {
        "orders.csv",
        "order_products__prior.csv",
        "order_products__train.csv",
        "products.csv",
        "aisles.csv",
        "departments.csv",
    }


def test_validate_contract_accepts_matching_header_and_row_count(tmp_path):
    contract = RawFileContract(
        filename="sample.csv",
        columns=("id", "name"),
        expected_rows=2,
        grain="one row per sample",
    )
    write_csv(tmp_path / "sample.csv", ["id,name", "1,apple", "2,banana"])

    assert validate_contract(contract, raw_dir=tmp_path) == ("sample.csv", 2)


def test_validate_contract_rejects_missing_file(tmp_path):
    contract = RawFileContract(
        filename="missing.csv",
        columns=("id",),
        expected_rows=0,
        grain="one row per sample",
    )

    with pytest.raises(RawDataValidationError, match="Missing required raw file"):
        validate_contract(contract, raw_dir=tmp_path)


def test_validate_contract_rejects_wrong_header(tmp_path):
    contract = RawFileContract(
        filename="sample.csv",
        columns=("id", "name"),
        expected_rows=1,
        grain="one row per sample",
    )
    write_csv(tmp_path / "sample.csv", ["name,id", "apple,1"])

    with pytest.raises(RawDataValidationError, match="unexpected columns"):
        validate_contract(contract, raw_dir=tmp_path)


def test_validate_contract_can_skip_row_counts(tmp_path):
    contract = RawFileContract(
        filename="sample.csv",
        columns=("id", "name"),
        expected_rows=999,
        grain="one row per sample",
    )
    write_csv(tmp_path / "sample.csv", ["id,name", "1,apple"])

    assert validate_contract(
        contract,
        raw_dir=tmp_path,
        check_row_counts=False,
    ) == ("sample.csv", None)
