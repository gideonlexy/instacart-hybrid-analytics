"""Source contract for the Instacart raw CSV files."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RawFileContract:
    """Expected structure for one raw source file."""

    filename: str
    columns: tuple[str, ...]
    expected_rows: int
    grain: str

    @property
    def path(self) -> Path:
        return Path("data/raw") / self.filename


RAW_FILE_CONTRACTS: tuple[RawFileContract, ...] = (
    RawFileContract(
        filename="orders.csv",
        columns=(
            "order_id",
            "user_id",
            "eval_set",
            "order_number",
            "order_dow",
            "order_hour_of_day",
            "days_since_prior_order",
        ),
        expected_rows=3_421_083,
        grain="one row per order",
    ),
    RawFileContract(
        filename="order_products__prior.csv",
        columns=("order_id", "product_id", "add_to_cart_order", "reordered"),
        expected_rows=32_434_489,
        grain="one row per prior order-product pair",
    ),
    RawFileContract(
        filename="order_products__train.csv",
        columns=("order_id", "product_id", "add_to_cart_order", "reordered"),
        expected_rows=1_384_617,
        grain="one row per train order-product pair",
    ),
    RawFileContract(
        filename="products.csv",
        columns=("product_id", "product_name", "aisle_id", "department_id"),
        expected_rows=49_688,
        grain="one row per product",
    ),
    RawFileContract(
        filename="aisles.csv",
        columns=("aisle_id", "aisle"),
        expected_rows=134,
        grain="one row per aisle",
    ),
    RawFileContract(
        filename="departments.csv",
        columns=("department_id", "department"),
        expected_rows=21,
        grain="one row per department",
    ),
)


def contract_by_filename() -> dict[str, RawFileContract]:
    """Return raw file contracts keyed by filename."""
    return {contract.filename: contract for contract in RAW_FILE_CONTRACTS}
