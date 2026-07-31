"""Compatibility imports for Silver layer helpers.

Shared Delta Lake helpers now live in ``src.data.lakehouse`` so Gold can reuse
them without importing from a Silver-specific module.
"""

from __future__ import annotations

from src.data.lakehouse import (
    collect_duplicate_key_rows,
    collect_invalid_rows,
    collect_orphan_key_rows,
    raise_if_duplicate_keys_found,
    raise_if_invalid_rows_found,
    raise_if_orphan_keys_found,
    read_delta_table,
    write_delta_table,
)

__all__ = (
    "collect_duplicate_key_rows",
    "collect_invalid_rows",
    "collect_orphan_key_rows",
    "raise_if_duplicate_keys_found",
    "raise_if_invalid_rows_found",
    "raise_if_orphan_keys_found",
    "read_delta_table",
    "write_delta_table",
)
