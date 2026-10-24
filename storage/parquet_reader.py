"""Analytical Lake Parquet reading utilities.

Provides optimized sub-column scans via DuckDB or PyArrow, allowing
analytical engines to read specific columnar arrays (e.g., `byte_delta`,
`is_revert`) without deserializing unneeded text and metadata.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional, Sequence

import pandas as pd
import pyarrow.dataset as ds

logger = logging.getLogger(__name__)


def scan_subcolumns_duckdb(
    parquet_path: str | Path,
    columns: Sequence[str],
    where_clause: Optional[str] = None,
) -> pd.DataFrame:
    """Perform a sub-column projection scan on Parquet files using DuckDB.

    Reads only the specified columns without row/string deserialization overhead.
    """
    import duckdb

    path_str = Path(parquet_path).as_posix()
    if Path(parquet_path).is_dir():
        path_str = f"{path_str}/**/*.parquet"

    col_str = ", ".join(f'"{col}"' for col in columns) if columns else "*"
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    query = f"SELECT {col_str} FROM read_parquet('{path_str}') {where_sql}"

    conn = duckdb.connect(":memory:")
    try:
        return conn.execute(query).df()
    finally:
        conn.close()


def scan_subcolumns_pyarrow(
    parquet_dir: str | Path,
    columns: Sequence[str],
    filter_expr: Optional[Any] = None,
) -> pd.DataFrame:
    """Perform a sub-column projection scan using PyArrow dataset."""
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    table = dataset.to_table(columns=list(columns), filter=filter_expr)
    return table.to_pandas()
