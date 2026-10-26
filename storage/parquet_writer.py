"""Parquet historical storage helper.

PostgreSQL holds a bounded, recent, query-optimized slice of the data for
the live dashboard (see `Database.purge_old_data`). Parquet holds the full
historical record for offline/analytical use, partitioned by wiki, UTC date
and UTC hour so a given wiki/day/hour can be read back cheaply without
scanning everything. Files are Snappy-compressed.

This module is intentionally plain-pandas/pyarrow rather than going through
Spark's own Parquet sink, so it can be reused identically by the streaming
job's `foreachBatch` callback and by any offline analysis script.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from common.config import ParquetConfig

logger = logging.getLogger(__name__)

COMPRESSION = "snappy"


@dataclass
class ParquetWriteStats:
    rows: int = 0
    files: int = 0
    file_bytes: int = 0
    # Column-chunk sizes from the Parquet footer, before and after Snappy.
    uncompressed_bytes: int = 0
    compressed_bytes: int = 0


class ParquetWriter:
    def __init__(self, config: ParquetConfig):
        self._base_path = Path(config.base_path)

    def write_records(
        self,
        dataset: str,
        rows: Sequence[dict[str, Any]],
        time_field: str,
        wiki_field: str = "wiki",
    ) -> ParquetWriteStats:
        """Append `rows` to the appropriate wiki/date/hour partitions.

        Rows are grouped by (wiki, date, hour) so a single call may write to
        several partition directories. Each partition file is named with a
        timestamp to avoid clobbering concurrent micro-batch writes. Naive
        datetimes in `time_field` are treated as UTC.
        """
        stats = ParquetWriteStats()
        if not rows:
            return stats

        df = pd.DataFrame(rows)
        df[time_field] = pd.to_datetime(df[time_field], utc=True)
        # Group on the hour and format one key per group: strftime over
        # every row costs more than the Parquet write itself.
        hour_starts = df[time_field].dt.floor("h")

        for (wiki, hour_start), group in df.groupby([wiki_field, hour_starts]):
            partition_dir = (
                self._base_path / dataset / f"wiki={wiki}" / f"date={hour_start:%Y-%m-%d}" / f"hour={hour_start:%H}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)
            file_path = partition_dir / f"part-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
            # pyarrow hands back the footer it wrote, which saves re-opening the file for sizes.
            written_metadata: list = []
            group.to_parquet(file_path, index=False, compression=COMPRESSION, metadata_collector=written_metadata)

            metadata = written_metadata[0]
            stats.rows += len(group)
            stats.files += 1
            stats.file_bytes += file_path.stat().st_size
            for row_group_index in range(metadata.num_row_groups):
                row_group = metadata.row_group(row_group_index)
                for column_index in range(row_group.num_columns):
                    column = row_group.column(column_index)
                    stats.uncompressed_bytes += column.total_uncompressed_size
                    stats.compressed_bytes += column.total_compressed_size
            logger.debug("Wrote %d rows to %s", len(group), file_path)
        return stats
