"""Parquet historical storage helper.

PostgreSQL holds a bounded, recent, query-optimized slice of the data for
the live dashboard (see `Database.purge_old_data`). Parquet holds the full
historical record for offline/analytical use, partitioned by wiki and date
so a given day/wiki can be read back cheaply without scanning everything.

This module is intentionally plain-pandas/pyarrow rather than going through
Spark's own Parquet sink, so it can be reused identically by the streaming
job's `foreachBatch` callback and by any offline analysis script.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from common.config import ParquetConfig

logger = logging.getLogger(__name__)


class ParquetWriter:
    def __init__(self, config: ParquetConfig):
        self._base_path = Path(config.base_path)

    def _partition_path(self, dataset: str, wiki: str, event_time: datetime) -> Path:
        date_str = event_time.astimezone(timezone.utc).strftime("%Y-%m-%d")
        return self._base_path / dataset / f"wiki={wiki}" / f"date={date_str}"

    def write_records(
        self,
        dataset: str,
        rows: Sequence[dict[str, Any]],
        time_field: str,
        wiki_field: str = "wiki",
    ) -> None:
        """Append `rows` to the appropriate wiki/date partitions.

        Rows are grouped by (wiki, date) so a single call may write to
        several partition directories. Each partition file is named with a
        timestamp to avoid clobbering concurrent micro-batch writes.
        """
        if not rows:
            return

        df = pd.DataFrame(rows)
        df[time_field] = pd.to_datetime(df[time_field], utc=True)

        for (wiki, date), group in df.groupby(
            [wiki_field, df[time_field].dt.strftime("%Y-%m-%d")]
        ):
            partition_dir = self._base_path / dataset / f"wiki={wiki}" / f"date={date}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            file_name = f"part-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}.parquet"
            group.to_parquet(partition_dir / file_name, index=False)
            logger.debug("Wrote %d rows to %s", len(group), partition_dir / file_name)
