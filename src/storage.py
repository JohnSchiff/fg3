"""Parquet writer (daily files), optional migration sync, and DuckDB materialization.

Architecture
------------
Pipeline Parquet (:data:`PARQUET_DIR` = ``~/fg3-data/parquet``)  ingest / enrich_spot write here (local disk)
        │  materialize_duckdb()
        ▼
Local DuckDB    (:data:`DUCKDB_PATH`)                         derived tables, rebuildable

``sync_parquet_local`` copies from a *source* tree (e.g. legacy ``output/parquet`` on OneDrive)
into :data:`LOCAL_PARQUET_DIR` when migrating; default src/dst are both local when using the
current config.
"""

from __future__ import annotations

import datetime
import logging
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import (
    DUCKDB_PATH,
    LOCAL_PARQUET_DIR,
    PARQUET_DIR,
)

log = logging.getLogger(__name__)

DATA_TYPES = ("trades", "quotes")


# ---------------------------------------------------------------------------
# Pipeline output (writes to PARQUET_DIR = local ~/fg3-data/parquet)
# ---------------------------------------------------------------------------

def write_daily_parquet(
    table: pa.Table,
    data_type: str,
    date: datetime.date,
) -> Path:
    """Write a single-day Arrow table to a Hive-partitioned Parquet file.

    Returns the path of the written file.
    """
    year = date.year
    month = date.month
    date_str = date.strftime("%Y%m%d")

    out_dir = PARQUET_DIR / data_type / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{data_type}_{date_str}.parquet"

    pq.write_table(table, out_path, compression="zstd")
    log.info("Wrote %s  (%d rows)", out_path.relative_to(PARQUET_DIR), len(table))
    return out_path


# ---------------------------------------------------------------------------
# Local sync: OneDrive parquet  →  local parquet (source of truth)
# ---------------------------------------------------------------------------

_COPY_RETRIES = 3
_COPY_RETRY_DELAY = 10  # seconds


def _copy_with_retry(src_file: Path, dst_file: Path) -> bool:
    """Copy a single file with retries for OneDrive timeout handling."""
    import time

    for attempt in range(1, _COPY_RETRIES + 1):
        try:
            shutil.copy2(src_file, dst_file)
            return True
        except (TimeoutError, OSError) as exc:
            if attempt < _COPY_RETRIES:
                log.warning(
                    "  Retry %d/%d for %s (%s)",
                    attempt, _COPY_RETRIES, src_file.name, exc,
                )
                time.sleep(_COPY_RETRY_DELAY * attempt)
            else:
                log.error("  FAILED after %d attempts: %s", _COPY_RETRIES, src_file.name)
                return False


def sync_parquet_local(
    src: Path | None = None,
    dst: Path | None = None,
    years: list[int] | None = None,
) -> dict[str, int]:
    """Copy Parquet files from *src* to *dst*, skipping files that already
    exist with the same size.

    Defaults copy :data:`PARQUET_DIR` → :data:`LOCAL_PARQUET_DIR` (same path
    with current config — no-op). Use ``src=LEGACY_PARQUET_DIR`` to pull an old
    ``output/parquet`` tree onto local disk.

    Retries on slow/cloud timeouts (up to 3 attempts per file).
    Returns ``{data_type: files_copied}`` counts.
    """
    src = src or PARQUET_DIR
    dst = dst or LOCAL_PARQUET_DIR
    if src.resolve() == dst.resolve():
        log.info("sync_parquet_local: src and dst are the same (%s); nothing to copy", src)
        return {"trades": 0, "quotes": 0}
    stats: dict[str, int] = {}
    failed: list[str] = []

    for data_type in DATA_TYPES:
        copied = 0
        src_root = src / data_type
        if not src_root.exists():
            log.warning("Source missing: %s", src_root)
            continue

        year_dirs = sorted(src_root.glob("year=*"))
        for yd in year_dirs:
            year_num = int(yd.name.split("=")[1])
            if years and year_num not in years:
                continue

            for month_dir in sorted(yd.glob("month=*")):
                dst_month = dst / data_type / yd.name / month_dir.name
                dst_month.mkdir(parents=True, exist_ok=True)

                for pf in sorted(month_dir.glob("*.parquet")):
                    dst_file = dst_month / pf.name
                    if dst_file.exists() and dst_file.stat().st_size == pf.stat().st_size:
                        continue
                    if _copy_with_retry(pf, dst_file):
                        copied += 1
                    else:
                        failed.append(str(pf))

        stats[data_type] = copied
        log.info("Synced %s: %d new files → %s", data_type, copied, dst / data_type)

    if failed:
        log.error("%d files failed to copy:\n  %s", len(failed), "\n  ".join(failed))

    return stats


# ---------------------------------------------------------------------------
# DuckDB views (lightweight, reads parquet at query time)
# ---------------------------------------------------------------------------

def register_duckdb_views(
    db_path: Path | str | None = None,
    parquet_root: Path | None = None,
    years: list[int] | None = None,
) -> duckdb.DuckDBPyConnection:
    """Create (or open) a DuckDB database and register Parquet views.

    Parameters
    ----------
    db_path : Path, ``\":memory:\"``, or None
        Path to the DuckDB file.  ``None`` defaults to :data:`DUCKDB_PATH`.
        Use ``\":memory:\"`` for an ephemeral DB (avoids clashing with
        materialized tables in an on-disk file).
    parquet_root : Path or None
        Parquet directory to read from.  Defaults to :data:`LOCAL_PARQUET_DIR`.
    years : list[int] or None
        If given, restrict views to only these years.  ``None`` includes all.

    Returns the open connection.
    """
    pq_root = parquet_root or LOCAL_PARQUET_DIR
    if db_path == ":memory:":
        con = duckdb.connect(":memory:")
    else:
        resolved = Path(db_path) if db_path is not None else DUCKDB_PATH
        resolved.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(resolved))

    for data_type in DATA_TYPES:
        if years is None:
            globs = [str(pq_root / data_type / "**" / "*.parquet")]
        else:
            globs = [
                str(pq_root / data_type / f"year={y}" / "**" / "*.parquet")
                for y in years
            ]

        if len(globs) == 1:
            src = f"read_parquet('{globs[0]}', hive_partitioning=true)"
        else:
            joined = ", ".join(f"'{g}'" for g in globs)
            src = f"read_parquet([{joined}], hive_partitioning=true)"

        con.execute(f"""
            CREATE OR REPLACE VIEW {data_type} AS
            SELECT * FROM {src}
        """)
        log.info("Registered DuckDB view: %s (years=%s)", data_type, years or "all")

    return con


# ---------------------------------------------------------------------------
# DuckDB materialization (local parquet → standalone DuckDB tables)
# ---------------------------------------------------------------------------

def materialize_duckdb(
    db_path: Path | None = None,
    parquet_root: Path | None = None,
    years: list[int] | None = None,
) -> Path:
    """Copy Parquet data into materialized DuckDB tables.

    The resulting file is fully standalone — no external file dependencies.
    Processes one year at a time to tolerate slow I/O gracefully.

    Parameters
    ----------
    db_path : Path or None
        Destination DuckDB file.  Defaults to :data:`DUCKDB_PATH`.
    parquet_root : Path or None
        Parquet directory to read from.  Defaults to :data:`LOCAL_PARQUET_DIR`.
    years : list[int] or None
        Years to include.  ``None`` auto-discovers from *parquet_root*.

    Returns the path to the DuckDB file.
    """
    db_path = db_path or DUCKDB_PATH
    pq_root = parquet_root or LOCAL_PARQUET_DIR
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))

    if years is None:
        years = sorted(
            int(p.name.split("=")[1])
            for p in (pq_root / "trades").glob("year=*")
            if p.is_dir()
        )
    log.info("Materializing years %s into %s (from %s)", years, db_path, pq_root)

    for data_type in DATA_TYPES:
        # DB may already hold a materialized TABLE; DROP VIEW first errors if name is a table.
        con.execute(f"DROP TABLE IF EXISTS {data_type}")
        con.execute(f"DROP VIEW IF EXISTS {data_type}")

        # One read_parquet over all year globs so hive partition columns stay aligned.
        # Per-year INSERT into an existing table can mis-bind ``year`` across partitions.
        globs = [
            str(pq_root / data_type / f"year={year}" / "**" / "*.parquet")
            for year in years
        ]
        quoted = ", ".join("'" + g.replace("'", "''") + "'" for g in globs)
        src = f"read_parquet([{quoted}], hive_partitioning=true)"
        con.execute(f"""
            CREATE TABLE {data_type} AS
            SELECT * FROM {src}
        """)

        for year in years:
            count = con.execute(
                f"SELECT count(*) FROM {data_type} WHERE year = {year}"
            ).fetchone()[0]
            log.info("  %s year=%d: %d rows", data_type, year, count)

        total = con.execute(f"SELECT count(*) FROM {data_type}").fetchone()[0]
        log.info("Materialized %s: %d total rows", data_type, total)

    con.execute("""
        CREATE OR REPLACE TABLE _metadata AS
        SELECT current_timestamp AS materialized_at,
               list_value({ylist}) AS years_included
    """.format(ylist=", ".join(str(y) for y in years)))

    con.close()
    size_mb = db_path.stat().st_size / (1024 * 1024)
    log.info("Done. %s  (%.1f MB)", db_path, size_mb)
    return db_path
