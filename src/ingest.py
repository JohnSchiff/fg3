"""Orchestrator: discover -> extract -> parse -> write.

Usage::

    python -m src.ingest --years 2023
    python -m src.ingest --years 2023 --months 1 2 3
    python -m src.ingest --quotes-only --years 2023
    python -m src.ingest --incremental
    python -m src.ingest --years 2023 --no-enrich   # quotes: skip underlying_index cleaning
    python -m src.ingest --years 2023 --with-orders  # also write orders Parquet (off by default)
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import duckdb

from src.config import PARQUET_DIR
from src.discover import (
    WeekFolder,
    discover_weeks,
    is_already_ingested,
    load_ingest_state,
    mark_ingested,
    save_ingest_state,
)
from src.enrich_spot import clean_spot_df
from src.extract import extract_rep_files, remove_extracted_reps
from src.parsers.orders import parse_orders_file
from src.parsers.quotes import parse_quotes_file
from src.parsers.trades import parse_trades_file
from src.storage import register_duckdb_views, write_daily_parquet

log = logging.getLogger(__name__)


def _load_contract_map_from_parquet() -> dict[str, dict]:
    """Build contract_number -> instrument info from existing Parquet files."""
    contract_map: dict[str, dict] = {}
    # Ephemeral DB + pipeline Parquet root: on-disk DUCKDB_PATH may hold
    # materialized tables, which conflict with CREATE OR REPLACE VIEW.
    con = register_duckdb_views(db_path=":memory:", parquet_root=PARQUET_DIR)
    try:
        try:
            rows = con.execute("""
                SELECT DISTINCT contract_number, underlying, option_type,
                       strike, expiry_code, expiry_date
                FROM trades
            """).fetchall()
            for cn, und, ot, strike, ec, ed in rows:
                contract_map[cn] = {
                    "underlying": und,
                    "option_type": ot,
                    "strike": strike,
                    "expiry_code": ec,
                    "expiry_date": ed,
                }
        except Exception as exc:
            log.warning("Could not load contracts from trades Parquet: %s", exc)

        if not contract_map:
            log.warning("No contracts from trades; trying orders")
            try:
                rows = con.execute("""
                    SELECT DISTINCT contract_number, underlying, option_type,
                           strike, expiry_code, expiry_date
                    FROM orders
                """).fetchall()
                for cn, und, ot, strike, ec, ed in rows:
                    contract_map[cn] = {
                        "underlying": und,
                        "option_type": ot,
                        "strike": strike,
                        "expiry_code": ec,
                        "expiry_date": ed,
                    }
            except Exception as exc:
                log.warning("Could not load contracts from orders Parquet: %s", exc)
    finally:
        con.close()
    return contract_map


def ingest(
    years: list[int] | None = None,
    months: list[int] | None = None,
    incremental: bool = False,
    quotes_only: bool = False,
    fail_fast: bool = False,
    no_enrich: bool = False,
    with_orders: bool = False,
) -> None:
    """Run the ingestion pipeline.

    By default, Phase 2 (orders) only extracts contract-number mappings
    needed for quote enrichment -- no orders Parquet is written.  Pass
    *with_orders=True* to also persist orders to Parquet (large dataset,
    ~155M rows/year).

    Parquet is written to :data:`src.config.PARQUET_DIR` (``~/fg3-data/parquet``),
    not under the repo ``output/`` tree.
    """
    state = load_ingest_state() if incremental else {}
    log.info("Scanning data directory for week-folders (years=%s, months=%s) ...", years, months)
    weeks = list(discover_weeks(years=years, months=months))
    if weeks:
        log.info("Discovered %d week-folders: %s ... %s", len(weeks), weeks[0].key, weeks[-1].key)
    else:
        log.warning("No week-folders found — check data/ directory and filters")

    global_contract_map: dict[str, dict] = {}
    daily_trades: dict[datetime.date, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    pipeline_start = time.time()

    if quotes_only:
        log.info("=== Quotes-only mode: loading contract map from Parquet ===")
        global_contract_map = _load_contract_map_from_parquet()
        log.info("Loaded %d contracts from existing Parquet", len(global_contract_map))
    else:
        # --- Phase 1: Trades ---
        log.info("=" * 60)
        log.info("=== Phase 1: Trades ===")
        log.info("=" * 60)
        phase_start = time.time()
        trade_rows_total = 0
        for wi, week in enumerate(weeks, 1):
            if incremental and is_already_ingested(state, week, "trades"):
                log.info("[%d/%d] SKIP (already ingested) %s", wi, len(weeks), week.key)
                continue
            if not week.trades_dir.is_dir():
                log.debug("[%d/%d] SKIP (no T/ dir) %s", wi, len(weeks), week.key)
                continue

            log.info("[%d/%d] Processing trades: %s", wi, len(weeks), week.key)
            extraction = extract_rep_files(week.trades_dir, "FKL501R")
            if not extraction.rep_files and fail_fast:
                raise RuntimeError(f"Extraction failed for trades in {week.key} — aborting (fail-fast)")
            log.info("  Extracted %d REP file(s)", len(extraction.rep_files))
            for rep in extraction.rep_files:
                log.info("  Parsing %s ...", rep.name)
                date_tables = parse_trades_file(rep)
                for dt, table in date_tables.items():
                    if len(table) == 0:
                        continue
                    write_daily_parquet(table, "trades", dt)
                    _collect_contracts(table, global_contract_map)
                    _count_trades(table, daily_trades, dt)
                    trade_rows_total += len(table)
                    log.info("    %s -> %d rows", dt, len(table))

            remove_extracted_reps(extraction)
            mark_ingested(state, week, "trades")

        elapsed = time.time() - phase_start
        log.info("--- Phase 1 done: %d total trade rows, %d contracts so far (%.1fs) ---",
                 trade_rows_total, len(global_contract_map), elapsed)

        # --- Phase 2: Orders (contract-map extraction; Parquet only with --with-orders) ---
        log.info("=" * 60)
        if with_orders:
            log.info("=== Phase 2: Orders (full — writing Parquet) ===")
        else:
            log.info("=== Phase 2: Orders (contract-map only — no Parquet) ===")
        log.info("=" * 60)
        phase_start = time.time()
        order_rows_total = 0
        contracts_before = len(global_contract_map)
        for wi, week in enumerate(weeks, 1):
            if incremental and is_already_ingested(state, week, "orders"):
                log.info("[%d/%d] SKIP (already ingested) %s", wi, len(weeks), week.key)
                continue
            if not week.orders_dir.is_dir():
                log.debug("[%d/%d] SKIP (no O/ dir) %s", wi, len(weeks), week.key)
                continue

            log.info("[%d/%d] Processing orders: %s", wi, len(weeks), week.key)
            extraction = extract_rep_files(week.orders_dir, "FKL502R")
            if not extraction.rep_files and fail_fast:
                raise RuntimeError(f"Extraction failed for orders in {week.key} — aborting (fail-fast)")
            log.info("  Extracted %d REP file(s)", len(extraction.rep_files))
            for rep in extraction.rep_files:
                log.info("  Parsing %s ...", rep.name)
                file_date, table = parse_orders_file(rep)
                if file_date is None or len(table) == 0:
                    log.info("    -> empty or unparseable, skipping")
                    continue
                if with_orders:
                    write_daily_parquet(table, "orders", file_date)
                _collect_contracts(table, global_contract_map)
                order_rows_total += len(table)
                log.info("    %s -> %d rows", file_date, len(table))

            remove_extracted_reps(extraction)
            mark_ingested(state, week, "orders")

        new_contracts = len(global_contract_map) - contracts_before
        elapsed = time.time() - phase_start
        log.info("--- Phase 2 done: %d order rows scanned, %d new contracts from orders (%.1fs) ---",
                 order_rows_total, new_contracts, elapsed)

    valid_contracts = set(global_contract_map.keys())
    log.info("Built contract lookup: %d TA-35 monthly contracts", len(valid_contracts))

    # --- Phase 3: Quotes (underlying_index cleaned in-memory unless no_enrich) ---
    log.info("=" * 60)
    log.info("=== Phase 3: Quotes ===")
    log.info("=" * 60)
    phase_start = time.time()
    quote_rows_total = 0
    skipped: list[str] = []
    for wi, week in enumerate(weeks, 1):
        if incremental and is_already_ingested(state, week, "quotes"):
            log.info("[%d/%d] SKIP (already ingested) %s", wi, len(weeks), week.key)
            continue
        if not week.quotes_dir.is_dir():
            log.debug("[%d/%d] SKIP (no B/ dir) %s", wi, len(weeks), week.key)
            continue

        log.info("[%d/%d] Processing quotes: %s", wi, len(weeks), week.key)
        extraction = extract_rep_files(week.quotes_dir, "FKL504R")
        if not extraction.rep_files and fail_fast:
            raise RuntimeError(f"Extraction failed for quotes in {week.key} — aborting (fail-fast)")
        log.info("  Extracted %d REP file(s)", len(extraction.rep_files))
        for rep in extraction.rep_files:
            log.info("  Parsing %s ...", rep.name)
            try:
                file_date, table = parse_quotes_file(rep, valid_contracts, global_contract_map)
            except (TimeoutError, OSError) as exc:
                if fail_fast:
                    raise RuntimeError(f"Failed to read {rep.name} — aborting (fail-fast)") from exc
                log.error("  FAILED %s: %s", rep.name, exc)
                skipped.append(rep.name)
                continue
            if file_date is None or len(table) == 0:
                log.info("    -> empty or no matching contracts, skipping")
                continue
            if not no_enrich:
                df = table.to_pandas()
                df_in = df
                df = clean_spot_df(df_in)
                if df is not df_in:
                    table = pa.Table.from_pandas(df, preserve_index=False)
            write_daily_parquet(table, "quotes", file_date)
            quote_rows_total += len(table)
            log.info("    %s -> %d rows", file_date, len(table))

        remove_extracted_reps(extraction)
        mark_ingested(state, week, "quotes")

    elapsed = time.time() - phase_start
    log.info("--- Phase 3 done: %d total quote rows (%.1fs) ---", quote_rows_total, elapsed)
    if skipped:
        log.warning("Skipped %d files due to read errors: %s", len(skipped), skipped)

    # --- Finalize ---
    if incremental:
        save_ingest_state(state)

    log.info("=== Registering DuckDB views (ephemeral; pipeline Parquet) ===")
    con = register_duckdb_views(db_path=":memory:", parquet_root=PARQUET_DIR)

    verify_tables = ["trades", "quotes"]
    if with_orders:
        verify_tables.insert(1, "orders")
    for dt_name in verify_tables:
        try:
            count = con.execute(f"SELECT count(*) FROM {dt_name}").fetchone()[0]
            log.info("  %s: %d rows", dt_name, count)
        except Exception:
            log.info("  %s: no data yet", dt_name)

    con.close()

    total_elapsed = time.time() - pipeline_start
    log.info("=" * 60)
    log.info("Pipeline complete in %.1fs", total_elapsed)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collect_contracts(table: pa.Table, contract_map: dict[str, dict]) -> None:
    """Extract contract_number -> instrument info from a parsed table."""
    cn_col = table.column("contract_number").to_pylist()
    for i, cn in enumerate(cn_col):
        if cn in contract_map:
            continue
        contract_map[cn] = {
            "underlying": table.column("underlying")[i].as_py(),
            "option_type": table.column("option_type")[i].as_py(),
            "strike": table.column("strike")[i].as_py(),
            "expiry_code": table.column("expiry_code")[i].as_py(),
            "expiry_date": table.column("expiry_date")[i].as_py(),
        }


def _count_trades(table: pa.Table, daily_trades: dict, dt: datetime.date) -> None:
    cn_col = table.column("contract_number").to_pylist()
    for cn in cn_col:
        daily_trades[dt][cn] += 1


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="MAOF TA-35 ingestion pipeline")
    parser.add_argument("--years", type=int, nargs="+", help="Years to ingest (e.g. 2023)")
    parser.add_argument("--months", type=int, nargs="+", help="Months to ingest (1-12)")
    parser.add_argument("--incremental", action="store_true", help="Skip already-ingested weeks")
    parser.add_argument("--quotes-only", action="store_true",
                        help="Skip phases 1+2; load contract map from existing Parquet and run quotes only")
    parser.add_argument("--fail-fast", action="store_true",
                        help="Abort immediately on any extraction or parsing failure")
    parser.add_argument("--no-enrich", action="store_true",
                        help="Skip in-memory underlying_index cleaning for quotes (raw exchange values)")
    parser.add_argument("--with-orders", action="store_true",
                        help="Also write orders Parquet (off by default — only contract map is extracted)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S",
    ))
    handler.addFilter(lambda _: handler.flush() or True)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    ingest(
        years=args.years,
        months=args.months,
        incremental=args.incremental,
        quotes_only=args.quotes_only,
        fail_fast=args.fail_fast,
        no_enrich=args.no_enrich,
        with_orders=args.with_orders,
    )


if __name__ == "__main__":
    main()
