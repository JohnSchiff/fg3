# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Research data platform for historical **intraday tick-level derivatives data** from the **Tel Aviv Stock Exchange (TASE) MAOF** market, spanning 2014--2023.

A Python **ingestion pipeline** parses raw `.REP` files into Hive-partitioned Parquet + DuckDB, filtering for **TA-35 monthly options only**.

## Environment Setup

```bash
# Python 3.14 venv already exists at .venv/
source .venv/bin/activate
pip install -r requirements.txt
# unar is required for extracting .rar/.arj archives
brew install unar
```

**Dependencies:** duckdb, pyarrow, pandas

## Running the Pipeline

```bash
python -m src.ingest --years 2023              # full pipeline for one year
python -m src.ingest --years 2023 --months 1 2 3  # specific months
python -m src.ingest --years 2023 --quotes-only    # re-run quotes only
python -m src.ingest --years 2023 --incremental    # resume after interruption
python -m src.ingest --years 2023 --no-enrich      # quotes: keep raw exchange underlying_index
python -m src.ingest --years 2023 --with-orders    # also write orders Parquet (off by default)
python -m src.extract --years 2023 --dry-run       # preview REP cleanup
python -m src.extract --years 2023 --execute       # delete extracted REPs
```

See `INGESTION_GUIDE.md` for the full walkthrough and troubleshooting.

## Source Code Architecture

```
src/
├── __main__.py          # entry point (delegates to ingest.main)
├── config.py            # paths, Arrow schemas, filter constants, local data paths
├── discover.py          # traverses data/ tree, yields WeekFolder objects
├── extract.py           # extracts .rar/.arj via unar; REP cleanup CLI
├── ingest.py            # orchestrator: discover → extract → parse → write (3 phases)
├── storage.py           # writes Parquet, registers DuckDB views, sync_parquet_local, materialize_duckdb
├── enrich_spot.py       # cleaned underlying_index (shared by ingest + optional Parquet re-run)
├── spot_index.py        # MAD outlier filtering, ATM option selection, put-call parity spot cleaning
└── parsers/
    ├── instruments.py   # parses instrument names, resolves expiry dates, TA-35 filter
    ├── rep_prices.py    # REP monetary strings → whole-ILS int
    ├── trades.py        # parses FKL501R (weekly trade files) → dict[date, pa.Table]
    ├── orders.py        # parses FKL502R (daily order files) → (date, pa.Table)
    └── quotes.py        # parses FKL504R (daily quote files), 3/5-layer, bid+ask pairing
```

### Pipeline phases (in `ingest.py`)

1. **Trades** — parses `FKL501R`, builds contract-number lookup
2. **Orders** — parses `FKL502R`, extends contract lookup (contract-map extraction only; no Parquet written unless `--with-orders` is passed)
3. **Quotes** — parses `FKL504R`, uses contract lookup to filter/enrich (must run after 1+2); cleans `underlying_index` in memory before writing Parquet (skip with `--no-enrich`): **60s-window median imputation** for null/out-of-band raw prints, then **MAD/ATM/PCP**, then optional **clip** to official session range (see below)

### Key design patterns

- All parsers return PyArrow Tables; schemas defined in `config.py`
- **`strike`** and **ILS prices** (`trades` `price`; quotes `bid_price_*`, `ask_price_*`, `theoretical_price`) are **`int32`** (whole shekels). **`underlying_index`** stays **`float64`**.
- Contract-number-to-instrument mapping is built from trades+orders (orders scanned for contract map only, not persisted by default), then used to filter quotes (quotes lack instrument names)
- `spot_index.py` uses MAD outlier filtering, ATM option selection, and put-call parity to derive a cleaned spot index
- `enrich_spot.clean_spot_df` runs during ingestion (Phase 3) so quote Parquet is written with cleaned `underlying_index`; pass `--no-enrich` to keep raw exchange values. `python -m src.enrich_spot` re-processes existing quote Parquet files in place after algorithm changes.
- **Spot outlier repair (implemented in `enrich_spot.py`):** Before MAD/ATM/PCP, each daily file imputes **null** or **out-of-band** raw `underlying_index` from the **median of "sane" raw prints** on the same date with exchange time in **`(t_cs − 6000, t_cs]`** (60 seconds in centiseconds; `timestamp` is `HHMMSSCC`). **Sane** means finite and inside the **session low/high** for that calendar date when `data/ta35_daily_range.csv` has a row, otherwise inside **`SPOT_SANE_MIN`–`SPOT_SANE_MAX`** in `src/config.py` (defaults **800–4000**, matching the historical validation band). Constants: `UI_IMPUTE_WINDOW_CS` (= 6000). If a day has no sane raw prints, imputation cannot run for that day's bad rows; MAD/ATM/PCP and clipping still apply. Not in the REP parser.
- **TA-35 daily range CSV:** `data/ta35_daily_range.csv` (`date`, `low`, `high`) — optional official **session low/high** per calendar date. Used both to define **sane** raw values for the 60s imputation and, after MAD/ATM/PCP, to **clip** cleaned `underlying_index` to `[low, high]` when the date is listed. Missing dates or empty file: imputation uses the numeric sane band only; no clip that day. See `INGESTION_GUIDE.md` (Step 3b).
- `storage.py` includes `sync_parquet_local()` (optional: copy from legacy `output/parquet` into `~/fg3-data/parquet`) and `materialize_duckdb()` (builds standalone DuckDB from Parquet).

## Output Structure

### Parquet (local disk — not under the repo / OneDrive)

Ingestion writes Hive-partitioned Parquet to **`~/fg3-data/parquet/`** (`PARQUET_DIR` in `src/config.py`). Raw MAOF inputs may still live under the repo's `data/` (e.g. on OneDrive); derived Parquet is kept off the cloud sync tree.

```
~/fg3-data/parquet/
├── trades/year=YYYY/month=MM/trades_YYYYMMDD.parquet
└── quotes/year=YYYY/month=MM/quotes_YYYYMMDD.parquet
```

Orders Parquet is only written when `--with-orders` is passed (off by default).

### Local DuckDB (derived)

```
~/fg3-data/
├── maof.duckdb          # materialized DuckDB
└── parquet/             # same tree as above — source for materialization
```

Build or refresh the DB from Parquet:

```python
from src.storage import materialize_duckdb
materialize_duckdb(years=[2021])   # list every year you want inside the file
```

Files are zstd-compressed, Hive-partitioned. Materialized DuckDB contents depend on what you pass to `materialize_duckdb` and what years exist under `~/fg3-data/parquet/`.

### Querying

```python
import duckdb
from pathlib import Path

# From local materialized DuckDB (standalone, no OneDrive needed):
con = duckdb.connect(str(Path.home() / "fg3-data" / "maof.duckdb"), read_only=True)
df = con.execute("SELECT * FROM trades WHERE year = 2021 AND month = 3").fetchdf()

# From local Parquet (PARQUET_DIR = ~/fg3-data/parquet):
con = duckdb.connect(":memory:")
root = str(Path.home() / "fg3-data" / "parquet")
df = con.execute(f"""
    SELECT * FROM read_parquet('{root}/trades/**/*.parquet', hive_partitioning=true)
    WHERE date BETWEEN '2021-01-01' AND '2021-12-31'
""").fetchdf()

# Or register views over PARQUET_DIR:
from src.storage import register_duckdb_views
con = register_duckdb_views()  # creates trades, quotes views
```

## Raw Data Layout

Three core datasets live under `data/YYYY/MON_MM/DDD-DD/`:

| Subdir | File pattern | Granularity | Record layout |
|--------|-------------|-------------|---------------|
| `T/` | `FKL501R.REP` | Trades (per week) | 13 fields: date, base_asset_code, contract_number, instrument_name, time, sequence, flag, price, quantity, direction_flag, buy_order_id, sell_order_id, cross_flag |
| `O/` | `FKL502RYYYYMMDD.REP` | Orders (per day) | 17 fields: includes order_number, status (O/M/X), side (B/S), price, quantity, filled_qty, condition (IOC/blank), timestamps, cancel info |
| `B/` | `FKL504RYYYYMMDD.REP` | Quotes (per day) | Paired rows — Type 2 (bid) + Type 3 (ask), 3 or 5 depth layers depending on period (5 layers from June 2014+) |

**Supplemental files:**
- `data/boi_rate.csv` — Bank of Israel risk-free rate (forward-fill between observations)
- `ta35_monthly_expirations_2014_2025.csv` — TA-35 monthly option expiration dates
- `README_dir/` — TASE documentation PDFs (Hebrew)

## Parsing Gotchas

- **Compression:** Early 2014 uses `.arj` (lowercase filenames); mid-2014 onward uses `.rar` (uppercase). Extraction handled automatically by `extract.py` via `unar`.
- **Trade files start with 2 blank lines** before the header — skip leading whitespace.
- **Headers:** `MAOF-TRADES,DDMMYYYY,...` (note: header dates are DDMMYYYY, but record dates are YYYYMMDD).
- **All numeric fields are zero-padded, fixed-width** within CSV: prices have 2 decimals, quantities are 6 digits, timestamps are HHMMSSCC (centisecond precision).
- **Quote depth changed** in June 2014 from 3 to 5 price layers per side — column count varies.
- **Instrument name encoding:** `T25 C00001 AUG4` = TA-25 Call, strike identifier 00001, August 2014. Two naming conventions exist (old: space-separated, new: dash-separated like `T35-C001720M301`); both handled by `parsers/instruments.py`.
- **OneDrive I/O:** Reading large REPs from synced folders can cause `TimeoutError`; the quote parser has retry logic (3 attempts, 10s delay). The `sync_parquet_local` function in `storage.py` also has retry logic for copying Parquet files.

## Trading Calendar

Israeli market trades **Sunday--Thursday**. Friday/Saturday and Jewish holidays are non-trading days.

## Full Data Documentation

See `DATA_GUIDE.MD` for complete field-by-field record layouts, sample rows, volume estimates, and reference table descriptions.
