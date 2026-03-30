# Data Ingestion Guide

How to ingest a year of TASE MAOF data (trades + quotes) into the
Parquet/DuckDB pipeline.  Orders are parsed for contract-number mapping
only (no Parquet written unless `--with-orders` is passed).  Applies to
any year with raw data under `data/` (currently 2014-2023).

---

## Prerequisites

| Requirement | Check |
|-------------|-------|
| Python 3.10+ with venv | `python3 --version` |
| `unar` CLI (extracts RAR + ARJ) | `which unar` — install with `brew install unar` |
| Raw data present | `ls data/YYYY/` should show `MON_01/` through `MON_12/` |
| Python venv activated | `source .venv/bin/activate` |
| Packages installed | `pip install -r requirements.txt` |

---

## Quick Start

Replace `YYYY` with the target year (e.g. 2022).

> **Running via AI agent (Cursor / Claude Code):**  The pipeline must
> be launched with `nohup` and the sandbox **disabled** (`--no-sandbox`
> or `required_permissions: ["all"]`).  OneDrive cloud-storage paths
> trigger OS-level timeouts (`[Errno 60]`) inside sandboxed shells, and
> long-running phases will be killed by the default agent timeout.
>
> ```bash
> nohup .venv/bin/python -m src.ingest --years YYYY -v > /tmp/ingest_YYYY.log 2>&1 &
> ```
>
> Monitor with `tail -f /tmp/ingest_YYYY.log`.

**From a regular terminal:**

```bash
source .venv/bin/activate
python -m src.ingest --years YYYY
```

This single command:

1. **Discovers** all week-folders under `data/YYYY/MON_*/D*/`
2. **Extracts** `.rar` / `.arj` archives on the fly using `unar`
3. **Parses** each REP file, filtering for TA-35 monthly options only
4. **Writes** daily Parquet files to **`~/fg3-data/parquet/{trades,quotes}/year=YYYY/month=MM/`**
   (local disk — not under the repo; avoids refilling OneDrive with Parquet).
   (Orders are scanned for contract-number mapping but not persisted.)
5. **Registers** DuckDB views over the Parquet files

Expect roughly **25-40 minutes** per year depending on disk speed
(OneDrive-synced folders are slower due to network I/O).  To speed up
Phase 3, pass `--no-enrich` and run spot enrichment separately
afterwards with `python -m src.enrich_spot --years YYYY` (time scales with
year size and disk I/O).

---

## Step-by-Step Walkthrough

### Step 1 — Verify the raw data exists

```bash
ls data/YYYY/
# Expected: MON_01  MON_02  ...  MON_12

ls data/YYYY/MON_01/D*/T/
# Expected: FKL501R.rar  (or FKL501R*.REP if already extracted)
```

Each week-folder (`D01-05/`, `D08-12/`, etc.) should contain up to three
subdirectories:

| Dir | Contents | File pattern |
|-----|----------|--------------|
| `T/` | Trades | `FKL501R.rar` or `.arj` or `.REP` |
| `O/` | Orders | `FKL502R.rar` or `.arj` or `.REP` |
| `B/` | Quotes | `FKL504R.rar` or `.arj` or `.REP` |

Note: early 2014 uses `.arj` archives with lowercase filenames; mid-2014
onward uses `.rar` with uppercase.  The pipeline handles both
automatically.

### Step 2 — Run the ingestion pipeline

```bash
source .venv/bin/activate
python -m src.ingest --years YYYY
```

The pipeline runs three phases in order:

- **Phase 1 — Trades** (fastest, ~1-2 min):
  Parses `FKL501R` files.  One weekly file produces multiple daily Parquet
  files.  Also builds the contract-number lookup table needed for Phase 3.

- **Phase 2 — Orders** (contract-map only, ~2-5 min):
  Parses `FKL502R` files to extend the contract-number lookup with
  instruments that appear in orders but have no trades.  **No Parquet is
  written** by default.  Pass `--with-orders` to also persist orders to
  Parquet (~155M rows/year).

- **Phase 3 — Quotes** (medium, ~10-15 min without enrichment, ~25 min with):
  Parses `FKL504R` files.  Uses the contract lookup from Phases 1+2 to
  filter and enrich quote rows (quotes don't contain instrument names, only
  contract numbers).  By default, `clean_spot_df()` runs in-memory on each
  daily file to clean `underlying_index` (see Step 3b for the full order:
  60s-window imputation, then MAD + ATM + PCP, then optional session clip).
  Pass `--no-enrich` to skip this and run `python -m src.enrich_spot --years YYYY`
  afterward (post-processing time depends on disk and year size).

**Important:** Phases 1+2 must complete before Phase 3, because quotes
require the contract-number-to-instrument mapping built from trades and
orders data.

### Step 3 — Verify the output

```bash
# Check that Parquet files were created
ls ~/fg3-data/parquet/trades/year=YYYY/
ls ~/fg3-data/parquet/quotes/year=YYYY/

# Quick row counts via DuckDB (replace YYYY)
python3 -c "
import duckdb
from pathlib import Path
con = duckdb.connect(':memory:')
root = Path.home() / 'fg3-data' / 'parquet'
for dt in ('trades', 'quotes'):
    glob = str(root / dt / 'year=YYYY' / '**' / '*.parquet')
    r = con.execute(f\"\"\"
        SELECT count(*) as rows,
               count(distinct date) as days,
               min(date) as first_date,
               max(date) as last_date
        FROM read_parquet('{glob}', hive_partitioning=true)
    \"\"\").fetchone()
    print(f'{dt:8s}: {r[0]:>12,} rows | {r[1]:>3} days | {r[2]} to {r[3]}')
"
```

### Step 3b — TA-35 spot (`underlying_index`): outliers and stale prints

**What ingest does (order of operations).** Phase 3 runs `clean_spot_df()` on each **daily** quotes table before Parquet write (unless `--no-enrich`). All of this lives in `src/enrich_spot.py` — **post-parse, pre-Parquet**, not inside the REP reader.

1. **Raw imputation (60-second window).** Rows whose raw `underlying_index` is **null** or **outside the sane band** are overwritten with a **consensus value** derived from other rows the same day. Exchange time uses **`timestamp`** as **HHMMSSCC**; convert to centiseconds from midnight  
   `t_cs = HH*360000 + MM*6000 + SS*100 + CC` (zero-pad to 8 digits). For each centisecond bucket, build the median of **sane** raw prints; roll a **60s** window (**`UI_IMPUTE_WINDOW_CS` = 6000** centiseconds in `src/config.py`) on that series and assign the rolled median to timestamps that need repair. **Sane** raw value = finite and inside **`[low, high]`** for that calendar **`date`** when `data/ta35_daily_range.csv` has a row; otherwise inside **`SPOT_SANE_MIN`–`SPOT_SANE_MAX`** (defaults **800–4000**). If the whole day has no sane raw prints, this step cannot fill gaps (later layers still run).

2. **MAD + ATM + PCP** — same as before: per-timestamp robust spot, forward-fill across the day.

3. **Clip to official session range (optional).** If **`date`** appears in `ta35_daily_range.csv`, clamp `underlying_index` to **`[low, high]`**. Omitted dates: no clip that day.

**Official session low / high (optional CSV).** Same idea as `data/boi_rate.csv`: maintain `data/ta35_daily_range.csv` with columns **`date`**, **`low`**, **`high`** (TA-35 **cash index** session low and high for that calendar date). The repo copy can be **refreshed from Yahoo Finance** (`TA35.TA` via `yfinance`: daily **Low** / **High**, e.g. `start=2014-01-01` through today to align with MAOF years); that is convenient but **not** the same as TASE’s own published session range — use TASE data if you need an authoritative match to exchange rules. Duplicate dates keep the **last** row. Empty or missing file: imputation uses only the numeric sane band; no clipping.

**Applying changes to existing Parquet.** After pulling code or CSV updates, run `python -m src.enrich_spot --years YYYY` (or `--file` / `--months`) to rewrite quote Parquet in place; then re-materialize DuckDB if you use `~/fg3-data/maof.duckdb`.

**Manual validation (investigation).** For a **suspect** row you can still compare to a hand-built 60s lookback: same `date`, same `t_cs` math; restrict to a plausible index band so spikes do not dominate the median. The **pipeline** uses the inclusive window **`(t_cs_row − 6000, t_cs_row]`** on sane raw values (implementation detail may differ slightly from a strict “strictly before this row only” diagnostic query). On real bad days (e.g. 2020-05-07), absurd prints often sit next to windows whose median is near the real index (~1.4k).

**What agents should do.**

- **Populate `data/ta35_daily_range.csv`** for years you care about so both **sane** detection and **final clipping** align with the cash index session.
- **Research / strategies** that depend on spot: prefer enriched Parquet; optional extra filters (winsorize, moneyness gates) remain valid defense in depth.
- **Quick SQL sanity:** global `min`/`max`/`quantile_cont` on `underlying_index` for a year often exposes tails; pair with a 60s band check when drilling into specific timestamps.

### Step 4 — Reclaim disk space (optional)

After a successful ingestion, the extracted `.REP` files take up a lot of
space (~100+ GB per year).  You can safely delete them since they can be
re-extracted from the `.rar`/`.arj` archives on demand:

```bash
# Preview what would be deleted (dry run)
python -m src.extract --years YYYY --dry-run

# Actually delete (only archive-backed REP files)
python -m src.extract --years YYYY --execute
```

This only deletes `.REP` files that sit next to a `.rar` or `.arj` archive
with a matching name.  It will never touch archives themselves or REP files
that have no corresponding archive.

---

## Common Variations

### Ingest only specific months

```bash
python -m src.ingest --years YYYY --months 1 2 3
```

### Ingest only quotes (if trades + orders already done)

If you already ran Phases 1+2 and just need to re-run quotes (e.g., after
a timeout or partial failure):

```bash
python -m src.ingest --years YYYY --quotes-only
```

This skips Phases 1 and 2 entirely and builds the contract lookup from the
**existing Parquet files** instead.  Requires that trades Parquet files
already exist under `~/fg3-data/parquet/` (or wherever `PARQUET_DIR` points).

### Incremental mode (resume after interruption)

```bash
python -m src.ingest --years YYYY --incremental
```

Tracks which week-folders have already been processed in
`ingest_state.json` and skips them.  Useful if the pipeline was interrupted
mid-way.

### Verbose logging

```bash
python -m src.ingest --years YYYY -v
```

### Multiple years at once

```bash
python -m src.ingest --years 2021 2022 2023
```

---

## Troubleshooting

### `TimeoutError: [Errno 60] Operation timed out`

This happens when reading archive files from OneDrive-synced folders.
Files on OneDrive may be cloud-only placeholders (`blocks=0`) that
haven't been downloaded yet.  The OS-level read times out instantly
before the pipeline's retry logic can help.

**Before running the pipeline**, force OneDrive to download the data:

1. Open Finder, navigate to `data/YYYY/`
2. Right-click → **Always Keep on This Device**
3. Wait for all files to hydrate.  Verify with:
   ```bash
   cd data/YYYY
   total=0; ready=0
   for f in $(find . -name "*.rar" -o -name "*.arj"); do
     total=$((total+1))
     b=$(stat -f "%b" "$f" 2>/dev/null)
     [ "$b" -gt 0 ] && ready=$((ready+1))
   done
   echo "$ready / $total files hydrated"
   ```
4. Only start the pipeline once all files show `blocks > 0`.

If individual months get stuck, right-click each `MON_XX` folder
separately and re-apply "Always Keep on This Device".

Other solutions:
- **Copy the year folder to local disk** first for faster I/O.
- Use `--quotes-only` to re-run just the failed phase after syncing.

### `unar: command not found`

```bash
brew install unar
```

### Pipeline interrupted mid-way

Re-run with `--incremental` to skip already-processed weeks:

```bash
python -m src.ingest --years YYYY --incremental
```

Or if you prefer a clean start, delete the partial output and re-run:

```bash
rm -rf ~/fg3-data/parquet/trades/year=YYYY
rm -rf ~/fg3-data/parquet/quotes/year=YYYY
python -m src.ingest --years YYYY
```

### Missing data for some days

Some week-folders may lack a `T/`, `O/`, or `B/` subdirectory — this is
normal (not all weeks have all three data types in the raw data).  The
pipeline logs warnings and continues.

### Bad or stale `underlying_index` on quotes

If quantiles or spot-dependent strategies look wrong after ingest, see
**Step 3b** (60s imputation, sane band, CSV clip, and how to re-run
`python -m src.enrich_spot`). Ensure `ta35_daily_range.csv` covers your
dates and that quote Parquet was built or re-enriched with a current
`enrich_spot.py`.

---

## Output Structure

After ingestion, Parquet lives under **`~/fg3-data/parquet/`** with Hive-style
partitioning:

```
~/fg3-data/parquet/
├── trades/
│   └── year=YYYY/
│       ├── month=01/
│       │   ├── trades_YYYYMMDD.parquet
│       │   └── ...
│       ├── month=02/
│       └── ...
└── quotes/
    └── year=YYYY/
        └── month=01/
            ├── quotes_YYYYMMDD.parquet
            └── ...
```

Orders Parquet appears under `orders/` only when ingested with `--with-orders`.

Each file contains **one trading day** of data for one data type,
compressed with zstd.  Years accumulate side-by-side — ingesting 2022 does
not affect existing 2023 data.

---

## Querying the Data

After ingestion, query directly with DuckDB:

```python
import duckdb
from pathlib import Path

con = duckdb.connect(":memory:")
pq = Path.home() / "fg3-data" / "parquet"

# Read a specific year + month
df = con.execute(f"""
    SELECT *
    FROM read_parquet('{pq}/trades/year=2022/month=03/*.parquet',
                      hive_partitioning=true)
    WHERE option_type = 'C' AND strike = 1600
    LIMIT 100
""").fetchdf()

# Read across all ingested years at once
df = con.execute(f"""
    SELECT *
    FROM read_parquet('{pq}/trades/**/*.parquet',
                      hive_partitioning=true)
    WHERE date BETWEEN '2022-01-01' AND '2023-12-31'
""").fetchdf()
```

Or register views for all available data:

```python
from src.storage import register_duckdb_views

con = register_duckdb_views()
# Now you can query across all ingested years:
#   SELECT * FROM trades WHERE date >= '2022-01-01'
#   SELECT * FROM quotes WHERE ...
```

---

## Full Command Reference

| Command | What it does |
|---------|-------------|
| `python -m src.ingest --years YYYY` | Full pipeline for one year |
| `python -m src.ingest --years 2021 2022 2023` | Full pipeline for multiple years |
| `python -m src.ingest --years YYYY --months 1 2 3` | Only specific months |
| `python -m src.ingest --years YYYY --quotes-only` | Re-run quotes only (needs existing trades Parquet) |
| `python -m src.ingest --years YYYY --incremental` | Resume after interruption |
| `python -m src.ingest --years YYYY --with-orders` | Also write orders Parquet (~155M rows/year) |
| `python -m src.ingest --years YYYY -v` | Verbose debug logging |
| `python -m src.extract --years YYYY --dry-run` | Preview REP file cleanup |
| `python -m src.extract --years YYYY --execute` | Delete extracted REP files to save space |
