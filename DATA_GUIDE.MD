# DATA GUIDE -- TASE MAOF Options Research Platform

## 1. Overview

This repository contains historical **intraday tick-level derivatives data** from the
**Tel Aviv Stock Exchange (TASE) MAOF** market, spanning **2014 through 2023** (10 calendar years).

The dataset is composed of:

| Category | Description |
|----------|-------------|
| **Trades** (`T/`) | Every executed trade on the MAOF derivatives market |
| **Orders** (`O/`) | Full order-book lifecycle: submissions, modifications, cancellations, fills |
| **Quotes** (`B/`) | Best bid/ask snapshots with up to 5 price-depth layers |
| **BOI Rate** | Bank of Israel policy interest-rate time series |
| **Expirations** | Monthly MAOF option settlement/expiration calendar |

All original documentation from TASE is stored as Hebrew-language PDFs in `README_dir/`.

---

## 2. Directory Layout

```
FG3/
├── DATA_GUIDE.MD                       # this file
├── ta35_monthly_expirations_2014_2025.csv  # monthly option expiration dates
├── data/
│   ├── boi_rate.csv                    # Bank of Israel interest rate
│   ├── 2014/                           # one folder per calendar year
│   │   ├── MON_01/ ... MON_12/         # one folder per calendar month
│   │   │   ├── D01-05/                 # one folder per trading-week (day range)
│   │   │   │   ├── T/                  #   Trades  (FKL501R)
│   │   │   │   ├── O/                  #   Orders  (FKL502R)
│   │   │   │   └── B/                  #   Quotes  (FKL504R)
│   │   │   ├── D08-12/
│   │   │   │   └── ...
│   │   │   └── ...
│   ├── 2015/
│   │   └── ...
│   └── 2023/
│       └── ...
└── README_dir/                         # TASE documentation PDFs (Hebrew)
    ├── הסבר למבנה המאגר ההיסטורי/      # archive structure explanation
    ├── טבלאות/                          # reference lookup tables
    ├── סוגי חומר/                       # data-type specifications (SH78/98/160/163/165)
    ├── פקודות ציטוטים עסקאות מעוף/      # MAOF orders/quotes/trades format
    ├── פקודות ציטוטים עסקאות רצף/       # continuous-trading format
    └── מבנה ציטוטים ופקודות .../        # versioned format specs (post-2018)
```

### Naming conventions

| Pattern | Meaning |
|---------|---------|
| `YYYY/` | Calendar year (2014--2023) |
| `MON_MM/` | Calendar month, zero-padded (01--12) |
| `DDD-DD/` | Day range within the month (e.g. `D01-05`, `D08-12`) |
| `T/`, `O/`, `B/` | Trades, Orders, Quotes (Best bid/ask) |

---

## 3. MAOF Trades -- `T/FKL501R`

**One file per trading week** containing every executed trade across all MAOF instruments.

### File naming

| Pattern | Example |
|---------|---------|
| `FKL501R.REP` | Uncompressed (some weeks in 2014) |
| `FKL501R.rar` | RAR archive (most weeks) |
| `fkl501r.arj` | ARJ archive (early 2014 months) |

### Header

```
MAOF-TRADES,DDMMYYYY,DDMMYYYY,00,00000000,000000,000000
```

The two dates are the start and end of the trading week.

### Record layout

All fields are comma-separated and zero-padded.

| # | Field | Example | Description |
|---|-------|---------|-------------|
| 1 | `date` | `20140727` | Trade date (YYYYMMDD) |
| 2 | `base_asset_code` | `01` | Underlying asset group code (see Table 602) |
| 3 | `contract_number` | `81148371` | Unique TASE contract identifier |
| 4 | `instrument_name` | `T25 C00001 AUG4` | Human-readable instrument string |
| 5 | `time` | `09504649` | Execution time (HHMMSSCC -- hours, minutes, seconds, centiseconds) |
| 6 | `sequence` | `003245` | Intraday sequence number |
| 7 | `flag` | `0` or `1` | Trade type flag |
| 8 | `price` | `139500.00` | Trade price (points for index options) |
| 9 | `quantity` | `000001` | Number of contracts |
| 10 | `direction_flag` | `0` or `1` | Aggressor side indicator |
| 11 | `buy_order_id` | `00034096` | Matched buy order reference |
| 12 | `sell_order_id` | `00036559` | Matched sell order reference |
| 13 | `cross_flag` | `0` or `1` | Cross-trade indicator |

### Sample row

```
20140727,01,81148371,T25 C00001 AUG4,09504649,003245,0,139500.00,000001,0,00034096,00036559,0
```

### Instrument name encoding

```
T25 C00001 AUG4
│   │      └── expiry: AUG 2014
│   └── C = Call (P = Put), strike identifier
└── T25 = TA-25 index
```

### Volume

Approximately **208,000 rows per weekly file** (varies by market activity).

---

## 4. MAOF Orders -- `O/FKL502R`

**One file per trading day** containing the full order lifecycle for every MAOF instrument.

### File naming

| Pattern | Example |
|---------|---------|
| `FKL502RYYYYMMDD.REP` | Uncompressed, one per day |
| `FKL502R.rar` | RAR archive (one per week, contains daily files) |
| `fkl502r.arj` | ARJ archive (early 2014) |

### Header

```
MAOF-ORDERS,YYYYMMDD,YYYYMMDD,00,00000000,000000,000000
```

### Record layout

| # | Field | Example | Description |
|---|-------|---------|-------------|
| 1 | `date` | `20140727` | Order date (YYYYMMDD) |
| 2 | `base_asset_code` | `01` | Underlying asset group code |
| 3 | `contract_number` | `81148371` | TASE contract identifier |
| 4 | `instrument_name` | `T25 C00001 AUG4` | Human-readable instrument string |
| 5 | `order_number` | `00034054` | Unique order ID for the day |
| 6 | `status` | `X ` | Order status (see below) |
| 7 | `side` | `B` | `B` = Buy, `S` = Sell |
| 8 | `price` | `000100.00` | Limit price |
| 9 | `quantity` | `000001` | Total order quantity |
| 10 | `filled_qty` | `000000` | Quantity already filled |
| 11 | `condition` | `IOC` | Time-in-force / order condition |
| 12 | `timestamp` | `09495981` | Event time (HHMMSSCC) |
| 13 | `ref_order_1` | `00000000` | Reference to prior order (amend chain) |
| 14 | `ref_order_2` | `00034096` | Reference to related order |
| 15 | `cancel_reason` | `C` | Reason code for cancellation |
| 16 | `cancel_time` | `09500462` | Time of cancellation (HHMMSSCC) |
| 17 | `extra_flag` | `J` | Additional flags (optional, trailing) |

### Order status codes

| Code | Meaning |
|------|---------|
| `O` | Open (resting on book) |
| `M` | Matched (filled) |
| `X` | Cancelled |

### Order condition codes

| Code | Meaning |
|------|---------|
| `IOC` | Immediate-Or-Cancel |
| `   ` | (blank) Standard limit order |

### Sample row

```
20140727,01,81148371,T25 C00001 AUG4,00034054,X ,B,000100.00,000001,000000,   ,09495981,00000000,00034096,C,09500462,
```

### Volume

Approximately **486,000 rows per daily file** (varies by market activity).

---

## 5. MAOF Quotes -- `B/FKL504R`

**One file per trading day** containing best-bid/ask snapshots for every MAOF instrument.

### File naming

| Pattern | Example |
|---------|---------|
| `FKL504RYYYYMMDD.REP` | Uncompressed, one per day |
| `FKL504R.rar` | RAR archive |
| `fkl504r.arj` | ARJ archive (early 2014) |

### Header

```
MAOF-QUOTES,YYYYMMDD,YYYYMMDD,00,00000000,000000,000000
```

### Record types

Each quote update produces a **pair** of consecutive rows: Type 2 (bid side) followed by Type 3 (ask side).

#### Type 2 -- Bid side

| # | Field | Example | Description |
|---|-------|---------|-------------|
| 1 | `record_type` | `2` | Always `2` |
| 2 | `date` | `20140727` | Quote date (YYYYMMDD) |
| 3 | `timestamp` | `09495981` | Snapshot time (HHMMSSCC) |
| 4 | `base_asset_code` | `01` | Underlying asset group |
| 5 | `contract_number` | `81148371` | TASE contract identifier |
| 6 | `bid_price_1` | `000100.00` | Best bid price (layer 1) |
| 7 | `bid_price_2` | `000000.00` | Bid price layer 2 |
| 8 | `bid_price_3` | `000000.00` | Bid price layer 3 |
| 9 | `bid_qty_1` | `0000001` | Bid quantity layer 1 |
| 10 | `bid_qty_2` | `0000000` | Bid quantity layer 2 |
| 11 | `bid_qty_3` | `0000000` | Bid quantity layer 3 |
| 12 | `theoretical_price` | `139678.00` | TASE-calculated theoretical price |
| 13 | `last_trade_flag` | ` ` or `L` | `L` = last trade used for settlement |
| 14 | `volume` | `0000000` | Cumulative volume |
| 15 | `extra_flag` | `J` | Additional flags (optional, trailing) |

#### Type 3 -- Ask side

| # | Field | Example | Description |
|---|-------|---------|-------------|
| 1 | `record_type` | `3` | Always `3` |
| 2 | `date` | `20140727` | Quote date |
| 3 | `timestamp` | `09495981` | Same timestamp as paired Type 2 |
| 4 | `base_asset_code` | `01` | Underlying asset group |
| 5 | `contract_number` | `81148371` | TASE contract identifier |
| 6 | `ask_price_1` | `000000.00` | Best ask price (layer 1) |
| 7 | `ask_price_2` | `000000.00` | Ask price layer 2 |
| 8 | `ask_price_3` | `000000.00` | Ask price layer 3 |
| 9 | `ask_qty_1` | `0000000` | Ask quantity layer 1 |
| 10 | `ask_qty_2` | `0000000` | Ask quantity layer 2 |
| 11 | `ask_qty_3` | `0000000` | Ask quantity layer 3 |
| 12 | `settlement_price` | `000000.00` | Settlement/closing price |

### Quote depth history

| Period | Layers |
|--------|--------|
| Before June 2014 | 3 price layers per side |
| From June 2014 onward | 5 price layers per side |

When 5 layers are present, fields 6--8 and 9--11 extend to include layers 4 and 5 (two additional price and quantity columns each).

### Sample rows (paired)

```
2,20140727,09495981,01,81148371,000100.00,000000.00,000000.00,0000001,0000000,0000000,139678.00, ,0000000,
3,20140727,09495981,01,81148371,000000.00,000000.00,000000.00,0000000,0000000,0000000,000000.00
```

### Volume

Approximately **1,069,000 rows per daily file** (~110--144 MB uncompressed). This is the largest dataset by far.

---

## 6. Bank of Israel Interest Rate -- `data/boi_rate.csv`

A step-function time series of the BOI policy rate, used as the **risk-free rate** for options pricing.

| Column | Format | Description |
|--------|--------|-------------|
| `date` | `YYYY-MM-DD` | Effective date of rate change |
| `rate` | Decimal | Annual rate (e.g. `0.0100` = 1.00%) |

**22 observations** from 2014-01-01 (1.00%) through 2023-04-03 (4.75%).

The rate is constant between observations -- forward-fill to obtain a daily series.

### Sample

```csv
date,rate
2014-01-01,0.0100
2014-02-24,0.0075
2014-09-01,0.0025
2015-02-23,0.0010
...
2023-04-03,0.0475
```

---

## 7. Option Expiration Calendar -- `ta35_monthly_expirations_2014_2025.csv`

Monthly TA-35 option settlement (expiration) dates. In Hebrew financial terminology, *pkiya* (פקיעה) means expiration.

| Column | Format | Description |
|--------|--------|-------------|
| `expiry_date` | `YYYY-MM-DD` | Exact expiration date |
| `year` | Integer | Calendar year |
| `month` | Integer | Calendar month (1--12) |

**144 rows** covering January 2014 through December 2025.

### Sample

```csv
expiry_date,year,month
2014-01-23,2014,1
2014-02-27,2014,2
2014-03-27,2014,3
2025-11-27,2025,11
```

---

## 8. Compressed Archives

Most data weeks are delivered as compressed archives that must be extracted before parsing.

| Format | Extension | Period | Filenames |
|--------|-----------|--------|-----------|
| ARJ | `.arj` | Early 2014 (MON_01 -- MON_05) | Lowercase: `fkl501r.arj` |
| RAR | `.rar` | Mid-2014 onward through 2023 | Uppercase: `FKL501R.rar` |

After extraction, the resulting `.REP` files follow the layouts described in Sections 3--5.

---

## 9. Reference Tables (from `README_dir/`)

The `README_dir/טבלאות/` folder contains PDF lookup tables from TASE. Key tables:

| Table | Name (Hebrew) | Content |
|-------|---------------|---------|
| **602** | נכסי בסיס מעוף | MAOF base assets and codes: TA-25 (`T25`), TA-35 (`T35`), USD (`DLR`), Banks (`BNK`), Euro (`EUR`), and individual stocks (ICL, POL, LUM, TVA, BZQ, etc.) with active-date ranges |
| **519** | שווקי רצף | Continuous-trading market codes: Shares (2), Bonds (4), Makam (4), Institutional (3), MTS (5), Repo (6) |
| **7** | סוגי גזירה | Derivation types: all securities (77), equities (77), bonds (07), government-only (00) |

### Data-type specifications (SH documents)

| Document | Content |
|----------|---------|
| **SH78** | TASE index values (periodic) |
| **SH98** | MAOF derivatives daily summaries (contract details, settlement, open interest) |
| **SH160** | End-of-day trading data for equities and bonds |
| **SH163** | Mutual fund NAV and composition data |
| **SH165** | Corporate actions: dividends, rights issues, stock splits, capital changes |

---

## 10. Ingestion Status (Parquet / DuckDB)

Raw data spans **2014--2023** (all 12 months present for every year). The Python
ingestion pipeline filters for **TA-35 monthly options only** and writes
Hive-partitioned Parquet under `output/parquet/`. Below is the current state.

### Fully ingested (all three data types, all 12 months)

| Year | Trades (files) | Orders (files) | Quotes (files) | Notes |
|------|---------------|----------------|----------------|-------|
| **2021** | 243 | 244 | 244 | Complete. Sep has ~13 trading days (Jewish holidays). |

### Partially ingested

| Year | Trades | Orders | Quotes | Issues |
|------|--------|--------|--------|--------|
| **2022** | 242 files, 12 months -- **complete** | 234 files, 12 months -- looks complete (Nov/Dec have 16 days, likely holidays + shorter months) | **153 files, very patchy** -- several months have far fewer files than expected (Feb: 5, Apr: 5, Jun: 10, Sep: 7, Nov: 10) | Quotes need re-ingestion |
| **2023** | 168 files, months 01--10 only; Aug has only 3 files; **Nov--Dec missing** | 217 files, 12 months but Nov (6) and Dec (5) are very incomplete | 207 files, 12 months but Nov (1) and Dec (4) are very incomplete | Trades Aug + Nov--Dec, and Orders/Quotes Nov--Dec need re-ingestion |

### Not yet ingested

| Year | Raw data available |
|------|--------------------|
| **2014** | 12 months |
| **2015** | 12 months |
| **2016** | 12 months |
| **2017** | 12 months |
| **2018** | 12 months |
| **2019** | 12 months |
| **2020** | 12 months |

**Summary:** 3 of 10 years have been run through the pipeline (2021--2023).
Only 2021 is fully complete across all data types. 2022 and 2023 have gaps
(especially quotes in 2022, and trades/orders/quotes in late 2023). Years
2014--2020 have raw data on disk but have not been ingested yet.

---

## 11. Key Notes for Researchers

### Trading calendar

The Israeli market trades **Sunday through Thursday**. Friday and Saturday are non-trading days. Jewish holidays also close the market.

### Date format inconsistencies

| Source | Format | Example |
|--------|--------|---------|
| REP file records | `YYYYMMDD` | `20140727` |
| Trade header dates | `DDMMYYYY` | `27072014` |
| `ta35_monthly_expirations_2014_2025.csv` | `YYYY-MM-DD` | `2023-01-26` |
| `boi_rate.csv` | `YYYY-MM-DD` | `2023-04-03` |

### Numeric formatting

All numeric fields in `.REP` files are **zero-padded** and **fixed-width** within their comma-separated positions:
- Prices: `139500.00` (always 2 decimal places)
- Quantities: `000001` (6 digits)
- Timestamps: `09504649` (HHMMSSCC, 8 digits)

### File sizes (typical uncompressed)

| File type | Per-file rows | Size |
|-----------|---------------|------|
| Trades (`FKL501R`) | ~208K / week | ~28 MB |
| Orders (`FKL502R`) | ~486K / day | ~58--76 MB |
| Quotes (`FKL504R`) | ~1.07M / day | ~110--144 MB |

### Blank lines

Trade files (`FKL501R.REP`) begin with two blank lines before the header. Parsers should skip leading whitespace/empty lines.

---

## 12. Quick-Start: Reading a Trade File in Python

```python
import csv
from pathlib import Path

rep_path = Path("data/2014/MON_07/D27-31/T/FKL501R.REP")

with open(rep_path, "r") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("MAOF-TRADES"):
            continue
        fields = line.split(",")
        date        = fields[0]   # YYYYMMDD
        base_code   = fields[1]   # e.g. "01"
        contract    = fields[2]   # e.g. "81148371"
        instrument  = fields[3]   # e.g. "T25 C00001 AUG4"
        time        = fields[4]   # HHMMSSCC
        price       = float(fields[7])
        qty         = int(fields[8])
        # ... process trade
```
