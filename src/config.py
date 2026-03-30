"""Central configuration: paths, Arrow schemas, and constants."""

from pathlib import Path
import pyarrow as pa

# ---------------------------------------------------------------------------
# Paths (all relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
INGEST_STATE_PATH = PROJECT_ROOT / "ingest_state.json"

# Standalone local store (NOT under the repo / OneDrive).
LOCAL_DATA_DIR = Path.home() / "fg3-data"
LOCAL_PARQUET_DIR = LOCAL_DATA_DIR / "parquet"
DUCKDB_PATH = LOCAL_DATA_DIR / "maof.duckdb"      # materialized from Parquet

# Pipeline reads/writes Parquet here only (local disk). Ingest does not persist Parquet under output/.
PARQUET_DIR = LOCAL_PARQUET_DIR
# Old layout (repo output/parquet on OneDrive). One-time: sync_parquet_local(src=LEGACY_PARQUET_DIR).
LEGACY_PARQUET_DIR = OUTPUT_DIR / "parquet"

BOI_RATE_CSV = DATA_DIR / "boi_rate.csv"
# Official TA-35 cash index session low/high per calendar date (optional).
# When populated, quote enrichment clamps cleaned underlying_index to [low, high].
TA35_DAILY_RANGE_CSV = DATA_DIR / "ta35_daily_range.csv"
EXPIRATIONS_CSV = PROJECT_ROOT / "ta35_monthly_expirations_2014_2025.csv"

# ---------------------------------------------------------------------------
# Filtering constants
# ---------------------------------------------------------------------------
TA35_BASE_ASSET_CODE = "01"
TA35_INSTRUMENT_PREFIXES = ("T25", "T35")
MIN_FUTURES_STRIKE = 2  # strike <= 1 means futures, not options

# ---------------------------------------------------------------------------
# Spot-index cleaning
# ---------------------------------------------------------------------------
MAD_MULTIPLIER = 3.0
PCP_TOLERANCE_PCT = 0.005  # 0.5 %
# Raw underlying_index: plausible TA-35 cash-index band when no row in ta35_daily_range.csv
SPOT_SANE_MIN = 800.0
SPOT_SANE_MAX = 4000.0
# Pre-clean impute: median of sane raw prints in this many centiseconds (60 s)
UI_IMPUTE_WINDOW_CS = 6000

# ---------------------------------------------------------------------------
# Arrow schemas  (typed columns for Parquet output)
# ---------------------------------------------------------------------------

_instrument_fields = [
    pa.field("underlying", pa.utf8()),
    pa.field("option_type", pa.utf8()),        # "C" or "P"
    pa.field("strike", pa.int32()),  # index strike in points (3–4 digits; TA-35 monthly)
    pa.field("expiry_code", pa.utf8()),         # raw code, e.g. "AUG4" / "M301"
    pa.field("expiry_date", pa.date32()),        # resolved calendar date
]

TRADES_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("base_asset_code", pa.utf8()),
    pa.field("contract_number", pa.utf8()),
    pa.field("instrument_name", pa.utf8()),
    pa.field("time", pa.utf8()),                 # HHMMSSCC kept as string
    pa.field("sequence", pa.int32()),
    pa.field("flag", pa.int8()),
    pa.field("price", pa.int32()),  # whole ILS (REP uses 2 decimals; value is integer)
    pa.field("quantity", pa.int32()),
    pa.field("direction_flag", pa.int8()),
    pa.field("buy_order_id", pa.utf8()),
    pa.field("sell_order_id", pa.utf8()),
    pa.field("cross_flag", pa.int8()),
] + _instrument_fields)

ORDERS_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("base_asset_code", pa.utf8()),
    pa.field("contract_number", pa.utf8()),
    pa.field("instrument_name", pa.utf8()),
    pa.field("order_number", pa.utf8()),
    pa.field("status", pa.utf8()),
    pa.field("side", pa.utf8()),
    pa.field("price", pa.int32()),  # whole ILS (REP uses 2 decimals; value is integer)
    pa.field("quantity", pa.int32()),
    pa.field("filled_qty", pa.int32()),
    pa.field("condition", pa.utf8()),
    pa.field("timestamp", pa.utf8()),            # HHMMSSCC
    pa.field("ref_order_1", pa.utf8()),
    pa.field("ref_order_2", pa.utf8()),
    pa.field("cancel_reason", pa.utf8()),
    pa.field("cancel_time", pa.utf8()),          # HHMMSSCC
    pa.field("extra_flag", pa.utf8()),
] + _instrument_fields)

QUOTES_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("timestamp", pa.utf8()),            # HHMMSSCC
    pa.field("base_asset_code", pa.utf8()),
    pa.field("contract_number", pa.utf8()),
    # bid side (5 layers, nullable for 3-layer files) — whole ILS
    pa.field("bid_price_1", pa.int32()),
    pa.field("bid_price_2", pa.int32()),
    pa.field("bid_price_3", pa.int32()),
    pa.field("bid_price_4", pa.int32()),
    pa.field("bid_price_5", pa.int32()),
    pa.field("bid_qty_1", pa.int32()),
    pa.field("bid_qty_2", pa.int32()),
    pa.field("bid_qty_3", pa.int32()),
    pa.field("bid_qty_4", pa.int32()),
    pa.field("bid_qty_5", pa.int32()),
    # ask side (5 layers) — whole ILS
    pa.field("ask_price_1", pa.int32()),
    pa.field("ask_price_2", pa.int32()),
    pa.field("ask_price_3", pa.int32()),
    pa.field("ask_price_4", pa.int32()),
    pa.field("ask_price_5", pa.int32()),
    pa.field("ask_qty_1", pa.int32()),
    pa.field("ask_qty_2", pa.int32()),
    pa.field("ask_qty_3", pa.int32()),
    pa.field("ask_qty_4", pa.int32()),
    pa.field("ask_qty_5", pa.int32()),
    # derived / meta
    pa.field("theoretical_price", pa.int32()),
    pa.field("price_type", pa.utf8()),
    pa.field("volume", pa.int32()),
    pa.field("underlying_index", pa.float64()),
] + _instrument_fields)
