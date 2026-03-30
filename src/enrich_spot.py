"""Cleaned ``underlying_index`` for quote data (MAD, ATM, put–call parity).

The core logic lives in :func:`clean_spot_df`, which runs automatically during
ingestion (Phase 3 quotes) unless ``--no-enrich`` is passed to ``src.ingest``.

First, null or out-of-band *raw* prints are imputed from the median of sane raw
values in the prior 60s window (see :data:`src.config.UI_IMPUTE_WINDOW_CS`).
Sane band = official session low/high from ``data/ta35_daily_range.csv`` when
present, else :data:`src.config.SPOT_SANE_MIN`–:data:`src.config.SPOT_SANE_MAX`.

Optional: ``data/ta35_daily_range.csv`` (``date``, ``low``, ``high``) clips the
cleaned spot to the official TA-35 session band per day when that date is listed.

This module can also re-enrich existing daily quote Parquet files in place.

Usage::

    python -m src.enrich_spot --years 2023
    python -m src.enrich_spot --years 2023 --months 1
    python -m src.enrich_spot --file ~/fg3-data/parquet/quotes/year=2023/month=01/quotes_20230102.parquet
"""

from __future__ import annotations

import argparse
import datetime
import logging
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import (
    BOI_RATE_CSV,
    MAD_MULTIPLIER,
    PARQUET_DIR,
    PCP_TOLERANCE_PCT,
    SPOT_SANE_MAX,
    SPOT_SANE_MIN,
    TA35_DAILY_RANGE_CSV,
    UI_IMPUTE_WINDOW_CS,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# BOI rate loader
# ---------------------------------------------------------------------------

_boi_df: pd.DataFrame | None = None


def _load_boi() -> pd.DataFrame:
    global _boi_df
    if _boi_df is not None:
        return _boi_df
    _boi_df = pd.read_csv(BOI_RATE_CSV, parse_dates=["date"]).set_index("date").sort_index()
    return _boi_df


def _get_rate(date: datetime.date) -> float:
    boi = _load_boi()
    ts = pd.Timestamp(date)
    mask = boi.index <= ts
    if not mask.any():
        return 0.0
    return float(boi.loc[mask, "rate"].iloc[-1])


# ---------------------------------------------------------------------------
# TA-35 official session low / high (optional daily band)
# ---------------------------------------------------------------------------

_range_df: pd.DataFrame | None = None


def _load_ta35_daily_range() -> pd.DataFrame:
    """Return DataFrame indexed by date with columns low, high. May be empty."""
    global _range_df
    if _range_df is not None:
        return _range_df
    if not TA35_DAILY_RANGE_CSV.is_file():
        _range_df = pd.DataFrame(columns=["low", "high"])
        _range_df.index.name = "date"
        return _range_df
    raw = pd.read_csv(TA35_DAILY_RANGE_CSV, parse_dates=["date"])
    if raw.empty:
        _range_df = pd.DataFrame(columns=["low", "high"])
        _range_df.index.name = "date"
        return _range_df
    raw = raw.dropna(subset=["low", "high"])
    raw["low"] = pd.to_numeric(raw["low"], errors="coerce")
    raw["high"] = pd.to_numeric(raw["high"], errors="coerce")
    raw = raw.dropna(subset=["low", "high"])
    _range_df = (
        raw.drop_duplicates(subset=["date"], keep="last")
        .set_index("date")
        .sort_index()[["low", "high"]]
    )
    return _range_df


def _get_daily_spot_bounds(trade_date: datetime.date) -> tuple[float, float] | None:
    """Session low/high for *trade_date* if present in CSV; else None."""
    rng = _load_ta35_daily_range()
    if rng.empty:
        return None
    key = pd.Timestamp(trade_date).normalize()
    try:
        row = rng.loc[key]
    except KeyError:
        return None
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    lo, hi = float(row["low"]), float(row["high"])
    if not (math.isfinite(lo) and math.isfinite(hi)) or lo <= 0 or hi <= 0:
        return None
    if lo > hi:
        lo, hi = hi, lo
    return lo, hi


def _timestamp_series_to_cs(ts: pd.Series) -> pd.Series:
    """HHMMSSCC string → centiseconds from midnight (nullable Int64)."""
    s = ts.astype(str).str.strip().str.replace(r"\..*$", "", regex=True)
    s8 = s.str.zfill(8).str.slice(0, 8)
    hh = pd.to_numeric(s8.str.slice(0, 2), errors="coerce")
    mm = pd.to_numeric(s8.str.slice(2, 4), errors="coerce")
    ss = pd.to_numeric(s8.str.slice(4, 6), errors="coerce")
    cc = pd.to_numeric(s8.str.slice(6, 8), errors="coerce")
    out = hh * 360000 + mm * 6000 + ss * 100 + cc
    return out.astype("Int64")


def _impute_raw_underlying_60s(df: pd.DataFrame, trade_date: datetime.date) -> pd.DataFrame:
    """Replace null / out-of-band raw ``underlying_index`` with a 60s-window median.

    For each row whose raw print is missing or outside the official session band
    (from ``ta35_daily_range.csv`` when present, else
    :data:`SPOT_SANE_MIN`–:data:`SPOT_SANE_MAX`), set the value to the median of
    **sane** raw prints on the same calendar date with timestamp in
    ``(t_row - UI_IMPUTE_WINDOW_CS, t_row]`` (centisecond resolution).

    Sane = finite and inside the same band used to flag outliers.
    """
    if df.empty or "underlying_index" not in df.columns or "timestamp" not in df.columns:
        return df

    bounds = _get_daily_spot_bounds(trade_date)
    if bounds is not None:
        lo, hi = float(bounds[0]), float(bounds[1])
    else:
        lo, hi = SPOT_SANE_MIN, SPOT_SANE_MAX

    df = df.copy()
    ui = pd.to_numeric(df["underlying_index"], errors="coerce")
    t_cs = _timestamp_series_to_cs(df["timestamp"])
    df["_t_cs"] = t_cs

    valid_t = t_cs.notna()
    sane = (
        valid_t
        & ui.notna()
        & np.isfinite(ui)
        & (ui >= lo)
        & (ui <= hi)
    )
    need = valid_t & (~sane)

    if not need.any():
        return df.drop(columns=["_t_cs"], errors="ignore")

    if not sane.any():
        return df.drop(columns=["_t_cs"], errors="ignore")

    sub = df.loc[sane, ["_t_cs", "underlying_index"]].copy()
    sub["underlying_index"] = pd.to_numeric(sub["underlying_index"], errors="coerce")
    g = sub.groupby("_t_cs", sort=True)["underlying_index"].median()

    all_ts = np.sort(df.loc[valid_t, "_t_cs"].dropna().unique().astype("int64"))
    g_aligned = g.reindex(all_ts)

    origin = pd.Timestamp("2000-01-01")
    # 1 centisecond = 10 ms for timedelta arithmetic
    dt_index = origin + pd.to_timedelta(all_ts * 10, unit="ms")
    ser = pd.Series(g_aligned.values, index=dt_index).sort_index()
    rolled = ser.rolling(f"{UI_IMPUTE_WINDOW_CS * 10}ms", min_periods=1).median()
    rolled = rolled.ffill().bfill()

    # Map back by position: rolling preserves order; avoid Timestamp.asi8 round-trip
    # (DatetimeIndex can be [us] while origin.value is [ns]).
    lookup = pd.Series(rolled.to_numpy(), index=all_ts)

    imputed = df.loc[need, "_t_cs"].astype("int64").map(lookup)
    df.loc[need, "underlying_index"] = imputed.to_numpy()
    return df.drop(columns=["_t_cs"], errors="ignore")


# ---------------------------------------------------------------------------
# Core: vectorized per-day cleaning
# ---------------------------------------------------------------------------

def clean_spot_df(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``df`` with cleaned ``underlying_index`` for one trading day.

    Derives the session date from the first row's ``date`` column, loads the BOI rate,
    builds per-timestamp spots via :func:`_compute_spot_series`, forward-fills,
    and maps back onto rows. If ``df`` is empty or no clean spots are found,
    returns ``df`` unchanged.
    """
    if df.empty:
        return df

    trade_date = df["date"].iloc[0]
    if isinstance(trade_date, pd.Timestamp):
        trade_date = trade_date.date()

    df = _impute_raw_underlying_60s(df, trade_date)

    r = _get_rate(trade_date)
    spot_by_ts = _compute_spot_series(df, trade_date, r)

    if spot_by_ts.empty:
        return df

    spot_by_ts = spot_by_ts.sort_index().ffill()
    df = df.copy()
    df["underlying_index"] = df["timestamp"].map(spot_by_ts)
    df["underlying_index"] = df["underlying_index"].ffill()

    bounds = _get_daily_spot_bounds(trade_date)
    if bounds is not None:
        lo, hi = bounds
        df["underlying_index"] = df["underlying_index"].clip(lower=lo, upper=hi)

    return df


def enrich_file(path: Path) -> None:
    """Read a daily quote Parquet, clean underlying_index, overwrite."""
    table = pq.ParquetFile(path).read()
    df = table.to_pandas()

    if df.empty:
        log.info("Skipping empty file %s", path.name)
        return

    df_in = df
    df = clean_spot_df(df_in)
    if df is df_in:
        log.warning("No clean spot values for %s; leaving raw", path.name)
        return

    out_table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(out_table, path, compression="zstd")
    log.info("Enriched %s  (%d rows)", path.name, len(df))


def _compute_spot_series(
    df: pd.DataFrame,
    trade_date: datetime.date,
    r: float,
) -> pd.Series:
    """Compute a clean spot value per unique timestamp (vectorized).

    Returns a Series indexed by timestamp with float spot values.
    """
    needed = ["timestamp", "underlying_index", "strike", "option_type",
              "bid_price_1", "ask_price_1", "expiry_date"]
    sub = df[needed].copy()
    for c in ("underlying_index", "strike", "bid_price_1", "ask_price_1"):
        sub[c] = pd.to_numeric(sub[c], errors="coerce")

    ts_col = sub["timestamp"]

    # --- Layer 1: MAD filter (vectorized) ---
    ts_median = sub.groupby(ts_col)["underlying_index"].transform("median")
    dev = (sub["underlying_index"] - ts_median).abs()
    ts_mad = dev.groupby(ts_col).transform("median")
    thresh = MAD_MULTIPLIER * ts_mad
    mad_ok = (dev <= thresh) | (ts_mad == 0)
    surv = sub[mad_ok & ts_median.gt(0)].copy()
    if surv.empty:
        return pd.Series(dtype=float)

    # --- Layer 2: ATM selection (vectorized) ---
    surv_median = surv.groupby("timestamp")["underlying_index"].transform("median")
    surv["_sdist"] = (surv["strike"] - surv_median).abs()
    atm_idx = surv.groupby("timestamp")["_sdist"].idxmin()
    atm = surv.loc[atm_idx].set_index("timestamp")
    spot = atm["underlying_index"].copy()
    bad = spot.isna() | (spot <= 0)
    if bad.any():
        fallback = surv.groupby("timestamp")["underlying_index"].median()
        spot = spot.where(~bad, fallback)

    # --- Layer 3: PCP override (vectorized) ---
    atm_info = atm[["strike", "expiry_date"]].rename(
        columns={"strike": "_K", "expiry_date": "_exp"}
    )
    merged = surv.join(atm_info, on="timestamp")
    at_k = merged[merged["strike"] == merged["_K"]]

    calls = at_k[at_k["option_type"] == "C"].groupby("timestamp").first()
    puts = at_k[at_k["option_type"] == "P"].groupby("timestamp").first()

    both = calls[["bid_price_1", "ask_price_1", "_K", "_exp"]].join(
        puts[["bid_price_1", "ask_price_1"]], lsuffix="_c", rsuffix="_p", how="inner",
    )
    valid = both[
        (both["bid_price_1_c"] > 0) & (both["ask_price_1_c"] > 0)
        & (both["bid_price_1_p"] > 0) & (both["ask_price_1_p"] > 0)
    ].copy()

    if not valid.empty:
        valid["_c_mid"] = (valid["bid_price_1_c"] + valid["ask_price_1_c"]) / 2
        valid["_p_mid"] = (valid["bid_price_1_p"] + valid["ask_price_1_p"]) / 2
        exp_dates = valid["_exp"].apply(
            lambda x: (x.date() if hasattr(x, "date") else x) if pd.notna(x) else None
        )
        valid["_T"] = exp_dates.apply(
            lambda x: (x - trade_date).days / 365.0 if x is not None else 0.0
        )
        valid = valid[valid["_T"] > 0]
        if not valid.empty:
            valid["_s_pcp"] = (
                (valid["_c_mid"] - valid["_p_mid"]) / 100
                + valid["_K"] * np.exp(-r * valid["_T"])
            )
            valid = valid[valid["_s_pcp"] > 0]

    if not valid.empty:
        common_ts = spot.index.intersection(valid.index)
        if len(common_ts) > 0:
            s_a = spot.reindex(common_ts)
            s_p = valid.reindex(common_ts)["_s_pcp"]
            disagree = ((s_p - s_a).abs() / s_a) > PCP_TOLERANCE_PCT
            spot.loc[disagree[disagree].index] = s_p[disagree]

    return spot[spot > 0]


def _spot_for_group(
    grp: pd.DataFrame,
    trade_date: datetime.date,
    r: float,
) -> float | None:
    """3-layer spot computation for one timestamp group."""

    # --- Layer 1: Median + MAD ---
    vals = grp["underlying_index"].dropna().values
    if len(vals) == 0:
        return None

    median_val = float(np.median(vals))
    if median_val <= 0:
        return None

    mad = float(np.median(np.abs(vals - median_val)))
    if mad == 0:
        surviving = grp
    else:
        thresh = MAD_MULTIPLIER * mad
        mask = np.abs(grp["underlying_index"].values - median_val) <= thresh
        surviving = grp.iloc[mask] if isinstance(mask, np.ndarray) else grp.loc[mask]

    if surviving.empty:
        return median_val

    # --- Layer 2: ATM selection ---
    strikes = surviving["strike"].dropna().values
    if len(strikes) == 0:
        return median_val

    atm_idx = int(np.argmin(np.abs(strikes - median_val)))
    atm_row = surviving.iloc[atm_idx]
    s_atm = float(atm_row["underlying_index"]) if pd.notna(atm_row["underlying_index"]) else median_val

    if s_atm <= 0:
        return median_val

    # --- Layer 3: Put-call parity ---
    s_pcp = _pcp_check(surviving, atm_row, trade_date, r)
    if s_pcp is not None and s_pcp > 0:
        if abs(s_pcp - s_atm) / s_atm <= PCP_TOLERANCE_PCT:
            return s_atm
        return s_pcp

    return s_atm


def _pcp_check(
    grp: pd.DataFrame,
    atm_row: pd.Series,
    trade_date: datetime.date,
    r: float,
) -> float | None:
    """Put-call parity implied spot: S = C_mid - P_mid + K * exp(-r*T)."""
    strike = atm_row.get("strike")
    if strike is None or pd.isna(strike):
        return None
    strike = int(strike)

    expiry = atm_row.get("expiry_date")
    if expiry is None or pd.isna(expiry):
        return None
    if isinstance(expiry, pd.Timestamp):
        expiry = expiry.date()

    T = (expiry - trade_date).days / 365.0
    if T <= 0:
        return None

    at_strike = grp[grp["strike"] == strike]
    calls = at_strike[at_strike["option_type"] == "C"]
    puts = at_strike[at_strike["option_type"] == "P"]
    if calls.empty or puts.empty:
        return None

    c = calls.iloc[0]
    p = puts.iloc[0]

    cb, ca = c["bid_price_1"], c["ask_price_1"]
    pb, pa_ = p["bid_price_1"], p["ask_price_1"]
    if any(pd.isna(x) or x <= 0 for x in [cb, ca, pb, pa_]):
        return None

    c_mid = (cb + ca) / 2.0
    p_mid = (pb + pa_) / 2.0
    s_impl = (c_mid - p_mid) / 100 + strike * math.exp(-r * T)
    return s_impl if s_impl > 0 else None


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def discover_quote_files(
    years: list[int] | None = None,
    months: list[int] | None = None,
) -> list[Path]:
    """Find daily quote Parquet files matching year/month filters."""
    base = PARQUET_DIR / "quotes"
    if not base.is_dir():
        return []

    files: list[Path] = []
    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.startswith("year="):
            continue
        y = int(year_dir.name.split("=")[1])
        if years and y not in years:
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.startswith("month="):
                continue
            m = int(month_dir.name.split("=")[1])
            if months and m not in months:
                continue

            for f in sorted(month_dir.glob("*.parquet")):
                files.append(f)

    return files


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich quote Parquet files with cleaned underlying_index (post-processing)")
    parser.add_argument("--years", type=int, nargs="+")
    parser.add_argument("--months", type=int, nargs="+")
    parser.add_argument("--file", type=str, help="Process a single Parquet file")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.file:
        enrich_file(Path(args.file))
        return

    files = discover_quote_files(years=args.years, months=args.months)
    log.info("Found %d quote Parquet files to enrich", len(files))

    for f in files:
        enrich_file(f)

    log.info("Done.")


if __name__ == "__main__":
    main()
