"""Three-layer TA-35 spot index cleaning for quote data.

Layer 1 -- Median + MAD outlier filter (scale-free)
Layer 2 -- ATM selection (closest strike to median)
Layer 3 -- Put-call parity cross-check (model-free validation)

The cleaned series replaces ``underlying_index`` in every quote row for a day.
"""

from __future__ import annotations

import datetime
import logging
import math
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa

from src.config import BOI_RATE_CSV, EXPIRATIONS_CSV, MAD_MULTIPLIER, PCP_TOLERANCE_PCT

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Reference data loaders
# ---------------------------------------------------------------------------

_boi_cache: pd.Series | None = None


def _load_boi_rate() -> pd.Series:
    """Daily BOI rate series (forward-filled)."""
    global _boi_cache
    if _boi_cache is not None:
        return _boi_cache
    df = pd.read_csv(BOI_RATE_CSV, parse_dates=["date"])
    df = df.set_index("date").sort_index()
    _boi_cache = df["rate"].resample("D").ffill()
    return _boi_cache


def _get_risk_free_rate(date: datetime.date) -> float:
    boi = _load_boi_rate()
    ts = pd.Timestamp(date)
    idx = boi.index.get_indexer([ts], method="ffill")[0]
    if idx < 0:
        return 0.0
    return float(boi.iloc[idx])


_expiry_cache: dict[tuple[int, int], datetime.date] | None = None


def _load_expirations() -> dict[tuple[int, int], datetime.date]:
    global _expiry_cache
    if _expiry_cache is not None:
        return _expiry_cache
    df = pd.read_csv(EXPIRATIONS_CSV)
    _expiry_cache = {}
    for _, row in df.iterrows():
        _expiry_cache[(int(row["year"]), int(row["month"]))] = pd.Timestamp(row["expiry_date"]).date()
    return _expiry_cache


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def clean_underlying_index(quotes_df: pd.DataFrame, trade_date: datetime.date) -> pd.DataFrame:
    """Replace ``underlying_index`` in *quotes_df* with the ATM-derived spot.

    Parameters
    ----------
    quotes_df : DataFrame
        Must contain at least: ``timestamp``, ``contract_number``, ``strike``,
        ``option_type``, ``underlying_index``, ``bid_price_1``, ``ask_price_1``,
        ``expiry_date``.
    trade_date : datetime.date
        The trading day for BOI rate lookup.

    Returns a copy of the dataframe with ``underlying_index`` replaced.
    """
    if quotes_df.empty:
        return quotes_df

    df = quotes_df.copy()
    r = _get_risk_free_rate(trade_date)

    timestamps = df["timestamp"].unique()
    cleaned_index = {}

    for ts in timestamps:
        mask = df["timestamp"] == ts
        snap = df.loc[mask]

        spot = _compute_spot_for_timestamp(snap, trade_date, r)
        if spot is not None:
            cleaned_index[ts] = spot

    if not cleaned_index:
        log.warning("No reliable spot values for %s; keeping raw underlying_index", trade_date)
        return df

    spot_series = pd.Series(cleaned_index).sort_index()
    spot_series = spot_series.ffill()

    df["underlying_index"] = df["timestamp"].map(spot_series)
    # Forward-fill any remaining gaps from timestamps before first clean value
    df["underlying_index"] = df["underlying_index"].ffill()

    return df


# ---------------------------------------------------------------------------
# Per-timestamp computation
# ---------------------------------------------------------------------------

def _compute_spot_for_timestamp(
    snap: pd.DataFrame,
    trade_date: datetime.date,
    r: float,
) -> Optional[float]:
    """Compute the clean spot for a single timestamp snapshot."""

    # --- Layer 1: Median + MAD filter ---
    raw_values = snap["underlying_index"].dropna()
    if raw_values.empty:
        return None

    values = raw_values.values.astype(float)
    median_val = float(np.median(values))

    if median_val <= 0:
        return None

    mad = float(np.median(np.abs(values - median_val)))
    if mad == 0:
        # All values agree -- use the median directly
        surviving = snap
    else:
        threshold = MAD_MULTIPLIER * mad
        surviving_mask = np.abs(snap["underlying_index"].values.astype(float) - median_val) <= threshold
        surviving = snap.loc[surviving_mask]

    if surviving.empty:
        return median_val

    # --- Layer 2: ATM selection ---
    strikes = surviving["strike"].dropna().values.astype(float)
    if len(strikes) == 0:
        return median_val

    diffs = np.abs(strikes - median_val)
    atm_idx = np.argmin(diffs)
    atm_row = surviving.iloc[atm_idx]
    s_atm = float(atm_row["underlying_index"]) if pd.notna(atm_row["underlying_index"]) else median_val

    # --- Layer 3: Put-call parity cross-check ---
    if s_atm <= 0:
        return median_val

    s_pcp = _put_call_parity_spot(surviving, atm_row, trade_date, r)
    if s_pcp is not None and s_pcp > 0:
        if abs(s_pcp - s_atm) / s_atm <= PCP_TOLERANCE_PCT:
            return s_atm
        else:
            log.debug(
                "PCP disagreement at %s: S_atm=%.2f S_pcp=%.2f, using PCP",
                atm_row.get("timestamp", "?"), s_atm, s_pcp,
            )
            return s_pcp

    return s_atm


def _put_call_parity_spot(
    snap: pd.DataFrame,
    atm_row: pd.Series,
    trade_date: datetime.date,
    r: float,
) -> Optional[float]:
    """Compute implied spot from put-call parity at the ATM strike.

    S = C_mid - P_mid + K * exp(-r * T)
    """
    strike = atm_row.get("strike")
    if strike is None or pd.isna(strike):
        return None
    strike = float(strike)

    expiry = atm_row.get("expiry_date")
    if expiry is None or pd.isna(expiry):
        return None
    if isinstance(expiry, pd.Timestamp):
        expiry = expiry.date()

    T = (expiry - trade_date).days / 365.0
    if T <= 0:
        return None

    atm_strike_rows = snap[snap["strike"] == strike]
    calls = atm_strike_rows[atm_strike_rows["option_type"] == "C"]
    puts = atm_strike_rows[atm_strike_rows["option_type"] == "P"]

    if calls.empty or puts.empty:
        return None

    call_row = calls.iloc[0]
    put_row = puts.iloc[0]

    c_mid = _bid_ask_mid(call_row)
    p_mid = _bid_ask_mid(put_row)
    if c_mid is None or p_mid is None:
        return None

    discount = math.exp(-r * T)
    s_implied = c_mid - p_mid + strike * discount

    if s_implied <= 0:
        return None
    return s_implied


def _bid_ask_mid(row: pd.Series) -> Optional[float]:
    """Compute bid-ask midpoint from layer 1 prices."""
    bid = row.get("bid_price_1")
    ask = row.get("ask_price_1")
    if bid is None or ask is None or pd.isna(bid) or pd.isna(ask):
        return None
    bid, ask = float(bid), float(ask)
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2.0
