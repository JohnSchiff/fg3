"""Parse MAOF quote files (FKL504R .REP).

Auto-detects 3-layer vs 5-layer format from field count.
Pairs consecutive Type 2 (bid) + Type 3 (ask) rows into single records.
Filters by a provided set of valid contract numbers (built from trades/orders).
"""

from __future__ import annotations

import datetime
import logging
import time
from pathlib import Path
from typing import Optional

import pyarrow as pa

from src.config import TA35_BASE_ASSET_CODE, QUOTES_SCHEMA
from src.parsers.rep_prices import rep_price_to_int

log = logging.getLogger(__name__)

MAX_READ_RETRIES = 3
RETRY_DELAY_SEC = 10


def parse_quotes_file(
    path: Path,
    valid_contracts: set[str],
    contract_instrument_map: dict[str, dict] | None = None,
) -> tuple[datetime.date | None, pa.Table]:
    """Parse a single FKL504R .REP file.

    Parameters
    ----------
    path : Path
        Path to the .REP file.
    valid_contracts : set[str]
        Contract numbers that pass the TA-35 monthly filter (from trades/orders).
    contract_instrument_map : dict or None
        Mapping ``contract_number -> {underlying, option_type, strike,
        expiry_code, expiry_date}`` for enrichment.

    Returns ``(date, table)`` conforming to :data:`QUOTES_SCHEMA`.
    """
    rows: list[dict] = []
    file_date: datetime.date | None = None
    pending_bid: dict | None = None

    lines = _read_lines_with_retry(path)

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("MAOF-QUOTES"):
            continue

        fields = line.split(",")
        if len(fields) < 10:
            continue

        rec_type = fields[0].strip()

        if rec_type == "2":
            pending_bid = _parse_bid(fields)
        elif rec_type == "3" and pending_bid is not None:
            ask = _parse_ask(fields)
            if ask is None:
                pending_bid = None
                continue

            contract = pending_bid["contract_number"]
            base_code = pending_bid["base_asset_code"]

            if base_code != TA35_BASE_ASSET_CODE or contract not in valid_contracts:
                pending_bid = None
                continue

            if file_date is None:
                file_date = pending_bid["date"]

            inst = {}
            if contract_instrument_map:
                inst = contract_instrument_map.get(contract, {})

            row = {**pending_bid, **ask}
            row["underlying"] = inst.get("underlying")
            row["option_type"] = inst.get("option_type")
            row["strike"] = inst.get("strike")
            row["expiry_code"] = inst.get("expiry_code")
            row["expiry_date"] = inst.get("expiry_date")
            rows.append(row)
            pending_bid = None
        else:
            pending_bid = None

    table = _rows_to_table(rows)
    log.info("Parsed %s: %d TA-35 monthly quote rows", path.name, len(table))
    return file_date, table


# ---------------------------------------------------------------------------
# Retry wrapper for OneDrive / cloud-synced filesystems
# ---------------------------------------------------------------------------

def _read_lines_with_retry(path: Path) -> list[str]:
    for attempt in range(1, MAX_READ_RETRIES + 1):
        try:
            with open(path, "r", encoding="latin-1") as fh:
                return fh.readlines()
        except (TimeoutError, OSError) as exc:
            if attempt < MAX_READ_RETRIES:
                log.warning("Read attempt %d/%d failed for %s: %s — retrying in %ds",
                            attempt, MAX_READ_RETRIES, path.name, exc, RETRY_DELAY_SEC)
                time.sleep(RETRY_DELAY_SEC)
            else:
                log.error("All %d read attempts failed for %s: %s", MAX_READ_RETRIES, path.name, exc)
                raise


# ---------------------------------------------------------------------------
# Bid (Type 2) parsing  -- 3-layer: ~15 fields,  5-layer: ~19 fields
# ---------------------------------------------------------------------------

def _parse_bid(fields: list[str]) -> dict | None:
    try:
        date_str = fields[1].strip()
        quote_date = datetime.date(
            int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
        )
    except (ValueError, IndexError):
        return None

    n = len(fields)
    # Determine number of layers from field count
    # 3-layer Type 2: rec_type, date, time, base, contract, p1,p2,p3, q1,q2,q3, theo, flag, vol, [extra] = ~15
    # 5-layer Type 2: rec_type, date, time, base, contract, p1..p5, q1..q5, theo, flag, vol, [extra] = ~19
    if n >= 19:
        layers = 5
    else:
        layers = 3

    base_idx = 5
    prices = []
    for i in range(layers):
        prices.append(rep_price_to_int(fields[base_idx + i]))
    qtys = []
    qty_idx = base_idx + layers
    for i in range(layers):
        qtys.append(_int(fields[qty_idx + i]))

    meta_idx = qty_idx + layers
    theo = rep_price_to_int(fields[meta_idx]) if meta_idx < n else None
    price_type = fields[meta_idx + 1].strip() if meta_idx + 1 < n else ""
    volume = _int(fields[meta_idx + 2]) if meta_idx + 2 < n else 0

    # Pad to 5 layers
    while len(prices) < 5:
        prices.append(None)
    while len(qtys) < 5:
        qtys.append(None)

    return {
        "date": quote_date,
        "timestamp": fields[2].strip(),
        "base_asset_code": fields[3].strip(),
        "contract_number": fields[4].strip(),
        "bid_price_1": prices[0],
        "bid_price_2": prices[1],
        "bid_price_3": prices[2],
        "bid_price_4": prices[3],
        "bid_price_5": prices[4],
        "bid_qty_1": qtys[0],
        "bid_qty_2": qtys[1],
        "bid_qty_3": qtys[2],
        "bid_qty_4": qtys[3],
        "bid_qty_5": qtys[4],
        "theoretical_price": theo,
        "price_type": price_type,
        "volume": volume,
    }


# ---------------------------------------------------------------------------
# Ask (Type 3) parsing
# ---------------------------------------------------------------------------

def _parse_ask(fields: list[str]) -> dict | None:
    n = len(fields)
    if n >= 16:
        layers = 5
    else:
        layers = 3

    base_idx = 5
    prices = []
    for i in range(layers):
        prices.append(rep_price_to_int(fields[base_idx + i]))
    qtys = []
    qty_idx = base_idx + layers
    for i in range(layers):
        qtys.append(_int(fields[qty_idx + i]))

    # Last field in ask row is underlying_index (base asset index)
    ui_idx = qty_idx + layers
    underlying_index = _float(fields[ui_idx]) if ui_idx < n else None

    while len(prices) < 5:
        prices.append(None)
    while len(qtys) < 5:
        qtys.append(None)

    return {
        "ask_price_1": prices[0],
        "ask_price_2": prices[1],
        "ask_price_3": prices[2],
        "ask_price_4": prices[3],
        "ask_price_5": prices[4],
        "ask_qty_1": qtys[0],
        "ask_qty_2": qtys[1],
        "ask_qty_3": qtys[2],
        "ask_qty_4": qtys[3],
        "ask_qty_5": qtys[4],
        "underlying_index": underlying_index,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _float(s: str) -> Optional[float]:
    try:
        return float(s.strip())
    except (ValueError, AttributeError):
        return None


def _int(s: str) -> Optional[int]:
    try:
        return int(s.strip())
    except (ValueError, AttributeError):
        return None


def _rows_to_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in QUOTES_SCHEMA})
    columns: dict[str, list] = {f.name: [] for f in QUOTES_SCHEMA}
    for r in rows:
        for col in columns:
            columns[col].append(r.get(col))
    arrays = [pa.array(columns[f.name], type=f.type) for f in QUOTES_SCHEMA]
    return pa.table(dict(zip([f.name for f in QUOTES_SCHEMA], arrays)), schema=QUOTES_SCHEMA)
