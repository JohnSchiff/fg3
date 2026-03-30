"""Parse MAOF trade files (FKL501R .REP).

Weekly files are split by the date column so the caller can write one
Parquet file per trading day.
"""

from __future__ import annotations

import datetime
import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterator

import pyarrow as pa

from src.config import TA35_BASE_ASSET_CODE, TA35_INSTRUMENT_PREFIXES, TRADES_SCHEMA
from src.parsers.instruments import is_ta35_monthly_option, parse_instrument
from src.parsers.rep_prices import rep_price_to_int

log = logging.getLogger(__name__)


def parse_trades_file(path: Path) -> dict[datetime.date, pa.Table]:
    """Parse a single FKL501R .REP file.

    Returns a dict mapping each trading date found in the file to a
    PyArrow Table conforming to :data:`TRADES_SCHEMA`.
    """
    rows_by_date: dict[datetime.date, list[dict]] = defaultdict(list)

    with open(path, "r", encoding="latin-1") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("MAOF-TRADES"):
                continue

            fields = line.split(",")
            if len(fields) < 13:
                continue

            base_code = fields[1].strip()
            if base_code != TA35_BASE_ASSET_CODE:
                continue

            instrument_name = fields[3].strip()
            if not instrument_name.startswith(TA35_INSTRUMENT_PREFIXES):
                continue

            date_str = fields[0].strip()
            try:
                trade_date = datetime.date(
                    int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
                )
            except (ValueError, IndexError):
                continue

            trade_year = trade_date.year
            if not is_ta35_monthly_option(instrument_name, trade_year):
                continue

            info = parse_instrument(instrument_name, trade_year)
            if info is None:
                continue

            price = rep_price_to_int(fields[7])
            if price is None:
                continue

            rows_by_date[trade_date].append({
                "date": trade_date,
                "base_asset_code": base_code,
                "contract_number": fields[2].strip(),
                "instrument_name": instrument_name,
                "time": fields[4].strip(),
                "sequence": int(fields[5].strip()),
                "flag": int(fields[6].strip()),
                "price": price,
                "quantity": int(fields[8].strip()),
                "direction_flag": int(fields[9].strip()),
                "buy_order_id": fields[10].strip(),
                "sell_order_id": fields[11].strip(),
                "cross_flag": int(fields[12].strip()),
                "underlying": info.underlying,
                "option_type": info.option_type,
                "strike": info.strike,
                "expiry_code": info.expiry_code,
                "expiry_date": info.expiry_date,
            })

    result: dict[datetime.date, pa.Table] = {}
    for dt, rows in rows_by_date.items():
        result[dt] = _rows_to_table(rows)

    log.info(
        "Parsed %s: %d dates, %d total TA-35 monthly rows",
        path.name, len(result), sum(len(t) for t in result.values()),
    )
    return result


def _rows_to_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in TRADES_SCHEMA})
    columns: dict[str, list] = {f.name: [] for f in TRADES_SCHEMA}
    for r in rows:
        for col in columns:
            columns[col].append(r.get(col))
    arrays = [pa.array(columns[f.name], type=f.type) for f in TRADES_SCHEMA]
    return pa.table(dict(zip([f.name for f in TRADES_SCHEMA], arrays)), schema=TRADES_SCHEMA)
