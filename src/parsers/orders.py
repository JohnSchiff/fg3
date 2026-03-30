"""Parse MAOF order files (FKL502R .REP).

Each file is one trading day.  Rows are filtered to TA-35 monthly options.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path

import pyarrow as pa

from src.config import TA35_BASE_ASSET_CODE, TA35_INSTRUMENT_PREFIXES, ORDERS_SCHEMA
from src.parsers.instruments import is_ta35_monthly_option, parse_instrument
from src.parsers.rep_prices import rep_price_to_int

log = logging.getLogger(__name__)


def parse_orders_file(path: Path) -> tuple[datetime.date | None, pa.Table]:
    """Parse a single FKL502R .REP file.

    Returns ``(date, table)`` where *date* is the trading day extracted from
    the first data row, and *table* conforms to :data:`ORDERS_SCHEMA`.
    Returns ``(None, empty_table)`` when the file contains no matching rows.
    """
    rows: list[dict] = []
    file_date: datetime.date | None = None

    with open(path, "r", encoding="latin-1") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("MAOF-ORDERS"):
                continue

            fields = line.split(",")
            if len(fields) < 16:
                continue

            base_code = fields[1].strip()
            if base_code != TA35_BASE_ASSET_CODE:
                continue

            instrument_name = fields[3].strip()
            if not instrument_name.startswith(TA35_INSTRUMENT_PREFIXES):
                continue

            date_str = fields[0].strip()
            try:
                order_date = datetime.date(
                    int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
                )
            except (ValueError, IndexError):
                continue

            if file_date is None:
                file_date = order_date

            trade_year = order_date.year
            if not is_ta35_monthly_option(instrument_name, trade_year):
                continue

            info = parse_instrument(instrument_name, trade_year)
            if info is None:
                continue

            price = rep_price_to_int(fields[7])
            if price is None:
                continue

            extra_flag = fields[16].strip() if len(fields) > 16 else ""

            rows.append({
                "date": order_date,
                "base_asset_code": base_code,
                "contract_number": fields[2].strip(),
                "instrument_name": instrument_name,
                "order_number": fields[4].strip(),
                "status": fields[5].strip(),
                "side": fields[6].strip(),
                "price": price,
                "quantity": int(fields[8].strip()),
                "filled_qty": int(fields[9].strip()),
                "condition": fields[10].strip(),
                "timestamp": fields[11].strip(),
                "ref_order_1": fields[12].strip(),
                "ref_order_2": fields[13].strip(),
                "cancel_reason": fields[14].strip(),
                "cancel_time": fields[15].strip(),
                "extra_flag": extra_flag,
                "underlying": info.underlying,
                "option_type": info.option_type,
                "strike": info.strike,
                "expiry_code": info.expiry_code,
                "expiry_date": info.expiry_date,
            })

    table = _rows_to_table(rows)
    log.info("Parsed %s: %d TA-35 monthly rows", path.name, len(table))
    return file_date, table


def _rows_to_table(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({f.name: pa.array([], type=f.type) for f in ORDERS_SCHEMA})
    columns: dict[str, list] = {f.name: [] for f in ORDERS_SCHEMA}
    for r in rows:
        for col in columns:
            columns[col].append(r.get(col))
    arrays = [pa.array(columns[f.name], type=f.type) for f in ORDERS_SCHEMA]
    return pa.table(dict(zip([f.name for f in ORDERS_SCHEMA], arrays)), schema=ORDERS_SCHEMA)
