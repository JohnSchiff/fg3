"""Parse MAOF instrument name strings into structured fields.

Two formats handled:
  Old (pre-~2017): "T25 C01340 AUG4"   -> underlying=T25, C, strike=1340, AUG4
  New (post-~2017): "T35-C001720M301"   -> underlying=T35, C, strike=1720, M301
"""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import pandas as pd

from src.config import EXPIRATIONS_CSV, MIN_FUTURES_STRIKE

# ---------------------------------------------------------------------------
# Expiration calendar (loaded once)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_expiration_calendar() -> dict[tuple[int, int], datetime.date]:
    """Return {(year, month): expiry_date} from the CSV."""
    df = pd.read_csv(EXPIRATIONS_CSV)
    cal: dict[tuple[int, int], datetime.date] = {}
    for _, row in df.iterrows():
        cal[(int(row["year"]), int(row["month"]))] = pd.Timestamp(row["expiry_date"]).date()
    return cal


MONTH_ABBR_TO_NUM = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

# ---------------------------------------------------------------------------
# Parsed result
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class InstrumentInfo:
    underlying: str           # "T25" or "T35"
    option_type: str          # "C" or "P"
    strike: int               # index strike in whole points (fits int32)
    expiry_code: str          # raw string, e.g. "AUG4" or "M301"
    expiry_date: Optional[datetime.date]
    is_monthly: bool
    is_futures: bool

# ---------------------------------------------------------------------------
# Old-format parser: "T25 C01340 AUG4"
# ---------------------------------------------------------------------------

_OLD_RE = re.compile(
    r"^(?P<und>[A-Z]\d{2})\s+"
    r"(?P<cp>[CP])"
    r"(?P<strike>\d{5})\s+"
    r"(?P<expiry>\S+)$"
)


def _parse_old_format(name: str, trade_year: int) -> Optional[InstrumentInfo]:
    m = _OLD_RE.match(name)
    if m is None:
        return None

    underlying = m.group("und")
    option_type = m.group("cp")
    strike = int(m.group("strike"))
    expiry_code = m.group("expiry")

    is_weekly = expiry_code.startswith("W")
    is_futures = strike < MIN_FUTURES_STRIKE

    expiry_date: Optional[datetime.date] = None
    if not is_weekly and not is_futures:
        month_abbr = expiry_code[:3].upper()
        year_digit = expiry_code[3:]
        month_num = MONTH_ABBR_TO_NUM.get(month_abbr)
        if month_num is not None and year_digit.isdigit():
            year_suffix = int(year_digit)
            decade = (trade_year // 10) * 10
            full_year = decade + year_suffix
            if full_year < trade_year - 1:
                full_year += 10
            cal = _load_expiration_calendar()
            expiry_date = cal.get((full_year, month_num))

    return InstrumentInfo(
        underlying=underlying,
        option_type=option_type,
        strike=strike,
        expiry_code=expiry_code,
        expiry_date=expiry_date,
        is_monthly=not is_weekly,
        is_futures=is_futures,
    )

# ---------------------------------------------------------------------------
# New-format parser: "T35-C001720M301"
# ---------------------------------------------------------------------------

_NEW_RE = re.compile(
    r"^(?P<und>[A-Z]\d{2})-"
    r"(?P<cp>[CP])"
    r"(?P<strike>\d{6})"
    r"(?P<mw>[MW])"
    r"(?P<code>\d+)$"
)


def _parse_new_format(name: str, trade_year: int) -> Optional[InstrumentInfo]:
    m = _NEW_RE.match(name)
    if m is None:
        return None

    underlying = m.group("und")
    option_type = m.group("cp")
    strike = int(m.group("strike"))
    mw = m.group("mw")
    code = m.group("code")

    is_weekly = mw == "W"
    is_futures = strike < MIN_FUTURES_STRIKE
    expiry_code = mw + code  # e.g. "M301"

    expiry_date: Optional[datetime.date] = None
    if not is_weekly and not is_futures and len(code) >= 3:
        month_num = int(code[-2:])
        cal = _load_expiration_calendar()
        # Try current trade_year first, then next year for Dec->Jan rollover
        expiry_date = cal.get((trade_year, month_num))
        if expiry_date is None:
            expiry_date = cal.get((trade_year + 1, month_num))
        if expiry_date is None:
            expiry_date = cal.get((trade_year - 1, month_num))

    return InstrumentInfo(
        underlying=underlying,
        option_type=option_type,
        strike=strike,
        expiry_code=expiry_code,
        expiry_date=expiry_date,
        is_monthly=not is_weekly,
        is_futures=is_futures,
    )

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_instrument(name: str, trade_year: int) -> Optional[InstrumentInfo]:
    """Parse an instrument name string into structured fields.

    Returns None if the name cannot be parsed.
    """
    name = name.strip()
    if "-" in name:
        return _parse_new_format(name, trade_year)
    return _parse_old_format(name, trade_year)


def is_ta35_monthly_option(name: str, trade_year: int) -> bool:
    """Quick predicate: is this a TA-35 monthly option (not weekly, not futures)?"""
    info = parse_instrument(name, trade_year)
    if info is None:
        return False
    if info.underlying not in ("T25", "T35"):
        return False
    if not info.is_monthly:
        return False
    if info.is_futures:
        return False
    return True
