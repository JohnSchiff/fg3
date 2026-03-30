"""REP monetary fields stored with two decimals; values are whole ILS → int."""

from __future__ import annotations

from typing import Optional


def rep_price_to_int(s: str) -> Optional[int]:
    """Parse a REP price field (e.g. ``1760.00``) to whole shekels as int."""
    try:
        return int(round(float(s.strip())))
    except (ValueError, AttributeError):
        return None
