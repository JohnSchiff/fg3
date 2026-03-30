"""Walk the data directory tree and enumerate available week-folders.

Also manages ``ingest_state.json`` for incremental ingestion tracking.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

from src.config import DATA_DIR, INGEST_STATE_PATH

log = logging.getLogger(__name__)

WEEK_DIR_RE = re.compile(r"^D\d{2}-\d{2}$")


@dataclass(frozen=True, slots=True)
class WeekFolder:
    year: int
    month: int
    week_range: str         # e.g. "D01-05"
    trades_dir: Path        # .../T/
    orders_dir: Path        # .../O/
    quotes_dir: Path        # .../B/

    @property
    def key(self) -> str:
        return f"{self.year}/{self.month:02d}/{self.week_range}"


def discover_weeks(
    years: Optional[list[int]] = None,
    months: Optional[list[int]] = None,
) -> Iterator[WeekFolder]:
    """Yield :class:`WeekFolder` objects from the data tree.

    Parameters
    ----------
    years : list[int] or None
        Restrict to these calendar years.  ``None`` means all years.
    months : list[int] or None
        Restrict to these months (1--12).  ``None`` means all months.
    """
    if not DATA_DIR.is_dir():
        log.error("Data directory does not exist: %s", DATA_DIR)
        return

    for year_dir in sorted(DATA_DIR.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)
        if years is not None and year not in years:
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.startswith("MON_"):
                continue
            try:
                month = int(month_dir.name.split("_")[1])
            except (IndexError, ValueError):
                continue
            if months is not None and month not in months:
                continue

            for week_dir in sorted(month_dir.iterdir()):
                if not week_dir.is_dir() or not WEEK_DIR_RE.match(week_dir.name):
                    continue

                t_dir = week_dir / "T"
                o_dir = week_dir / "O"
                b_dir = week_dir / "B"

                if not (t_dir.is_dir() or o_dir.is_dir() or b_dir.is_dir()):
                    continue

                yield WeekFolder(
                    year=year,
                    month=month,
                    week_range=week_dir.name,
                    trades_dir=t_dir,
                    orders_dir=o_dir,
                    quotes_dir=b_dir,
                )


# ---------------------------------------------------------------------------
# Incremental state management
# ---------------------------------------------------------------------------

def load_ingest_state() -> dict:
    if INGEST_STATE_PATH.exists():
        return json.loads(INGEST_STATE_PATH.read_text())
    return {}


def save_ingest_state(state: dict) -> None:
    INGEST_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True))


def is_already_ingested(state: dict, week: WeekFolder, data_type: str) -> bool:
    return state.get(f"{week.key}/{data_type}", False)


def mark_ingested(state: dict, week: WeekFolder, data_type: str) -> None:
    state[f"{week.key}/{data_type}"] = True
