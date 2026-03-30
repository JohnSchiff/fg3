"""On-the-fly archive extraction (RAR / ARJ) using the ``unar`` CLI.

If extracted data files already exist alongside the archive, they are returned
directly and no extraction is performed.

Also provides :func:`cleanup_extracted_reps` to delete REP files that can be
re-extracted from their archive, and :func:`verify_archives` to pre-check
that every archive can be successfully extracted.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

from src.config import DATA_DIR

log = logging.getLogger(__name__)

_CHUNK_SIZE = 1 << 20            # 1 MB per read
_CHUNK_RETRIES = 3               # retries per individual chunk
_CHUNK_RETRY_DELAY_S = 5         # seconds between chunk retries
_COPY_FALLBACK_RETRIES = 2       # full-copy fallback attempts
_COPY_FALLBACK_DELAY_S = 30      # delay before each fallback retry

_DATA_EXTENSIONS = {".rep", ".txt"}


def _read_chunk_with_retry(f, name: str) -> bytes:
    """Read a single chunk from *f*, retrying on transient OneDrive stalls."""
    for attempt in range(1, _CHUNK_RETRIES + 1):
        try:
            return f.read(_CHUNK_SIZE)
        except (OSError, TimeoutError) as exc:
            if attempt < _CHUNK_RETRIES:
                log.debug(
                    "Chunk read retry %d/%d for %s: %s",
                    attempt, _CHUNK_RETRIES, name, exc,
                )
                time.sleep(_CHUNK_RETRY_DELAY_S)
            else:
                raise


def _copy_with_retry(src: Path, dst: Path) -> bool:
    """Copy *src* to *dst* resilient to OneDrive cloud-sync stalls.

    Phase 1 — streams the file in 1 MB chunks with per-chunk retry.
    Small reads succeed even when OneDrive is still hydrating the file.

    Phase 2 — if chunked copy fails, falls back to ``shutil.copy2``.
    By this point the chunked reads have already forced OneDrive to
    hydrate most of the file, so the bulk copy usually succeeds.

    Returns ``True`` on success, ``False`` after all strategies are exhausted.
    """
    # --- Phase 1: chunked streaming copy ---
    try:
        with open(src, "rb") as fin, open(dst, "wb") as fout:
            while True:
                chunk = _read_chunk_with_retry(fin, src.name)
                if not chunk:
                    break
                fout.write(chunk)
        return True
    except (OSError, TimeoutError) as exc:
        log.warning(
            "Chunked copy failed for %s: %s — trying shutil fallback",
            src.name, exc,
        )
        dst.unlink(missing_ok=True)

    # --- Phase 2: shutil.copy2 fallback (file likely hydrated by now) ---
    for attempt in range(1, _COPY_FALLBACK_RETRIES + 1):
        try:
            time.sleep(_COPY_FALLBACK_DELAY_S)
            shutil.copy2(src, dst)
            return True
        except (OSError, TimeoutError) as exc:
            log.warning(
                "Fallback copy %d/%d failed for %s: %s",
                attempt, _COPY_FALLBACK_RETRIES, src.name, exc,
            )

    log.error("All copy strategies exhausted for %s", src.name)
    return False


@dataclass
class ExtractionResult:
    """Outcome of :func:`extract_rep_files`."""

    rep_files: list[Path] = field(default_factory=list)
    freshly_extracted: bool = False


def _find_data_files(directory: Path, prefix: str) -> list[Path]:
    """Return existing data files matching *prefix* in *directory*.

    Searches for ``.REP``, ``.TXT``, and extensionless files whose upper-cased
    name starts with *prefix*.  Earlier years (e.g. 2021) sometimes have trade
    data extracted as ``.REP.txt``, ``.TXT``, or with no extension at all.
    """
    prefix_upper = prefix.upper()
    matches: list[Path] = []
    if not directory.is_dir():
        return matches
    for f in directory.iterdir():
        if not f.is_file():
            continue
        name_upper = f.name.upper()
        if not name_upper.startswith(prefix_upper):
            continue
        ext = f.suffix.lower()
        if ext in _DATA_EXTENSIONS or ext == "" or ext == ".rar" or ext == ".arj":
            pass
        else:
            continue
        if ext in (".rar", ".arj"):
            continue
        matches.append(f)
    return sorted(matches)


def extract_rep_files(directory: Path, prefix: str) -> ExtractionResult:
    """Return paths to data files for a given *prefix* inside *directory*.

    Checks for already-extracted files first.  If only an archive exists
    (.rar or .arj), extracts into *directory* using ``unar`` and returns the
    resulting file paths.

    Parameters
    ----------
    directory : Path
        Folder such as ``data/2023/MON_01/D01-05/T/``
    prefix : str
        File-name prefix, e.g. ``"FKL501R"`` (case-insensitive match on
        the archive name).

    Returns
    -------
    ExtractionResult
        Contains the list of data file paths and whether they were freshly
        extracted from an archive (``freshly_extracted=True``) or already
        existed on disk (``freshly_extracted=False``).
    """
    existing = _find_data_files(directory, prefix)
    if existing:
        return ExtractionResult(rep_files=existing, freshly_extracted=False)

    archive = _find_archive(directory, prefix)
    if archive is None:
        log.warning("No archive or data files for %s in %s", prefix, directory)
        return ExtractionResult()

    try:
        archive_size = archive.stat().st_size
    except OSError:
        archive_size = -1

    if archive_size == 0:
        log.error(
            "Skipping 0-byte archive %s — likely an OneDrive placeholder "
            "that hasn't been downloaded. Right-click the parent folder in "
            "Finder → 'Always Keep on This Device', then re-run.",
            archive,
        )
        return ExtractionResult()

    log.info("Extracting %s", archive)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_archive = Path(tmpdir) / archive.name
        if not _copy_with_retry(archive, tmp_archive):
            return ExtractionResult()

        try:
            subprocess.run(
                ["unar", "-f", "-o", str(directory), str(tmp_archive)],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            log.error("unar failed on %s: %s", archive, exc.stderr)
            return ExtractionResult()

    rep_files = _find_data_files(directory, prefix)
    if not rep_files:
        extracted_dir = directory / archive.stem
        if extracted_dir.is_dir():
            rep_files = sorted(
                f for f in extracted_dir.iterdir()
                if f.is_file() and f.suffix.lower() not in (".rar", ".arj")
            )

    return ExtractionResult(rep_files=rep_files, freshly_extracted=True)


def remove_extracted_reps(result: ExtractionResult) -> None:
    """Delete data files that were freshly extracted from an archive.

    No-op if ``result.freshly_extracted`` is False (the files pre-existed).
    """
    if not result.freshly_extracted:
        return
    for rep in result.rep_files:
        if not rep.exists():
            continue
        sz = rep.stat().st_size
        rep.unlink()
        log.info("Cleaned up extracted %s (%.1f MB)", rep.name, sz / 1024 / 1024)


def _find_archive(directory: Path, prefix: str) -> Path | None:
    """Locate a .rar or .arj archive for *prefix* in *directory*.

    Uses a two-pass strategy:
    1. Exact prefix match (stem starts with *prefix*, case-insensitive).
    2. Fuzzy match allowing a single repeated character (handles typos like
       ``FKLL504R`` for ``FKL504R``).
    """
    candidates: list[Path] = []
    if not directory.is_dir():
        return None
    for f in directory.iterdir():
        if f.suffix.lower() in (".rar", ".arj") and f.is_file():
            candidates.append(f)
    if not candidates:
        return None

    prefix_upper = prefix.upper()
    for c in candidates:
        if c.stem.upper().startswith(prefix_upper):
            return c

    for c in candidates:
        if _fuzzy_prefix_match(c.stem.upper(), prefix_upper):
            log.info("Fuzzy-matched archive %s for prefix %s", c.name, prefix)
            return c

    if len(candidates) == 1:
        log.info("Single-archive fallback: using %s for prefix %s", candidates[0].name, prefix)
        return candidates[0]

    return None


def _fuzzy_prefix_match(stem: str, prefix: str) -> bool:
    """Check if *stem* matches *prefix* with at most one extra repeated char.

    Handles cases like ``FKLL504R`` matching ``FKL504R`` (extra L).
    """
    si = pi = 0
    skips = 0
    while si < len(stem) and pi < len(prefix):
        if stem[si] == prefix[pi]:
            si += 1
            pi += 1
        elif skips == 0 and si > 0 and stem[si] == stem[si - 1]:
            si += 1
            skips += 1
        else:
            return False
    return pi == len(prefix)


# ---------------------------------------------------------------------------
# Cleanup: delete data files that have a matching archive
# ---------------------------------------------------------------------------

def cleanup_extracted_reps(
    years: list[int],
    dry_run: bool = True,
) -> None:
    """Delete data files that sit alongside a .rar/.arj archive.

    Only processes the specified *years* under ``DATA_DIR``.
    Only deletes files whose name (upper-cased stem) starts with the
    archive's stem, so they can be re-extracted on demand.
    """
    total_bytes = 0
    total_count = 0

    for year in years:
        year_dir = DATA_DIR / str(year)
        if not year_dir.is_dir():
            log.warning("Year directory does not exist: %s", year_dir)
            continue

        for archive in sorted(list(year_dir.rglob("*.rar")) + list(year_dir.rglob("*.arj"))):
            d = archive.parent
            prefix = archive.stem.upper()
            for rep in sorted(d.iterdir()):
                if not rep.is_file() or rep == archive:
                    continue
                if rep.suffix.lower() in (".rar", ".arj"):
                    continue
                if not rep.stem.upper().startswith(prefix):
                    continue
                sz = rep.stat().st_size
                if dry_run:
                    log.info("[DRY RUN] Would delete %s  (%.1f MB)", rep.name, sz / 1024 / 1024)
                else:
                    rep.unlink()
                    log.info("Deleted %s  (%.1f MB)", rep.name, sz / 1024 / 1024)
                total_bytes += sz
                total_count += 1

    action = "Would delete" if dry_run else "Deleted"
    log.info("%s %d files, %.1f GB total", action, total_count, total_bytes / 1024 / 1024 / 1024)


# ---------------------------------------------------------------------------
# Pre-check: verify every archive can be extracted
# ---------------------------------------------------------------------------

def verify_archives(years: list[int], months: list[int] | None = None) -> dict:
    """Extract every .rar/.arj under *years* into a temp dir, then discard.

    Returns a summary dict with ``ok``, ``failed``, and ``failures`` (list of
    paths that could not be extracted).
    """
    ok_count = 0
    fail_count = 0
    failures: list[str] = []

    for year in years:
        year_dir = DATA_DIR / str(year)
        if not year_dir.is_dir():
            log.warning("Year directory does not exist: %s", year_dir)
            continue

        archives = sorted(
            list(year_dir.rglob("*.rar")) + list(year_dir.rglob("*.arj"))
        )

        if months is not None:
            filtered = []
            for a in archives:
                for part in a.parts:
                    if part.startswith("MON_"):
                        try:
                            m = int(part.split("_")[1])
                            if m in months:
                                filtered.append(a)
                        except (IndexError, ValueError):
                            pass
                        break
            archives = filtered

        log.info("Verifying %d archives for year %d ...", len(archives), year)

        for ai, archive in enumerate(archives, 1):
            rel = archive.relative_to(DATA_DIR)

            try:
                archive_size = archive.stat().st_size
            except OSError:
                archive_size = -1
            if archive_size == 0:
                log.error(
                    "[%d/%d] FAIL (0-byte OneDrive placeholder) %s",
                    ai, len(archives), rel,
                )
                fail_count += 1
                failures.append(str(rel))
                continue

            with tempfile.TemporaryDirectory() as tmpdir:
                tmp_path = Path(tmpdir)
                tmp_archive = tmp_path / archive.name

                if not _copy_with_retry(archive, tmp_archive):
                    log.error("[%d/%d] FAIL (copy) %s", ai, len(archives), rel)
                    fail_count += 1
                    failures.append(str(rel))
                    continue

                try:
                    extract_dir = tmp_path / "out"
                    extract_dir.mkdir()
                    subprocess.run(
                        ["unar", "-f", "-o", str(extract_dir), str(tmp_archive)],
                        check=True,
                        capture_output=True,
                        text=True,
                    )
                    data_files = [
                        f for f in extract_dir.rglob("*") if f.is_file()
                    ]
                    if not data_files:
                        log.error(
                            "[%d/%d] FAIL (empty) %s", ai, len(archives), rel
                        )
                        fail_count += 1
                        failures.append(str(rel))
                    else:
                        ok_count += 1
                        log.info(
                            "[%d/%d] OK  %s  (%d file(s))",
                            ai, len(archives), rel, len(data_files),
                        )
                except subprocess.CalledProcessError as exc:
                    log.error(
                        "[%d/%d] FAIL %s: %s",
                        ai, len(archives), rel, exc.stderr.strip(),
                    )
                    fail_count += 1
                    failures.append(str(rel))

    log.info("=" * 60)
    log.info(
        "Verification complete: %d OK, %d FAILED out of %d total",
        ok_count, fail_count, ok_count + fail_count,
    )
    if failures:
        log.error("Failed archives:")
        for f in failures:
            log.error("  %s", f)

    return {"ok": ok_count, "failed": fail_count, "failures": failures}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Archive utilities: verify integrity or clean up extracted files",
    )
    sub = parser.add_subparsers(dest="command")

    # --- verify ---
    verify_p = sub.add_parser("verify", help="Test-extract every archive and report failures")
    verify_p.add_argument("--years", type=int, nargs="+", required=True)
    verify_p.add_argument("--months", type=int, nargs="+", default=None)

    # --- cleanup ---
    clean_p = sub.add_parser("cleanup", help="Delete extracted data files that can be re-extracted")
    clean_p.add_argument("--years", type=int, nargs="+", required=True)
    clean_p.add_argument("--dry-run", action="store_true", default=False)
    clean_p.add_argument("--execute", action="store_true", default=False)

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.command == "verify":
        verify_archives(years=args.years, months=args.months)
    elif args.command == "cleanup":
        if not args.execute and not args.dry_run:
            args.dry_run = True
            log.info("No --execute flag; running in dry-run mode")
        cleanup_extracted_reps(years=args.years, dry_run=not args.execute)
    else:
        parser.print_help()
