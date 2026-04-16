"""
fetch_awards.py -- Download DoD contract awards from USASpending bulk archives.

Downloads agency/FY ZIP files from files.usaspending.gov for DoD agencies.
No NAICS filter by default -- captures all contract types so Angela can search
by scope, ceiling, and contractor across the full DoD portfolio.

Each agency/FY is checkpointed individually. Re-running skips completed files.
Safe to interrupt and resume. DoD files are large (500MB-1GB+ per year) so
expect the first run to take a while.

Run:
    python3 fetch_awards.py                       # all DoD agencies, FY2022-FY2026
    python3 fetch_awards.py --fy 2024 2025 2026   # specific years
    python3 fetch_awards.py --naics-prefix 5415   # filter to IT services only
    python3 fetch_awards.py --force                # re-download everything
"""

import argparse
import csv
import io
import os
import re
import tempfile
import time
import zipfile
from datetime import date
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ARCHIVE_BASE   = "https://files.usaspending.gov/award_data_archive/"
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
OUTPUT_CSV     = Path("data/dod_contracts_bulk.csv")

# DoD toptier agency code -- includes Army, Navy, Air Force, Marines,
# defense agencies (DISA, DARPA, DLA, MDA, etc.) as sub-agencies
DOD_AGENCIES = ["097"]


def _get_latest_datestamp(fallback: str = "20260306") -> str:
    """Fetch the archive index page and find the most recent datestamp."""
    try:
        r = requests.get(ARCHIVE_BASE, timeout=15)
        r.raise_for_status()
        dates = re.findall(r'Contracts_Full_(\d{8})\.zip', r.text)
        if dates:
            latest = max(dates)
            print(f"Auto-detected datestamp: {latest}")
            return latest
    except Exception as exc:
        print(f"Could not auto-detect datestamp ({exc}), using fallback {fallback}")
    return fallback


DATESTAMP = _get_latest_datestamp()


def _current_fy() -> int:
    today = date.today()
    return today.year + 1 if today.month >= 10 else today.year


DEFAULT_YEARS = list(range(_current_fy(), _current_fy() - 5, -1))  # 5 years

# No column filter -- keep ALL columns from the source CSV.
# USASpending bulk archives have ~200 columns. Re-downloading is expensive
# (hours for DoD), so we save everything and filter at the build step.
# The build_dashboard.py script picks out the columns it needs.

# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

NOT_FOUND  = "NOT_FOUND"
IP_BLOCKED = "IP_BLOCKED"
FAILED     = "FAILED"


def download_zip(url: str, max_retries: int = 3) -> str:
    """Download zip to a temp file. Returns temp path or sentinel string."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, stream=True, timeout=600)
            if r.status_code == 404:
                return NOT_FOUND
            if r.status_code >= 500:
                return IP_BLOCKED
            r.raise_for_status()
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
            downloaded = 0
            last_print = 0
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
                    downloaded += len(chunk)
                    mb = downloaded / 1024 / 1024
                    if mb - last_print >= 50:
                        print(f"{mb:.0f}MB...", end=" ", flush=True)
                        last_print = mb
            tmp.close()
            return tmp.name
        except requests.exceptions.ConnectionError:
            return IP_BLOCKED
        except Exception as exc:
            wait = min(30 * (attempt + 1), 180)
            print(f"\n    retry {attempt+1}/{max_retries} in {wait}s ({exc})...")
            time.sleep(wait)
    return FAILED


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------

def checkpoint_path(fy: int, code: str) -> Path:
    return CHECKPOINT_DIR / f"FY{fy}_{code}.csv"


def not_found_path(fy: int, code: str) -> Path:
    return CHECKPOINT_DIR / f"FY{fy}_{code}.not_found"


def is_done(fy: int, code: str) -> bool:
    return checkpoint_path(fy, code).exists() or not_found_path(fy, code).exists()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Download DoD contracts from USASpending bulk archives")
    parser.add_argument("--fy", nargs="+", type=int, default=DEFAULT_YEARS,
                        help=f"Fiscal years to download (default: {DEFAULT_YEARS})")
    parser.add_argument("--agencies", nargs="+", default=DOD_AGENCIES,
                        help=f"Agency codes (default: {DOD_AGENCIES} = DoD)")
    parser.add_argument("--naics-prefix", nargs="+", default=None,
                        help="Optional NAICS prefix filter (default: all)")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if checkpoint exists")
    args = parser.parse_args()

    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    agencies = {c: f"Agency {c}" for c in args.agencies}

    if args.force:
        for fy in args.fy:
            for code in agencies:
                for p in [checkpoint_path(fy, code), not_found_path(fy, code)]:
                    if p.exists():
                        print(f"  --force: removing {p.name}")
                        p.unlink()

    already = sum(1 for fy in args.fy for c in agencies if is_done(fy, c))
    todo    = sum(1 for fy in args.fy for c in agencies if not is_done(fy, c))
    naics_msg = f"NAICS filter: {args.naics_prefix}" if args.naics_prefix else "NAICS filter: NONE (all contracts)"
    print(f"Already done: {already}  To download: {todo}")
    print(f"Years: {args.fy}")
    print(f"Agencies: {list(agencies.keys())}")
    print(f"{naics_msg}\n")

    ip_blocked = False
    total_kept = 0
    total_scanned = 0

    for fy in args.fy:
        if ip_blocked:
            break
        fy_done = sum(1 for c in agencies if is_done(fy, c))
        fy_todo = len(agencies) - fy_done
        print(f"\n{'='*60}")
        print(f"FY{fy}  --  {fy_done} done, {fy_todo} to download")
        print(f"{'='*60}")

        for code in agencies:
            if is_done(fy, code):
                continue

            url = f"{ARCHIVE_BASE}FY{fy}_{code}_Contracts_Full_{DATESTAMP}.zip"
            print(f"  [{code}] downloading FY{fy}...", end=" ", flush=True)
            resp = download_zip(url)

            if resp is IP_BLOCKED:
                print("IP BLOCKED -- stopping.")
                ip_blocked = True
                break
            if resp is FAILED:
                print("FAILED -- will retry next run")
                continue
            if resp is NOT_FOUND:
                print("404")
                not_found_path(fy, code).touch()
                continue

            zip_path = resp
            zip_mb = os.path.getsize(zip_path) / 1024 / 1024
            print(f"{zip_mb:.1f} MB  |  scanning...", end=" ", flush=True)

            rows_scanned = 0
            rows_kept = 0
            cp = checkpoint_path(fy, code)

            try:
                with zipfile.ZipFile(zip_path) as zf:
                    csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                    if not csv_names:
                        print("no CSV in zip")
                        cp.touch()
                        continue

                    with open(cp, "w", newline="", encoding="utf-8") as out_f:
                        writer = None  # initialized from first CSV's header

                        for csv_name in csv_names:
                            with zf.open(csv_name) as raw:
                                reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))

                                # Use the source CSV's own columns -- keep everything
                                if writer is None:
                                    fieldnames = reader.fieldnames
                                    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                                    writer.writeheader()

                                for row in reader:
                                    rows_scanned += 1
                                    if rows_scanned % 500_000 == 0:
                                        print(f"{rows_scanned//1000}k...", end=" ", flush=True)

                                    # Optional NAICS filter
                                    if args.naics_prefix:
                                        naics = (row.get("naics_code") or "").strip()
                                        if not any(naics.startswith(p) for p in args.naics_prefix):
                                            continue

                                    writer.writerow(row)
                                    rows_kept += 1

            except Exception as exc:
                print(f"\n    ERROR: {exc}")
                if cp.exists():
                    cp.unlink()
                os.unlink(zip_path)
                continue

            os.unlink(zip_path)
            total_kept += rows_kept
            total_scanned += rows_scanned
            print(f"scanned {rows_scanned:,}  ->  kept {rows_kept:,} rows")

    # Merge all checkpoints into one CSV
    print(f"\n{'='*60}")
    print("Merging checkpoints...")

    # Discover the union of all column headers across checkpoints
    all_fieldnames = []
    seen = set()
    checkpoint_files = sorted(CHECKPOINT_DIR.glob("FY*.csv"))
    for cp_file in checkpoint_files:
        if cp_file.stat().st_size > 0:
            try:
                with open(cp_file, newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for col in (reader.fieldnames or []):
                        if col not in seen:
                            all_fieldnames.append(col)
                            seen.add(col)
            except Exception:
                pass

    if not all_fieldnames:
        print("No contracts found yet.")
        return

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for cp_file in checkpoint_files:
            if cp_file.stat().st_size == 0:
                continue
            try:
                with open(cp_file, newline="", encoding="utf-8") as cf:
                    reader = csv.DictReader(cf)
                    for row in reader:
                        writer.writerow(row)
                        total_rows += 1
            except Exception:
                pass

    num_files = sum(1 for cp_file in checkpoint_files if cp_file.stat().st_size > 0)
    print(f"Wrote {total_rows:,} rows to {OUTPUT_CSV}")
    print(f"From {num_files} agency/FY files")

    status = "blocked" if ip_blocked else "done"
    Path("data/scan_status.txt").write_text(status)

    if ip_blocked:
        print(f"\nIP blocked -- re-run to continue. Progress saved.")
    else:
        print("\nDone!")


if __name__ == "__main__":
    main()
