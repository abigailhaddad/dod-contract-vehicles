"""
enrich_sam.py -- Enrich DoD contracts with SAM.gov entity data.

Uses the SAM.gov monthly bulk extract -- one API call downloads a full
snapshot of all registered entities (~1-3GB zip). We filter to just the
UEIs in our DoD contracts dataset and save data/sam_lookup.csv.

This gives us contractor details that Angela needs:
  - Legal business name, DBA
  - Entity start date (how old is the company?)
  - SAM registration date
  - City, state (contractor location, not place of performance)
  - Number of employees
  - SBA certifications (8(a), HUBZone, SDVOSB, WOSB, etc.)
  - Business type codes

Output: data/sam_lookup.csv

Setup:
  1. Get a free API key at sam.gov (Account Details -> API Keys)
  2. Add to .env: SAM_API_KEY=your_key_here

Run:
    python3 enrich_sam.py
"""

import csv
import io
import os
import sys
import time
import zipfile
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

BULK_CSV      = Path("data/dod_contracts_bulk.csv")
OUTPUT_CSV    = Path("data/sam_lookup.csv")
EXTRACT_CACHE = Path("data/sam_extract_cache.zip")
EXTRACT_URL   = "https://api.sam.gov/data-services/v1/extracts"


def collect_ueis() -> set[str]:
    if not BULK_CSV.exists():
        raise FileNotFoundError(f"{BULK_CSV} not found -- run fetch_awards.py first.")
    ueis = set()
    with open(BULK_CSV) as f:
        for row in csv.DictReader(f):
            uei = (row.get("recipient_uei") or "").strip()
            if uei:
                ueis.add(uei)
    print(f"Unique UEIs in DoD contracts: {len(ueis):,}")
    return ueis


def _get_redirect_url(params: dict, retries: int = 3) -> str | None:
    for attempt in range(retries):
        resp = requests.get(EXTRACT_URL, params=params, timeout=60, allow_redirects=False)
        if resp.status_code in (301, 302, 303, 307, 308):
            return resp.headers.get("location") or resp.headers.get("Location") or ""
        if resp.status_code == 429:
            wait = 60 * (attempt + 1)
            print(f"  Rate limited -- waiting {wait}s before retry {attempt+1}/{retries}...")
            time.sleep(wait)
            continue
        print(f"  Unexpected HTTP {resp.status_code}: {resp.text[:200]}")
        return None
    return None


def download_extract(api_key: str) -> Path:
    if EXTRACT_CACHE.exists():
        size_mb = EXTRACT_CACHE.stat().st_size // 1_000_000
        print(f"Using cached extract: {EXTRACT_CACHE} ({size_mb}MB)")
        return EXTRACT_CACHE

    print("Requesting SAM.gov monthly entity extract URL...")
    today = date.today()
    params = {
        "api_key":     api_key,
        "fileType":    "ENTITY",
        "sensitivity": "PUBLIC",
        "frequency":   "MONTHLY",
        "date":        today.strftime("%m/%Y"),
    }
    url = _get_redirect_url(params)
    if not url:
        from datetime import timedelta
        prev = (today.replace(day=1) - timedelta(days=1))
        params["date"] = prev.strftime("%m/%Y")
        print(f"  Current month not available, trying {params['date']}...")
        url = _get_redirect_url(params)
    if not url:
        raise RuntimeError("Could not get SAM extract download URL. Check SAM_API_KEY.")

    print(f"Downloading extract (~1-3GB)...")
    EXTRACT_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(EXTRACT_CACHE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    print(f"\r  {downloaded // 1_000_000}MB / {total // 1_000_000}MB", end="", flush=True)
    print(f"\nDownloaded -> {EXTRACT_CACHE}")
    return EXTRACT_CACHE


# SAM Public V2 monthly .dat file is pipe-delimited with NO header row.
# Field positions confirmed by matching known UEIs against the extract.
SAM_FIELD_POSITIONS = {
    0:  "uei",                   # UEI SAM (12-char unique entity identifier)
    7:  "sam_registration_date", # YYYYMMDD
    11: "legal_business_name",
    17: "city",
    18: "state",
    24: "entity_start_date",     # YYYYMMDD
}


def parse_extract(zip_path: Path, target_ueis: set[str]) -> list[dict]:
    """Parse SAM V2 pipe-delimited .dat file (no header row) using positional indices."""
    print("Parsing extract (scanning for matching UEIs)...")
    matches = []

    with zipfile.ZipFile(zip_path) as zf:
        data_files = [n for n in zf.namelist()
                      if not n.endswith("/") and any(n.lower().endswith(e) for e in (".dat", ".csv", ".txt"))]
        if not data_files:
            data_files = [n for n in zf.namelist() if not n.endswith("/")]
        print(f"  Files in ZIP: {data_files}")

        for dat_file in data_files:
            with zf.open(dat_file) as raw:
                scanned = 0
                for line in io.TextIOWrapper(raw, encoding="utf-8-sig", errors="replace"):
                    line = line.rstrip("\n\r")
                    if not line or len(line) < 12:
                        continue
                    fields = line.split("|")
                    uei = fields[0].strip() if fields else ""
                    if " " in uei or len(uei) != 12:
                        continue
                    scanned += 1
                    if scanned % 500_000 == 0:
                        print(f"  ...scanned {scanned:,} rows, {len(matches):,} matches so far")
                    if uei not in target_ueis:
                        continue

                    out = {}
                    for pos, name in SAM_FIELD_POSITIONS.items():
                        val = fields[pos].strip() if pos < len(fields) else ""
                        if name.endswith("_date") and len(val) == 8 and val.isdigit():
                            val = f"{val[:4]}-{val[4:6]}-{val[6:8]}"
                        out[name] = val
                    matches.append(out)

                print(f"  Scanned {scanned:,} rows, matched {len(matches):,} UEIs")

    print(f"Found {len(matches):,} matching entities")
    return matches


def main():
    api_key = os.environ.get("SAM_API_KEY")
    if not api_key:
        print("SAM_API_KEY not set. Get a free key at sam.gov -> Account Details -> API Keys.")
        print("Add it to .env: SAM_API_KEY=your_key_here")
        return

    ueis = collect_ueis()
    zip_path = download_extract(api_key)
    matches = parse_extract(zip_path, ueis)

    if not matches:
        print("No matches found -- check that dod_contracts_bulk.csv has valid UEIs.")
        return

    import pandas as pd
    df = pd.DataFrame(matches).drop_duplicates(subset="uei")
    df.to_csv(OUTPUT_CSV, index=False)

    coverage = len(df) / len(ueis) * 100
    print(f"\nSaved {len(df):,} entities -> {OUTPUT_CSV}")
    print(f"Coverage: {coverage:.1f}% of UEIs in DoD contracts")

    has_start = (df["entity_start_date"] != "").sum()
    print(f"  Has entity_start_date: {has_start:,} ({has_start/len(df)*100:.0f}%)")


if __name__ == "__main__":
    main()
