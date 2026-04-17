"""
build_dashboard.py -- Aggregate DoD contract data and build dashboard JSONs.

Streams transaction-level CSV data row by row (no pandas for the 18M+ row
load), accumulates into dict-based accumulators keyed by contract ID, then
outputs JSON files to web/data/ for the dashboard.

Memory usage: O(unique contracts) not O(total transactions).
A GitHub Actions runner with 7GB RAM handles this fine.

Two views:
  1. Contracts -- one row per contract_award_unique_key
  2. Vehicles  -- grouped by parent_award_id_piid (IDIQ/BPA rollup)

Run:
    python3 build_dashboard.py
"""

import csv
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import yaml

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

BULK_CSV       = Path("data/dod_contracts_bulk.csv")
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
SAM_CSV        = Path("data/sam_lookup.csv")
WEB_DATA_DIR   = Path("web/data")
CONFIG_PATH    = Path("config.yaml")

TODAY = date.today()
TODAY_STR = TODAY.isoformat()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

with open(CONFIG_PATH) as _f:
    CONFIG = yaml.safe_load(_f)

CLASSIFY              = CONFIG["classification"]
EXPIRING_SOON_DAYS    = int(CLASSIFY["expiring_soon_days"])
EFFECTIVE_END_FIELDS  = list(CLASSIFY["effective_end_fields"])
DROP_EXPIRED          = bool(CLASSIFY["drop_expired"])
DROP_UNKNOWN          = bool(CLASSIFY["drop_unknown"])

EXPIRING_SOON_CUTOFF  = (TODAY + timedelta(days=EXPIRING_SOON_DAYS)).isoformat()

PARENT_AWARD_TYPE_LABELS = dict(CONFIG["labels"]["parent_award_types"])
PRICING_LABELS           = dict(CONFIG["labels"]["pricing_types"])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _val(row: dict, key: str) -> str | None:
    """Get a non-empty string value from a row, or None."""
    v = row.get(key, "")
    if v and str(v).strip() and str(v).strip().lower() not in ("nan", "none", ""):
        return str(v).strip()
    return None


def _float(row: dict, key: str) -> float | None:
    v = _val(row, key)
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _best_description(award_desc, base_desc, txn_desc) -> str | None:
    """Pick the longest non-null description."""
    candidates = [s for s in (base_desc, award_desc, txn_desc) if s]
    if not candidates:
        return None
    return max(candidates, key=len)


def _effective_end(row_or_dict: dict, fields: list = None) -> str:
    """Return the max (latest) date string among the configured effective-end fields.
    Empty string if none are populated. Inputs may be raw CSV rows or aggregated
    contract dicts — both use the same field names."""
    if fields is None:
        fields = EFFECTIVE_END_FIELDS
    best = ""
    for f in fields:
        v = (row_or_dict.get(f) or "")
        if isinstance(v, str):
            v = v.strip()[:10]
        if v and v > best:
            best = v
    return best


def _classify_status(effective_end_str: str | None) -> str:
    """Classify by effective end date. Returns Active / Expiring Soon / Expired /
    Unknown. Unknown means unparseable or missing date; Expired means end < today."""
    if not effective_end_str:
        return "Unknown"
    try:
        end = datetime.strptime(effective_end_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return "Unknown"
    if end < TODAY:
        return "Expired"
    if (end - TODAY).days <= EXPIRING_SOON_DAYS:
        return "Expiring Soon"
    return "Active"


def _sam_url(sol_id: str | None) -> str | None:
    if not sol_id:
        return None
    return f"https://sam.gov/search/?keywords={sol_id}&index=opp"


# ---------------------------------------------------------------------------
# Streaming aggregation
# ---------------------------------------------------------------------------

def _r2_client():
    """Create an R2 client if credentials are available."""
    import os
    account_id = os.environ.get("CF_R2_ACCOUNT_ID")
    if not account_id:
        return None, None
    import boto3
    from botocore.config import Config
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ["CF_R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["CF_R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )
    return s3, os.environ["CF_R2_BUCKET"]


def _stream_local_csv(path: Path):
    """Stream a local CSV row by row."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        yield from reader


def stream_and_aggregate() -> dict:
    """Stream all transaction rows, accumulate one entry per contract_award_unique_key.

    For each contract, we keep the latest transaction's values (by action_date).
    federal_action_obligation is summed across all transactions.
    Memory: ~2-4KB per unique contract, ~500K-1M contracts = ~2-4GB peak.

    Data sources (in priority order):
      1. Local merged CSV (data/dod_contracts_bulk.csv)
      2. Local checkpoint files (data/bulk_checkpoints/FY*.csv)
      3. R2 checkpoint files, downloaded one at a time to scratch disk
         and deleted after processing (to fit within the runner's ~14GB
         root-partition limit; five FY files collectively exceed it).

    Single pass: each source is streamed once; descriptions and all
    other fields are captured inline. No more two-pass scheme -- the
    drop-expired filter keeps memory manageable on its own.
    """
    # Determine data sources as (name, callable -> Path or iterator).
    # For R2 we yield (name, callable that downloads + returns local Path);
    # the loop deletes the file after processing.
    local_sources = []  # (name, Path)
    r2_pairs = []       # (name, R2 key)

    if BULK_CSV.exists():
        local_sources.append((BULK_CSV.name, BULK_CSV))
    else:
        local_cps = sorted(CHECKPOINT_DIR.glob("FY*.csv"))
        local_cps = [cp for cp in local_cps if cp.stat().st_size > 0]
        if local_cps:
            for cp in local_cps:
                local_sources.append((cp.name, cp))

    s3 = bucket = None
    if not local_sources:
        s3, bucket = _r2_client()
        if s3:
            prefix = "dod_vehicles/"
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    name = Path(key).name
                    if name.startswith("FY") and name.endswith(".csv"):
                        r2_pairs.append((name, key))

    total_count = len(local_sources) + len(r2_pairs)
    if total_count == 0:
        raise FileNotFoundError("No data found -- run fetch_awards.py first.")

    print(f"Reading from {total_count} source(s)...")

    # contracts[key] = {field: value, ...} — latest transaction wins
    contracts = {}
    # Track sum of federal_action_obligation per contract (it's per-transaction)
    fao_sums = defaultdict(float)

    skipped_expired = 0
    total_rows = 0
    scratch_dir = None

    def _process_source(src_name: str, path: Path):
        nonlocal total_rows, skipped_expired
        print(f"  Streaming {src_name}...", end=" ", flush=True)
        file_rows = 0
        for row in _stream_local_csv(path):
            total_rows += 1
            file_rows += 1
            if total_rows % 2_000_000 == 0:
                print(f"{total_rows // 1_000_000}M...", end=" ", flush=True)

            key = _val(row, "contract_award_unique_key")
            if not key:
                continue

            eff_end = _effective_end(row)
            if DROP_EXPIRED and eff_end and eff_end < TODAY_STR:
                skipped_expired += 1
                continue

            action_date = _val(row, "action_date") or ""

            fao = _float(row, "federal_action_obligation")
            if fao:
                fao_sums[key] += fao

            existing = contracts.get(key)
            if existing and existing.get("_action_date", "") > action_date:
                continue

            contracts[key] = {
                "_action_date":       action_date,
                "key":                key,
                "piid":               _val(row, "award_id_piid"),
                "parent_piid":        _val(row, "parent_award_id_piid"),
                "obligated":          _float(row, "total_dollars_obligated"),
                "ceiling":            _float(row, "potential_total_value_of_award"),
                "pop_start":          _val(row, "period_of_performance_start_date"),
                "pop_end":            _val(row, "period_of_performance_current_end_date"),
                "pop_potential_end":  _val(row, "period_of_performance_potential_end_date"),
                "ordering_period_end": _val(row, "ordering_period_end_date"),
                "effective_end":      eff_end or None,
                "department":         _val(row, "awarding_agency_name"),
                "sub_agency":         _val(row, "awarding_sub_agency_name"),
                "awarding_office":    _val(row, "awarding_office_name"),
                "funding_office":     _val(row, "funding_office_name"),
                "recipient_uei":      _val(row, "recipient_uei"),
                "recipient_name":     _val(row, "recipient_name"),
                "recipient_parent":   _val(row, "recipient_parent_name"),
                "parent_award_type_code": _val(row, "parent_award_type_code"),
                "parent_award_type":  _val(row, "parent_award_type"),
                "award_type_code":    _val(row, "award_type_code"),
                "award_description":  _val(row, "award_description"),
                "base_description":   _val(row, "prime_award_base_transaction_description"),
                "txn_description":    _val(row, "transaction_description"),
                "naics_code":         _val(row, "naics_code"),
                "naics_description":  _val(row, "naics_description"),
                "psc_code":           _val(row, "product_or_service_code"),
                "psc_description":    _val(row, "product_or_service_code_description"),
                "solicitation_id":    _val(row, "solicitation_identifier"),
                "set_aside":          _val(row, "type_of_set_aside_code") or "NONE",
                "pricing_type":       _val(row, "type_of_contract_pricing_code"),
                "pricing_label":      _val(row, "type_of_contract_pricing"),
                "num_offers":         _val(row, "number_of_offers_received"),
                "place_state":        _val(row, "primary_place_of_performance_state_code"),
                "business_size_label": _val(row, "contracting_officers_determination_of_business_size"),
                "usaspending_link":   _val(row, "usaspending_permalink"),
            }
        print(f"{file_rows:,} rows")

    # Process local sources first, then R2 (one file at a time: download,
    # process, delete). Keeps disk usage capped at ~one file at a time.
    for name, path in local_sources:
        _process_source(name, path)

    if r2_pairs:
        import tempfile
        scratch_dir = Path(tempfile.mkdtemp(prefix="r2_cache_"))
        print(f"Downloading R2 sources one-at-a-time to {scratch_dir}...")
        for name, r2_key in r2_pairs:
            local = scratch_dir / name
            print(f"  Downloading {name}...", end=" ", flush=True)
            s3.download_file(bucket, r2_key, str(local))
            print(f"{local.stat().st_size // (1024*1024):,} MB", flush=True)
            try:
                _process_source(name, local)
            finally:
                try:
                    local.unlink()
                except OSError:
                    pass
        try:
            scratch_dir.rmdir()
        except OSError:
            pass

    print(f"  Total: {total_rows:,} transactions, {skipped_expired:,} skipped (expired), {len(contracts):,} unique contracts")

    # fao fallback for contracts missing total_dollars_obligated
    for key, c in contracts.items():
        if not c["obligated"] or c["obligated"] == 0:
            c["obligated"] = fao_sums.get(key)

    return contracts


# ---------------------------------------------------------------------------
# Derived fields + JSON builders
# ---------------------------------------------------------------------------

def enrich_contracts(contracts: dict) -> dict:
    """Add derived fields to each contract dict. Also drops Unknown (and Expired,
    if still present) when configured — applied here rather than streaming so
    later transactions can fill in a missing end date."""
    for c in contracts.values():
        # Ceiling remaining
        ceiling = c.get("ceiling")
        obligated = c.get("obligated")
        if ceiling is not None and obligated is not None:
            c["ceiling_remaining"] = ceiling - obligated
        else:
            c["ceiling_remaining"] = None

        # Status (from effective_end computed during streaming)
        c["status"] = _classify_status(c.get("effective_end"))

        # IDV master = the vehicle itself (IDIQ/BPA/GWAC record), not an order
        # against one. Identified by award_type_code starting with IDV, or the
        # CONT_IDV_ key prefix as fallback. IDV masters belong on the Vehicles
        # view only -- including them in Contracts sums double-counts ceiling
        # against their task orders.
        awt = c.get("award_type_code") or ""
        c["is_idv_master"] = awt.startswith("IDV") or (c.get("key") or "").startswith("CONT_IDV_")

        # Vehicle type
        c["vehicle_type"] = PARENT_AWARD_TYPE_LABELS.get(
            c.get("parent_award_type_code") or "", "Standalone"
        )

        # Pricing label
        c["pricing_type_label"] = PRICING_LABELS.get(
            c.get("pricing_type") or "", c.get("pricing_label")
        )

        # Best description
        c["description"] = _best_description(
            c.get("award_description"),
            c.get("base_description"),
            c.get("txn_description"),
        )

        # SAM solicitation URL
        c["sam_url"] = _sam_url(c.get("solicitation_id"))

        # Has parent vehicle
        c["has_parent"] = bool(c.get("parent_piid"))

    return contracts


def build_contracts_json(contracts: dict) -> list:
    """Build JSON for all contract awards except IDV masters (those appear
    in the Vehicles view instead)."""
    records = []
    for c in contracts.values():
        if c.get("is_idv_master"):
            continue
        status = c["status"]
        records.append({
            "key":                c["key"],
            "piid":               c.get("piid"),
            "parent_piid":        c.get("parent_piid"),
            "contractor":         c.get("recipient_name"),
            "contractor_parent":  c.get("recipient_parent"),
            "department":         c.get("department"),
            "sub_agency":         c.get("sub_agency"),
            "awarding_office":    c.get("awarding_office"),
            "funding_office":     c.get("funding_office"),
            "description":        c.get("description"),
            "solicitation_id":    c.get("solicitation_id"),
            "sam_url":            c.get("sam_url"),
            "naics":              c.get("naics_code"),
            "naics_desc":         c.get("naics_description"),
            "psc":                c.get("psc_code"),
            "psc_desc":           c.get("psc_description"),
            "pricing":            c.get("pricing_type_label"),
            "vehicle_type":       c.get("vehicle_type"),
            "set_aside":          c.get("set_aside"),
            "business_size":      c.get("business_size_label"),
            "status":             status,
            "pop_start":          (c.get("pop_start") or "")[:10] or None,
            "pop_end":            (c.get("pop_end") or "")[:10] or None,
            "pop_potential_end":  (c.get("pop_potential_end") or "")[:10] or None,
            "ordering_period_end": (c.get("ordering_period_end") or "")[:10] or None,
            "effective_end":      (c.get("effective_end") or "")[:10] or None,
            "ceiling":            round(c["ceiling"]) if c.get("ceiling") else None,
            "obligated":          round(c["obligated"]) if c.get("obligated") else None,
            "ceiling_remaining":  round(c["ceiling_remaining"]) if c.get("ceiling_remaining") is not None else None,
            "place_state":        c.get("place_state"),
            "link":               c.get("usaspending_link"),
        })

    # Sort: active first, then by ceiling remaining desc
    status_order = {"Active": 0, "Expiring Soon": 1, "Unknown": 2, "Expired": 3}
    records.sort(key=lambda r: (status_order.get(r["status"], 9), -(r["ceiling_remaining"] or 0)))

    return records


def build_vehicles_json(contracts: dict) -> list:
    """Group contracts by parent PIID for vehicle-level rollup.

    Two inputs feed a vehicle:
      - Task orders under the IDV (grouped by their parent_piid).
      - The IDV master record itself (keyed by its own piid), when present.
        Supplies the vehicle's own ceiling and effective_end, which are more
        authoritative than task-order-level aggregations.
    """
    # Index IDV masters by their piid for O(1) lookup during rollup.
    idv_masters = {
        c["piid"]: c for c in contracts.values()
        if c.get("is_idv_master") and c.get("piid")
    }

    vehicles = defaultdict(lambda: {
        "order_count": 0, "active_orders": 0,
        "total_obligated": 0.0, "total_ceiling": 0.0,
        "contractors": set(), "naics_codes": set(),
        "naics_descriptions": set(), "descriptions": set(),
        "states": set(),
        "department": None, "sub_agency": None, "awarding_office": None,
        "vehicle_type": None, "parent_award_type": None,
        "earliest_start": None, "latest_end": None, "latest_potential_end": None,
        "latest_effective_end": None,
        "vehicle_ceiling": None,     # IDV master's own potential_total_value
        "vehicle_obligated": None,   # IDV master's own obligated
        "all_orders": [],            # for top-N selection after rollup
    })

    for c in contracts.values():
        if c.get("is_idv_master"):
            continue
        parent = c.get("parent_piid")
        if not parent:
            continue

        v = vehicles[parent]
        v["order_count"] += 1
        if c["status"] in ("Active", "Expiring Soon"):
            v["active_orders"] += 1
        v["total_obligated"] += c.get("obligated") or 0
        v["total_ceiling"] += c.get("ceiling") or 0

        # Collect order slim record for drill-down (filtered + sorted after loop).
        v["all_orders"].append({
            "piid":       c.get("piid"),
            "contractor": c.get("recipient_name"),
            "ceiling":    c.get("ceiling"),
            "obligated":  c.get("obligated"),
            "pop_end":    (c.get("pop_end") or "")[:10] or None,
            "pop_potential_end": (c.get("pop_potential_end") or "")[:10] or None,
            "status":     c.get("status"),
            "link":       c.get("usaspending_link"),
        })

        if c.get("recipient_name"):
            v["contractors"].add(c["recipient_name"])
        if c.get("naics_code"):
            v["naics_codes"].add(c["naics_code"])
        if c.get("naics_description"):
            v["naics_descriptions"].add(c["naics_description"])
        if c.get("award_description"):
            v["descriptions"].add(c["award_description"])
        if c.get("place_state"):
            v["states"].add(c["place_state"])

        # Take first non-null for categorical fields
        for field in ("department", "sub_agency", "awarding_office",
                      "vehicle_type", "parent_award_type"):
            if not v[field] and c.get(field):
                v[field] = c[field]

        # Date ranges
        ps = (c.get("pop_start") or "")[:10]
        pe = (c.get("pop_end") or "")[:10]
        ppe = (c.get("pop_potential_end") or "")[:10]
        ee = (c.get("effective_end") or "")[:10]
        if ps and (not v["earliest_start"] or ps < v["earliest_start"]):
            v["earliest_start"] = ps
        if pe and (not v["latest_end"] or pe > v["latest_end"]):
            v["latest_end"] = pe
        if ppe and (not v["latest_potential_end"] or ppe > v["latest_potential_end"]):
            v["latest_potential_end"] = ppe
        if ee and (not v["latest_effective_end"] or ee > v["latest_effective_end"]):
            v["latest_effective_end"] = ee

    # Fold in IDV master info: add its ceiling/obligated as the vehicle's
    # own, and use its effective_end to refine latest_effective_end.
    for parent_piid, v in vehicles.items():
        master = idv_masters.get(parent_piid)
        if not master:
            continue
        if master.get("ceiling"):
            v["vehicle_ceiling"] = master["ceiling"]
        if master.get("obligated"):
            v["vehicle_obligated"] = master["obligated"]
        master_ee = (master.get("effective_end") or "")[:10]
        if master_ee and master_ee > (v["latest_effective_end"] or ""):
            v["latest_effective_end"] = master_ee

    # Include IDV masters that have no task orders yet (so empty-but-live
    # vehicles are still findable).
    for piid, master in idv_masters.items():
        if piid in vehicles:
            continue
        v = vehicles[piid]  # creates fresh entry via defaultdict
        v["vehicle_ceiling"] = master.get("ceiling")
        v["vehicle_obligated"] = master.get("obligated")
        v["latest_effective_end"] = (master.get("effective_end") or "")[:10] or None
        v["latest_end"] = (master.get("pop_end") or "")[:10] or None
        v["latest_potential_end"] = (master.get("pop_potential_end") or "")[:10] or None
        v["earliest_start"] = (master.get("pop_start") or "")[:10] or None
        if master.get("recipient_name"):
            v["contractors"].add(master["recipient_name"])
        for field in ("department", "sub_agency", "awarding_office"):
            v[field] = master.get(field)
        # IDV master's award_type_code (e.g. "IDV_B") maps to vehicle_type
        v["vehicle_type"] = PARENT_AWARD_TYPE_LABELS.get(
            master.get("award_type_code") or "", v["vehicle_type"]
        )

    # Build JSON records. Vehicle status from its latest effective end across
    # all contributing orders; apply configured drop policy.
    records = []
    for parent_piid, v in vehicles.items():
        status = _classify_status(v["latest_effective_end"])
        if DROP_EXPIRED and status == "Expired":
            continue
        if DROP_UNKNOWN and status == "Unknown":
            continue

        remaining = v["total_ceiling"] - v["total_obligated"]
        pct_used = round(v["total_obligated"] / v["total_ceiling"] * 100, 1) if v["total_ceiling"] > 0 else None

        # Top 10 orders for drill-down: live (Active/Expiring Soon) only,
        # sorted by ceiling desc.
        live_orders = [o for o in v["all_orders"] if o.get("status") in ("Active", "Expiring Soon")]
        live_orders.sort(key=lambda o: -(o.get("ceiling") or 0))
        top_orders = [
            {
                "piid":       o["piid"],
                "contractor": o["contractor"],
                "ceiling":    round(o["ceiling"]) if o.get("ceiling") else None,
                "obligated":  round(o["obligated"]) if o.get("obligated") else None,
                "pop_end":    o["pop_end"],
                "pop_potential_end": o["pop_potential_end"],
                "status":     o["status"],
                "link":       o["link"],
            }
            for o in live_orders[:10]
        ]

        records.append({
            "parent_piid":        parent_piid,
            "vehicle_type":       v["vehicle_type"],
            "parent_award_type":  v["parent_award_type"],
            "department":         v["department"],
            "sub_agency":         v["sub_agency"],
            "awarding_office":    v["awarding_office"],
            "order_count":        v["order_count"],
            "active_orders":      v["active_orders"],
            "contractor_count":   len(v["contractors"]),
            "contractors":        sorted(v["contractors"])[:10],
            "naics_codes":        sorted(v["naics_codes"])[:10],
            "naics_descriptions": sorted(v["naics_descriptions"])[:5],
            "descriptions":       sorted(v["descriptions"])[:5],
            "status":             status,
            "earliest_start":     v["earliest_start"],
            "latest_end":         v["latest_end"],
            "latest_potential_end": v["latest_potential_end"],
            "latest_effective_end": v["latest_effective_end"],
            "total_ceiling":      round(v["total_ceiling"]) if v["total_ceiling"] else None,
            "total_obligated":    round(v["total_obligated"]) if v["total_obligated"] else None,
            "vehicle_ceiling":    round(v["vehicle_ceiling"]) if v.get("vehicle_ceiling") else None,
            "vehicle_obligated":  round(v["vehicle_obligated"]) if v.get("vehicle_obligated") else None,
            "ceiling_remaining":  round(remaining),
            "pct_ceiling_used":   pct_used,
            "top_orders":         top_orders,
            "states":             sorted(v["states"])[:10],
        })

    records.sort(key=lambda r: -(r["ceiling_remaining"] or 0))
    return records


def build_summary(contracts: dict, vehicles_json: list) -> dict:
    # Summary excludes IDV masters -- they live on the Vehicles view and
    # would double-count ceiling against their task orders here.
    IN_FORCE = ("Active", "Expiring Soon")
    non_idv = [c for c in contracts.values() if not c.get("is_idv_master")]
    in_force = [c for c in non_idv if c["status"] in IN_FORCE]

    active_count = len(in_force)
    expiring_count = sum(1 for c in non_idv if c["status"] == "Expiring Soon")
    active_ceiling = sum(c.get("ceiling") or 0 for c in in_force)
    active_obligated = sum(c.get("obligated") or 0 for c in in_force)
    active_remaining = active_ceiling - active_obligated
    active_contractors = len({c["recipient_name"] for c in in_force if c.get("recipient_name")})
    active_offices = len({c["awarding_office"] for c in in_force if c.get("awarding_office")})
    active_vehicles = sum(1 for v in vehicles_json if v["status"] in IN_FORCE)

    return {
        "total_contracts":      len(non_idv),
        "active_contracts":     active_count,
        "expiring_soon":        expiring_count,
        "total_ceiling_b":      round(active_ceiling / 1e9, 1),
        "total_obligated_b":    round(active_obligated / 1e9, 1),
        "ceiling_remaining_b":  round(active_remaining / 1e9, 1),
        "unique_contractors":   active_contractors,
        "unique_vehicles":      active_vehicles,
        "unique_offices":       active_offices,
        "as_of":                TODAY_STR,
    }


def build_filter_options(contracts: dict) -> dict:
    values = list(contracts.values())

    def unique_sorted(field, n=50):
        vals = sorted(set(c.get(field) for c in values if c.get(field)))
        return vals[:n]

    statuses = sorted(set(c["status"] for c in values))
    naics_2 = sorted(set(
        c.get("naics_code", "")[:2]
        for c in values
        if c.get("naics_code") and len(c["naics_code"]) >= 2
    ))

    return {
        "statuses":       statuses,
        "departments":    unique_sorted("department"),
        "sub_agencies":   unique_sorted("sub_agency"),
        "vehicle_types":  unique_sorted("vehicle_type"),
        "pricing_types":  unique_sorted("pricing_type_label"),
        "naics_2digit":   naics_2,
        "states":         unique_sorted("place_state"),
    }


def build_config_mirror() -> dict:
    """A small frontend-facing mirror of config.yaml so the methodology page
    can display the live values."""
    return {
        "as_of":                TODAY_STR,
        "fetch":                CONFIG["fetch"],
        "classification": {
            "expiring_soon_days":   EXPIRING_SOON_DAYS,
            "effective_end_fields": EFFECTIVE_END_FIELDS,
            "drop_expired":         DROP_EXPIRED,
            "drop_unknown":         DROP_UNKNOWN,
        },
    }


def main():
    import os
    has_local = BULK_CSV.exists() or list(CHECKPOINT_DIR.glob("FY*.csv"))
    has_r2 = bool(os.environ.get("CF_R2_ACCOUNT_ID"))
    if not has_local and not has_r2:
        print("No data found -- run fetch_awards.py first or set R2 credentials.")
        return

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Stream and aggregate (no pandas, constant memory per row)
    contracts = stream_and_aggregate()
    contracts = enrich_contracts(contracts)

    # Apply configured drop policy (Expired was already early-filtered during
    # streaming; Unknown is dropped here because a later transaction might have
    # populated an end date).
    dropped = {"Expired": 0, "Unknown": 0}
    keep = {}
    for k, c in contracts.items():
        s = c["status"]
        if DROP_EXPIRED and s == "Expired":
            dropped["Expired"] += 1
            continue
        if DROP_UNKNOWN and s == "Unknown":
            dropped["Unknown"] += 1
            continue
        keep[k] = c
    contracts = keep
    print(f"  After drop policy: {len(contracts):,} kept "
          f"(dropped {dropped['Expired']:,} Expired, {dropped['Unknown']:,} Unknown)")

    # Build JSONs. Contracts (task orders / standalones) are not emitted as
    # their own file -- they're folded into each vehicle's top_orders for
    # the drill-down. Summary stats still come from the full contracts dict.
    print("\nBuilding dashboard JSONs...")

    vehicles_json = build_vehicles_json(contracts)
    print(f"  vehicles.json: {len(vehicles_json):,} records")

    summary = build_summary(contracts, vehicles_json)
    print(f"  summary.json: {summary['active_contracts']:,} active, "
          f"${summary['ceiling_remaining_b']}B remaining")

    filters = build_filter_options(contracts)
    config_mirror = build_config_mirror()

    # Write outputs
    outputs = {
        "vehicles.json":  vehicles_json,
        "summary.json":   summary,
        "filters.json":   filters,
        "config.json":    config_mirror,
    }

    for fname, data in outputs.items():
        path = WEB_DATA_DIR / fname
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  Wrote {path}")

    print(f"\nDone. {len(vehicles_json):,} vehicles (summary counts in summary.json).")
    print("Commit web/data/ to deploy.")


if __name__ == "__main__":
    main()
