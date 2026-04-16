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
import io
import json
import sys
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

BULK_CSV       = Path("data/dod_contracts_bulk.csv")
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
SAM_CSV        = Path("data/sam_lookup.csv")
WEB_DATA_DIR   = Path("web/data")

TODAY = date.today()
TODAY_STR = TODAY.isoformat()

# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

PARENT_AWARD_TYPE_LABELS = {
    "IDV_A": "GWAC",
    "IDV_B": "IDC",
    "IDV_B_A": "IDC / Requirements",
    "IDV_B_B": "IDC / IDIQ",
    "IDV_B_C": "IDC / Definite Quantity",
    "IDV_C": "FSS",
    "IDV_D": "BOA",
    "IDV_E": "BPA",
}

PRICING_LABELS = {
    "J": "Firm Fixed Price",
    "Y": "Time & Materials",
    "Z": "Labor Hours",
    "U": "Cost Plus Fixed Fee",
    "V": "Cost Plus Award Fee",
    "S": "Cost No Fee",
    "R": "Cost Plus Incentive Fee",
    "A": "Fixed Price Redetermination",
}

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


def _classify_status(pop_end_str: str | None) -> str:
    if not pop_end_str:
        return "Unknown"
    try:
        pop_end = datetime.strptime(pop_end_str[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return "Unknown"
    if pop_end >= TODAY:
        days_left = (pop_end - TODAY).days
        if days_left <= 180:
            return "Expiring Soon"
        return "Active"
    return "Expired"


def _sam_url(sol_id: str | None) -> str | None:
    if not sol_id:
        return None
    return f"https://sam.gov/search/?keywords={sol_id}&index=opp"


# ---------------------------------------------------------------------------
# Streaming aggregation
# ---------------------------------------------------------------------------

def _data_sources() -> list[Path]:
    """Find CSV files to read: merged CSV or checkpoint files."""
    if BULK_CSV.exists():
        return [BULK_CSV]
    checkpoints = sorted(CHECKPOINT_DIR.glob("FY*.csv"))
    sources = [cp for cp in checkpoints if cp.stat().st_size > 0]
    if sources:
        return sources
    raise FileNotFoundError("No data found -- run fetch_awards.py first.")


def stream_and_aggregate() -> dict:
    """Stream all transaction rows, accumulate one entry per contract_award_unique_key.

    For each contract, we keep the latest transaction's values (by action_date).
    federal_action_obligation is summed across all transactions.
    Memory: ~2-4KB per unique contract, ~500K-1M contracts = ~2-4GB peak.
    """
    sources = _data_sources()
    print(f"Reading from {len(sources)} file(s)...")

    # contracts[key] = {field: value, ...} — latest transaction wins
    contracts = {}
    # Track sum of federal_action_obligation per contract (it's per-transaction)
    fao_sums = defaultdict(float)

    total_rows = 0
    for src in sources:
        print(f"  Streaming {src.name}...", end=" ", flush=True)
        file_rows = 0
        with open(src, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_rows += 1
                file_rows += 1
                if total_rows % 2_000_000 == 0:
                    print(f"{total_rows // 1_000_000}M...", end=" ", flush=True)

                key = _val(row, "contract_award_unique_key")
                if not key:
                    continue

                action_date = _val(row, "action_date") or ""

                # Sum federal_action_obligation across all mods
                fao = _float(row, "federal_action_obligation")
                if fao:
                    fao_sums[key] += fao

                # Keep latest transaction (by action_date string comparison)
                existing = contracts.get(key)
                if existing and existing.get("_action_date", "") > action_date:
                    continue  # existing is newer, skip this row

                # Store only the fields we need
                contracts[key] = {
                    "_action_date":       action_date,
                    "key":                key,
                    "piid":               _val(row, "award_id_piid"),
                    "parent_piid":        _val(row, "parent_award_id_piid"),
                    # Dollars (cumulative — take latest)
                    "obligated":          _float(row, "total_dollars_obligated"),
                    "ceiling":            _float(row, "potential_total_value_of_award"),
                    # Dates
                    "pop_start":          _val(row, "period_of_performance_start_date"),
                    "pop_end":            _val(row, "period_of_performance_current_end_date"),
                    "pop_potential_end":  _val(row, "period_of_performance_potential_end_date"),
                    # Agency
                    "department":         _val(row, "awarding_agency_name"),
                    "sub_agency":         _val(row, "awarding_sub_agency_name"),
                    "awarding_office":    _val(row, "awarding_office_name"),
                    "funding_office":     _val(row, "funding_office_name"),
                    # Contractor
                    "recipient_uei":      _val(row, "recipient_uei"),
                    "recipient_name":     _val(row, "recipient_name"),
                    "recipient_parent":   _val(row, "recipient_parent_name"),
                    # Vehicle
                    "parent_award_type_code": _val(row, "parent_award_type_code"),
                    "parent_award_type":  _val(row, "parent_award_type"),
                    # Scope
                    "award_description":  _val(row, "award_description"),
                    "base_description":   _val(row, "prime_award_base_transaction_description"),
                    "txn_description":    _val(row, "transaction_description"),
                    "naics_code":         _val(row, "naics_code"),
                    "naics_description":  _val(row, "naics_description"),
                    "psc_code":           _val(row, "product_or_service_code"),
                    "psc_description":    _val(row, "product_or_service_code_description"),
                    "solicitation_id":    _val(row, "solicitation_identifier"),
                    # Competition
                    "set_aside":          _val(row, "type_of_set_aside_code") or "NONE",
                    "pricing_type":       _val(row, "type_of_contract_pricing_code"),
                    "pricing_label":      _val(row, "type_of_contract_pricing"),
                    "num_offers":         _val(row, "number_of_offers_received"),
                    # Place
                    "place_state":        _val(row, "primary_place_of_performance_state_code"),
                    # Business size
                    "business_size_label": _val(row, "contracting_officers_determination_of_business_size"),
                    # Link
                    "usaspending_link":   _val(row, "usaspending_permalink"),
                }

        print(f"{file_rows:,} rows")

    print(f"  Total: {total_rows:,} transactions -> {len(contracts):,} unique contracts")

    # Apply fao fallback for contracts missing total_dollars_obligated
    for key, c in contracts.items():
        if not c["obligated"] or c["obligated"] == 0:
            c["obligated"] = fao_sums.get(key)

    return contracts


# ---------------------------------------------------------------------------
# Derived fields + JSON builders
# ---------------------------------------------------------------------------

def enrich_contracts(contracts: dict) -> dict:
    """Add derived fields to each contract dict."""
    cutoff = date(TODAY.year - 1, TODAY.month, TODAY.day)
    cutoff_str = cutoff.isoformat()

    for c in contracts.values():
        # Ceiling remaining
        ceiling = c.get("ceiling")
        obligated = c.get("obligated")
        if ceiling is not None and obligated is not None:
            c["ceiling_remaining"] = ceiling - obligated
        else:
            c["ceiling_remaining"] = None

        # Status
        c["status"] = _classify_status(c.get("pop_end"))

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
    """Build JSON for active + expiring + recently expired contracts."""
    cutoff = date(TODAY.year - 1, TODAY.month, TODAY.day).isoformat()

    records = []
    for c in contracts.values():
        status = c["status"]
        # Include active, expiring, unknown, and recently expired
        if status == "Expired":
            pop_end = (c.get("pop_end") or "")[:10]
            if pop_end < cutoff:
                continue
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
    """Group contracts by parent PIID for vehicle-level rollup."""
    vehicles = defaultdict(lambda: {
        "order_count": 0, "active_orders": 0,
        "total_obligated": 0.0, "total_ceiling": 0.0,
        "contractors": set(), "naics_codes": set(),
        "naics_descriptions": set(), "descriptions": set(),
        "states": set(),
        "department": None, "sub_agency": None, "awarding_office": None,
        "vehicle_type": None, "parent_award_type": None,
        "earliest_start": None, "latest_end": None, "latest_potential_end": None,
    })

    for c in contracts.values():
        parent = c.get("parent_piid")
        if not parent:
            continue

        v = vehicles[parent]
        v["order_count"] += 1
        if c["status"] in ("Active", "Expiring Soon"):
            v["active_orders"] += 1
        v["total_obligated"] += c.get("obligated") or 0
        v["total_ceiling"] += c.get("ceiling") or 0

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
        if ps and (not v["earliest_start"] or ps < v["earliest_start"]):
            v["earliest_start"] = ps
        if pe and (not v["latest_end"] or pe > v["latest_end"]):
            v["latest_end"] = pe
        if ppe and (not v["latest_potential_end"] or ppe > v["latest_potential_end"]):
            v["latest_potential_end"] = ppe

    # Build JSON records
    cutoff = date(TODAY.year - 1, TODAY.month, TODAY.day).isoformat()
    records = []
    for parent_piid, v in vehicles.items():
        status = _classify_status(v["latest_end"])
        if status == "Expired" and (v["latest_end"] or "") < cutoff:
            continue

        remaining = v["total_ceiling"] - v["total_obligated"]
        pct_used = round(v["total_obligated"] / v["total_ceiling"] * 100, 1) if v["total_ceiling"] > 0 else None

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
            "total_ceiling":      round(v["total_ceiling"]) if v["total_ceiling"] else None,
            "total_obligated":    round(v["total_obligated"]) if v["total_obligated"] else None,
            "ceiling_remaining":  round(remaining),
            "pct_ceiling_used":   pct_used,
            "states":             sorted(v["states"])[:10],
        })

    records.sort(key=lambda r: -(r["ceiling_remaining"] or 0))
    return records


def build_summary(contracts: dict, vehicles_json: list) -> dict:
    active_count = sum(1 for c in contracts.values() if c["status"] == "Active")
    expiring_count = sum(1 for c in contracts.values() if c["status"] == "Expiring Soon")
    active_ceiling = sum(c.get("ceiling") or 0 for c in contracts.values() if c["status"] == "Active")
    active_obligated = sum(c.get("obligated") or 0 for c in contracts.values() if c["status"] == "Active")
    active_remaining = active_ceiling - active_obligated
    active_contractors = len(set(
        c.get("recipient_name") for c in contracts.values()
        if c["status"] == "Active" and c.get("recipient_name")
    ))
    active_offices = len(set(
        c.get("awarding_office") for c in contracts.values()
        if c["status"] == "Active" and c.get("awarding_office")
    ))
    active_vehicles = sum(1 for v in vehicles_json if v["status"] in ("Active", "Expiring Soon"))

    return {
        "total_contracts":      len(contracts),
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
    active = [c for c in contracts.values() if c["status"] in ("Active", "Expiring Soon", "Unknown")]

    def unique_sorted(field, n=50):
        vals = sorted(set(c.get(field) for c in active if c.get(field)))
        return vals[:n]

    naics_2 = sorted(set(
        c.get("naics_code", "")[:2]
        for c in active
        if c.get("naics_code") and len(c["naics_code"]) >= 2
    ))

    return {
        "statuses":       ["Active", "Expiring Soon", "Expired", "Unknown"],
        "departments":    unique_sorted("department"),
        "sub_agencies":   unique_sorted("sub_agency"),
        "vehicle_types":  unique_sorted("vehicle_type"),
        "pricing_types":  unique_sorted("pricing_type_label"),
        "naics_2digit":   naics_2,
        "states":         unique_sorted("place_state"),
    }


def main():
    if not BULK_CSV.exists() and not list(CHECKPOINT_DIR.glob("FY*.csv")):
        print("No data found -- run fetch_awards.py first.")
        return

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Stream and aggregate (no pandas, constant memory per row)
    contracts = stream_and_aggregate()
    contracts = enrich_contracts(contracts)

    # Build JSONs
    print("\nBuilding dashboard JSONs...")

    contracts_json = build_contracts_json(contracts)
    print(f"  contracts.json: {len(contracts_json):,} records")

    vehicles_json = build_vehicles_json(contracts)
    print(f"  vehicles.json: {len(vehicles_json):,} records")

    summary = build_summary(contracts, vehicles_json)
    print(f"  summary.json: {summary['active_contracts']:,} active, "
          f"${summary['ceiling_remaining_b']}B remaining")

    filters = build_filter_options(contracts)

    # Write outputs
    outputs = {
        "contracts.json": contracts_json,
        "vehicles.json":  vehicles_json,
        "summary.json":   summary,
        "filters.json":   filters,
    }

    for fname, data in outputs.items():
        path = WEB_DATA_DIR / fname
        path.write_text(json.dumps(data, indent=2, default=str))
        print(f"  Wrote {path}")

    print(f"\nDone. {len(contracts_json):,} contracts + {len(vehicles_json):,} vehicles.")
    print("Commit web/data/ to deploy.")


if __name__ == "__main__":
    main()
