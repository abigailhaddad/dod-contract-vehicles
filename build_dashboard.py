"""
build_dashboard.py -- Aggregate DoD contract data and build dashboard JSONs.

Reads data/dod_contracts_bulk.csv (from fetch_awards.py), aggregates
transaction-level data to one row per contract, computes ceiling remaining
and active status, and outputs JSON files to web/data/ for the dashboard.

Two views:
  1. Contracts -- one row per contract_award_unique_key
  2. Vehicles  -- grouped by parent_award_id_piid (IDIQ/BPA rollup)

Run:
    python3 build_dashboard.py
"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

BULK_CSV       = Path("data/dod_contracts_bulk.csv")
CHECKPOINT_DIR = Path("data/bulk_checkpoints")
SAM_CSV        = Path("data/sam_lookup.csv")
WEB_DATA_DIR   = Path("web/data")

TODAY = pd.Timestamp.now().normalize()

# ---------------------------------------------------------------------------
# Award type labels
# ---------------------------------------------------------------------------

AWARD_TYPE_LABELS = {
    "A": "BPA Call",
    "B": "Purchase Order",
    "C": "Delivery Order",
    "D": "Definitive Contract",
}

IDC_TYPE_LABELS = {
    "A": "Indefinite Delivery / Requirements",
    "B": "Indefinite Delivery / Indefinite Quantity",
    "C": "Indefinite Delivery / Definite Quantity",
}

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


def _best_description(row) -> str | None:
    """Pick the most informative description from available fields.

    USASpending has three description fields with varying detail:
      - prime_award_base_transaction_description: original base award text (often best)
      - award_description: latest transaction description
      - transaction_description: per-modification description
    We pick the longest non-null one, since longer = more informative."""
    candidates = []
    for col in ["base_description", "award_description", "transaction_description"]:
        val = row.get(col)
        if pd.notna(val) and str(val).strip():
            candidates.append(str(val).strip())
    if not candidates:
        return None
    return max(candidates, key=len)


def _sam_solicitation_url(sol_id) -> str | None:
    """Build a SAM.gov search URL for a solicitation identifier."""
    if pd.isna(sol_id) or not str(sol_id).strip():
        return None
    return f"https://sam.gov/search/?keywords={str(sol_id).strip()}&index=opp"


# Only the columns build_dashboard actually needs -- keeps memory manageable
# when the bulk CSV has ~200 columns and 18M+ rows.
USE_COLUMNS = [
    "contract_award_unique_key", "award_id_piid", "parent_award_id_piid",
    # Dollars
    "federal_action_obligation", "total_dollars_obligated",
    "potential_total_value_of_award", "base_and_exercised_options_value",
    "base_and_all_options_value", "current_total_value_of_award",
    # Dates
    "action_date", "period_of_performance_start_date",
    "period_of_performance_current_end_date", "period_of_performance_potential_end_date",
    # Agency
    "awarding_agency_name", "awarding_sub_agency_name",
    "awarding_office_name", "awarding_office_code",
    "funding_agency_name", "funding_sub_agency_name", "funding_office_name",
    # Contractor
    "recipient_uei", "recipient_name", "recipient_doing_business_as_name",
    "recipient_parent_name", "cage_code",
    # Vehicle info
    "award_type_code", "award_type",
    "parent_award_type_code", "parent_award_type",
    "idv_type_code", "type_of_idc_code", "multiple_or_single_award_idv_code",
    # Scope
    "award_description", "prime_award_base_transaction_description",
    "transaction_description", "solicitation_identifier",
    "naics_code", "naics_description",
    "product_or_service_code", "product_or_service_code_description",
    # Competition
    "extent_competed_code", "type_of_set_aside_code",
    "type_of_contract_pricing_code", "type_of_contract_pricing",
    "number_of_offers_received",
    # Place
    "primary_place_of_performance_state_code",
    "primary_place_of_performance_country_code",
    "primary_place_of_performance_county_name",
    # Business size
    "contracting_officers_determination_of_business_size_code",
    "contracting_officers_determination_of_business_size",
    # Link
    "usaspending_permalink",
]


def _find_data_source() -> str | list[Path]:
    """Find the data to load: merged CSV or checkpoint files."""
    if BULK_CSV.exists():
        return str(BULK_CSV)
    # Fall back to reading checkpoint files directly (on GitHub Actions,
    # the merged CSV may not exist if we skipped the merge step)
    checkpoints = sorted(CHECKPOINT_DIR.glob("FY*.csv"))
    if checkpoints:
        return [cp for cp in checkpoints if cp.stat().st_size > 0]
    raise FileNotFoundError("No data found -- run fetch_awards.py first.")


def load_and_aggregate() -> pd.DataFrame:
    """Load bulk data and aggregate to one row per contract."""
    source = _find_data_source()

    # Only read the columns we need -- cuts memory ~75% vs reading all 200
    print("Loading bulk data...")
    if isinstance(source, str):
        df = pd.read_csv(source, low_memory=False, dtype={"naics_code": str},
                         usecols=lambda c: c in USE_COLUMNS)
    else:
        print(f"  Reading from {len(source)} checkpoint files...")
        frames = []
        for cp in source:
            chunk = pd.read_csv(cp, low_memory=False, dtype={"naics_code": str},
                                usecols=lambda c: c in USE_COLUMNS)
            frames.append(chunk)
            print(f"    {cp.name}: {len(chunk):,} rows")
        df = pd.concat(frames, ignore_index=True)
        del frames

    print(f"  {len(df):,} transaction rows, {len(df.columns)} columns")

    # Ensure optional columns exist
    for col in USE_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Parse dates and sort
    df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.sort_values("action_date")

    # Aggregate to contract level
    print("Aggregating to contract level...")
    agg = df.groupby("contract_award_unique_key").agg(
        piid=("award_id_piid", "last"),
        parent_piid=("parent_award_id_piid", "last"),
        award_date=("action_date", "max"),

        # Dollars -- cumulative fields take "last" (latest transaction)
        obligated=("total_dollars_obligated", "last"),
        federal_action_obligation=("federal_action_obligation", "sum"),
        ceiling=("potential_total_value_of_award", "last"),
        base_exercised=("base_and_exercised_options_value", "last"),
        base_all_options=("base_and_all_options_value", "last"),
        current_value=("current_total_value_of_award", "last"),

        # Dates
        pop_start=("period_of_performance_start_date", "first"),
        pop_end=("period_of_performance_current_end_date", "last"),
        pop_potential_end=("period_of_performance_potential_end_date", "last"),

        # Agency
        department=("awarding_agency_name", "last"),
        sub_agency=("awarding_sub_agency_name", "last"),
        awarding_office=("awarding_office_name", "last"),
        awarding_office_code=("awarding_office_code", "last"),
        funding_agency=("funding_agency_name", "last"),
        funding_sub_agency=("funding_sub_agency_name", "last"),
        funding_office=("funding_office_name", "last"),

        # Contractor
        recipient_uei=("recipient_uei", "last"),
        recipient_name=("recipient_name", "last"),
        recipient_dba=("recipient_doing_business_as_name", "last"),
        recipient_parent_name=("recipient_parent_name", "last"),
        cage_code=("cage_code", "last"),

        # Vehicle info
        award_type_code=("award_type_code", "last"),
        award_type=("award_type", "last"),
        parent_award_type_code=("parent_award_type_code", "last"),
        parent_award_type=("parent_award_type", "last"),
        idv_type_code=("idv_type_code", "last"),
        idc_type_code=("type_of_idc_code", "last"),
        multi_single=("multiple_or_single_award_idv_code", "last"),

        # Scope -- multiple description fields, often different levels of detail
        award_description=("award_description", "last"),
        base_description=("prime_award_base_transaction_description", "last"),
        transaction_description=("transaction_description", "last"),
        naics_code=("naics_code", "last"),
        naics_description=("naics_description", "last"),
        psc_code=("product_or_service_code", "last"),
        psc_description=("product_or_service_code_description", "last"),
        solicitation_id=("solicitation_identifier", "last"),

        # Competition
        extent_competed=("extent_competed_code", "last"),
        set_aside=("type_of_set_aside_code", "last"),
        pricing_type=("type_of_contract_pricing_code", "last"),
        pricing_label=("type_of_contract_pricing", "last"),
        num_offers=("number_of_offers_received", "last"),

        # Place
        place_state=("primary_place_of_performance_state_code", "last"),
        place_country=("primary_place_of_performance_country_code", "last"),
        place_county=("primary_place_of_performance_county_name", "last"),

        # Business size
        business_size=("contracting_officers_determination_of_business_size_code", "last"),
        business_size_label=("contracting_officers_determination_of_business_size", "last"),

        # Link
        usaspending_link=("usaspending_permalink", "last"),

        num_transactions=("contract_award_unique_key", "count"),
    ).reset_index()

    agg = agg.rename(columns={"contract_award_unique_key": "key"})

    # Numeric cleanup
    for col in ["obligated", "ceiling", "base_exercised", "base_all_options",
                "current_value", "federal_action_obligation"]:
        agg[col] = pd.to_numeric(agg[col], errors="coerce")

    # Use total_dollars_obligated, fall back to summed actions
    mask = agg["obligated"].isna() | (agg["obligated"] == 0)
    agg.loc[mask, "obligated"] = agg.loc[mask, "federal_action_obligation"]

    # Parse dates
    for col in ["pop_start", "pop_end", "pop_potential_end"]:
        agg[col] = pd.to_datetime(agg[col], errors="coerce")

    # --- Derived fields ---

    # Best description: pick the longest of the three description fields
    agg["best_description"] = agg.apply(_best_description, axis=1)

    # SAM.gov solicitation link (for full scope/SOW documents)
    agg["sam_solicitation_url"] = agg["solicitation_id"].apply(_sam_solicitation_url)

    # Ceiling remaining
    agg["ceiling_remaining"] = agg["ceiling"] - agg["obligated"]

    # Active status
    agg["is_active"] = agg["pop_end"] >= TODAY
    agg["is_potentially_active"] = agg["pop_potential_end"] >= TODAY
    agg["days_until_expiry"] = (agg["pop_end"] - TODAY).dt.days

    def classify_status(row):
        if pd.isna(row["pop_end"]):
            return "Unknown"
        if row["pop_end"] >= TODAY:
            if row["days_until_expiry"] <= 180:
                return "Expiring Soon"
            return "Active"
        return "Expired"

    agg["status"] = agg.apply(classify_status, axis=1)

    # Has parent vehicle?
    agg["has_parent_vehicle"] = agg["parent_piid"].notna() & (agg["parent_piid"] != "")

    # Vehicle type label
    agg["vehicle_type"] = agg["parent_award_type_code"].map(PARENT_AWARD_TYPE_LABELS).fillna("Standalone")
    agg["pricing_type_label"] = agg["pricing_type"].map(PRICING_LABELS).fillna(agg["pricing_label"])

    # Set aside cleanup
    agg["set_aside"] = agg["set_aside"].fillna("NONE").replace("", "NONE")

    # --- SAM enrichment (optional) ---
    if SAM_CSV.exists():
        print("Joining SAM entity data...")
        sam = pd.read_csv(SAM_CSV)
        sam["entity_start_date"] = pd.to_datetime(sam["entity_start_date"], errors="coerce")
        sam["sam_registration_date"] = pd.to_datetime(sam["sam_registration_date"], errors="coerce")

        merge_cols = ["uei", "legal_business_name", "sam_registration_date",
                      "entity_start_date", "city", "state"]
        available = [c for c in merge_cols if c in sam.columns]
        agg = agg.merge(sam[available], left_on="recipient_uei", right_on="uei", how="left")

        matched = agg["uei"].notna().sum()
        print(f"  SAM match: {matched:,} / {len(agg):,} contracts ({matched/len(agg)*100:.1f}%)")

        # Contractor location from SAM (distinct from place of performance)
        agg["contractor_city"] = agg.get("city")
        agg["contractor_state"] = agg.get("state")
    else:
        print("  No SAM data -- run enrich_sam.py for contractor details")
        agg["contractor_city"] = None
        agg["contractor_state"] = None

    print(f"  {len(agg):,} unique contracts")
    print(f"  Active: {(agg['status'] == 'Active').sum():,}")
    print(f"  Expiring Soon: {(agg['status'] == 'Expiring Soon').sum():,}")
    print(f"  Expired: {(agg['status'] == 'Expired').sum():,}")

    return agg


def build_vehicle_rollup(df: pd.DataFrame) -> pd.DataFrame:
    """Group contracts by parent vehicle PIID for a vehicle-level view."""
    # Only contracts that have a parent vehicle
    has_parent = df[df["has_parent_vehicle"]].copy()
    if has_parent.empty:
        return pd.DataFrame()

    print("Building vehicle rollup...")
    vehicles = has_parent.groupby("parent_piid").agg(
        order_count=("key", "count"),
        total_obligated=("obligated", "sum"),
        total_ceiling=("ceiling", "sum"),

        # Take the most common values
        department=("department", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
        sub_agency=("sub_agency", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
        awarding_office=("awarding_office", lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None),
        vehicle_type=("vehicle_type", "first"),
        parent_award_type=("parent_award_type", "first"),

        # Contractors on this vehicle
        contractors=("recipient_name", lambda x: list(x.dropna().unique())),
        contractor_count=("recipient_name", "nunique"),

        # Scope -- collect unique NAICS and descriptions
        naics_codes=("naics_code", lambda x: list(x.dropna().unique())),
        naics_descriptions=("naics_description", lambda x: list(x.dropna().unique())),
        descriptions=("award_description", lambda x: list(x.dropna().unique())[:5]),

        # Date range
        earliest_start=("pop_start", "min"),
        latest_end=("pop_end", "max"),
        latest_potential_end=("pop_potential_end", "max"),

        # Place -- collect unique states
        states=("place_state", lambda x: list(x.dropna().unique())),

        # Active orders
        active_orders=("is_active", "sum"),
    ).reset_index()

    vehicles["ceiling_remaining"] = vehicles["total_ceiling"] - vehicles["total_obligated"]
    vehicles["is_active"] = vehicles["latest_end"] >= TODAY
    vehicles["pct_ceiling_used"] = (
        (vehicles["total_obligated"] / vehicles["total_ceiling"] * 100)
        .where(vehicles["total_ceiling"] > 0)
        .round(1)
    )

    def classify_vehicle_status(row):
        if pd.isna(row["latest_end"]):
            return "Unknown"
        if row["latest_end"] >= TODAY:
            days_left = (row["latest_end"] - TODAY).days
            if days_left <= 180:
                return "Expiring Soon"
            return "Active"
        return "Expired"

    vehicles["status"] = vehicles.apply(classify_vehicle_status, axis=1)

    print(f"  {len(vehicles):,} unique parent vehicles")
    print(f"  Active: {(vehicles['status'] == 'Active').sum():,}")

    return vehicles


def build_contracts_json(df: pd.DataFrame) -> list:
    """Build the contracts table JSON for the dashboard.
    Includes active + expiring + recently expired (last 12 months)."""
    cutoff = TODAY - pd.Timedelta(days=365)
    recent = df[
        (df["status"].isin(["Active", "Expiring Soon"])) |
        ((df["status"] == "Expired") & (df["pop_end"] >= cutoff)) |
        (df["status"] == "Unknown")
    ].copy()

    # Sort: active first, then by ceiling remaining descending
    status_order = {"Active": 0, "Expiring Soon": 1, "Unknown": 2, "Expired": 3}
    recent["_sort"] = recent["status"].map(status_order)
    recent = recent.sort_values(["_sort", "ceiling_remaining"], ascending=[True, False])

    records = []
    for _, r in recent.iterrows():
        records.append({
            "key":                  r["key"],
            "piid":                 r["piid"],
            "parent_piid":          r["parent_piid"] if pd.notna(r["parent_piid"]) else None,
            "contractor":           r["recipient_name"],
            "contractor_parent":    r["recipient_parent_name"] if pd.notna(r["recipient_parent_name"]) else None,
            "department":           r["department"],
            "sub_agency":           r["sub_agency"],
            "awarding_office":      r["awarding_office"],
            "funding_office":       r["funding_office"] if pd.notna(r.get("funding_office")) else None,
            "description":          r["best_description"] if pd.notna(r.get("best_description")) else (r["award_description"] if pd.notna(r["award_description"]) else None),
            "solicitation_id":      r["solicitation_id"] if pd.notna(r.get("solicitation_id")) else None,
            "sam_url":              r["sam_solicitation_url"] if pd.notna(r.get("sam_solicitation_url")) else None,
            "naics":                r["naics_code"] if pd.notna(r["naics_code"]) else None,
            "naics_desc":           r["naics_description"] if pd.notna(r["naics_description"]) else None,
            "psc":                  r["psc_code"] if pd.notna(r["psc_code"]) else None,
            "psc_desc":             r["psc_description"] if pd.notna(r["psc_description"]) else None,
            "pricing":              r["pricing_type_label"] if pd.notna(r["pricing_type_label"]) else None,
            "vehicle_type":         r["vehicle_type"],
            "set_aside":            r["set_aside"],
            "business_size":        r["business_size_label"] if pd.notna(r["business_size_label"]) else None,
            "status":               r["status"],
            "pop_start":            r["pop_start"].strftime("%Y-%m-%d") if pd.notna(r["pop_start"]) else None,
            "pop_end":              r["pop_end"].strftime("%Y-%m-%d") if pd.notna(r["pop_end"]) else None,
            "pop_potential_end":    r["pop_potential_end"].strftime("%Y-%m-%d") if pd.notna(r["pop_potential_end"]) else None,
            "ceiling":              round(r["ceiling"]) if pd.notna(r["ceiling"]) else None,
            "obligated":            round(r["obligated"]) if pd.notna(r["obligated"]) else None,
            "ceiling_remaining":    round(r["ceiling_remaining"]) if pd.notna(r["ceiling_remaining"]) else None,
            "num_offers":           int(r["num_offers"]) if pd.notna(r.get("num_offers")) else None,
            "place_state":          r["place_state"] if pd.notna(r["place_state"]) else None,
            "contractor_city":      r["contractor_city"] if pd.notna(r.get("contractor_city")) else None,
            "contractor_state":     r["contractor_state"] if pd.notna(r.get("contractor_state")) else None,
            "link":                 r["usaspending_link"] if pd.notna(r["usaspending_link"]) else None,
        })

    return records


def build_vehicles_json(vehicles: pd.DataFrame) -> list:
    """Build the vehicle rollup JSON."""
    if vehicles.empty:
        return []

    # Active + expiring + recently expired
    cutoff = TODAY - pd.Timedelta(days=365)
    recent = vehicles[
        (vehicles["status"].isin(["Active", "Expiring Soon"])) |
        ((vehicles["status"] == "Expired") & (vehicles["latest_end"] >= cutoff)) |
        (vehicles["status"] == "Unknown")
    ].copy()

    recent = recent.sort_values("total_ceiling", ascending=False)

    records = []
    for _, r in recent.iterrows():
        records.append({
            "parent_piid":          r["parent_piid"],
            "vehicle_type":         r["vehicle_type"],
            "parent_award_type":    r["parent_award_type"] if pd.notna(r["parent_award_type"]) else None,
            "department":           r["department"],
            "sub_agency":           r["sub_agency"],
            "awarding_office":      r["awarding_office"],
            "order_count":          int(r["order_count"]),
            "active_orders":        int(r["active_orders"]),
            "contractor_count":     int(r["contractor_count"]),
            "contractors":          r["contractors"][:10],  # Cap at 10 for JSON size
            "naics_codes":          r["naics_codes"][:10],
            "naics_descriptions":   r["naics_descriptions"][:5],
            "descriptions":         r["descriptions"],
            "status":               r["status"],
            "earliest_start":       r["earliest_start"].strftime("%Y-%m-%d") if pd.notna(r["earliest_start"]) else None,
            "latest_end":           r["latest_end"].strftime("%Y-%m-%d") if pd.notna(r["latest_end"]) else None,
            "latest_potential_end": r["latest_potential_end"].strftime("%Y-%m-%d") if pd.notna(r["latest_potential_end"]) else None,
            "total_ceiling":        round(r["total_ceiling"]) if pd.notna(r["total_ceiling"]) else None,
            "total_obligated":      round(r["total_obligated"]) if pd.notna(r["total_obligated"]) else None,
            "ceiling_remaining":    round(r["ceiling_remaining"]) if pd.notna(r["ceiling_remaining"]) else None,
            "pct_ceiling_used":     r["pct_ceiling_used"] if pd.notna(r["pct_ceiling_used"]) else None,
            "states":               r["states"][:10],
        })

    return records


def build_summary(df: pd.DataFrame, vehicles: pd.DataFrame) -> dict:
    """Summary stats for the dashboard header."""
    active = df[df["status"] == "Active"]
    expiring = df[df["status"] == "Expiring Soon"]

    return {
        "total_contracts":      int(len(df)),
        "active_contracts":     int(len(active)),
        "expiring_soon":        int(len(expiring)),
        "total_ceiling_b":      round(active["ceiling"].sum() / 1e9, 1),
        "total_obligated_b":    round(active["obligated"].sum() / 1e9, 1),
        "ceiling_remaining_b":  round(active["ceiling_remaining"].sum() / 1e9, 1),
        "unique_contractors":   int(active["recipient_name"].nunique()),
        "unique_vehicles":      int(len(vehicles[vehicles["status"].isin(["Active", "Expiring Soon"])])) if not vehicles.empty else 0,
        "unique_offices":       int(active["awarding_office"].nunique()),
        "as_of":                TODAY.strftime("%Y-%m-%d"),
    }


def build_filter_options(df: pd.DataFrame) -> dict:
    """Build filter option lists for the dashboard dropdowns."""
    active = df[df["status"].isin(["Active", "Expiring Soon", "Unknown"])]

    def top_values(series, n=50):
        return sorted(series.dropna().unique().tolist())[:n]

    return {
        "statuses":         ["Active", "Expiring Soon", "Expired", "Unknown"],
        "departments":      top_values(active["department"]),
        "sub_agencies":     top_values(active["sub_agency"]),
        "vehicle_types":    top_values(active["vehicle_type"]),
        "pricing_types":    top_values(active["pricing_type_label"]),
        "naics_2digit":     sorted(active["naics_code"].dropna().str[:2].unique().tolist()),
        "states":           top_values(active["place_state"]),
    }


def main():
    if not BULK_CSV.exists() and not list(CHECKPOINT_DIR.glob("FY*.csv")):
        print(f"No data found -- run fetch_awards.py first.")
        return

    WEB_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Load and aggregate
    contracts = load_and_aggregate()
    vehicles = build_vehicle_rollup(contracts)

    # Build JSONs
    print("\nBuilding dashboard JSONs...")

    contracts_json = build_contracts_json(contracts)
    print(f"  contracts.json: {len(contracts_json):,} records")

    vehicles_json = build_vehicles_json(vehicles)
    print(f"  vehicles.json: {len(vehicles_json):,} records")

    summary = build_summary(contracts, vehicles)
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
