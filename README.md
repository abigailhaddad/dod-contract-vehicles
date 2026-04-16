# Federal Contract Vehicles Finder

A searchable dashboard of active federal contract vehicles — IDIQs, BPAs, and
other awards with scope, ceiling, and remaining capacity info. Find existing
vehicles you can place orders on instead of starting a new procurement.

**[View the dashboard](#)** (coming soon)

## What this is

Federal agencies have thousands of active contract vehicles, but no single place
to search across them by scope, contractor, ceiling remaining, or expiration date.
This project pulls all contract data from USASpending bulk archives, aggregates
transactions to the contract and vehicle level, and serves a searchable dashboard.

Two views:
- **All Contracts** — searchable table of individual awards with ceiling, obligations,
  remaining capacity, contractor, scope, and dates
- **Vehicle Rollup** — contracts grouped by parent IDIQ/BPA PIID, showing total
  ceiling, obligations across all orders, contractor list, and scope

## How it works

```
fetch_awards.py         USASpending bulk archives → data/dod_contracts_bulk.csv
                        (all ~200 columns preserved — no field filtering)

enrich_sam.py           SAM.gov monthly extract → data/sam_lookup.csv
                        (contractor details: entity age, location, business name)

build_dashboard.py      bulk + SAM → web/data/*.json
                        (aggregates to contract level, computes ceiling remaining,
                         builds vehicle-level rollup, outputs dashboard JSONs)
```

## Quick start

```bash
pip install -r requirements.txt

# Step 1: Download contract data from USASpending (no API key needed)
python3 fetch_awards.py                  # default: 5 fiscal years
python3 fetch_awards.py --fy 2025 2026   # or specific years

# Step 2: SAM vendor enrichment (optional — adds contractor details)
echo "SAM_API_KEY=your_key" >> .env      # free key from sam.gov
python3 enrich_sam.py

# Step 3: Build dashboard data
python3 build_dashboard.py

# View locally
cd web && python3 -m http.server 8000
```

All scripts are checkpoint/resume safe. The USASpending download can be
interrupted and re-run — it picks up where it left off.

## Key fields

| Dashboard field | Source | Notes |
|---|---|---|
| **Ceiling** | `potential_total_value_of_award` | Includes all options |
| **Obligated** | `total_dollars_obligated` | Cumulative from latest transaction |
| **Ceiling Remaining** | Ceiling - Obligated | Approximate |
| **PoP End** | `period_of_performance_current_end_date` | Current end date |
| **Potential End** | `period_of_performance_potential_end_date` | With all options |
| **Description** | Best of 3 USASpending description fields | Often terse — click SAM link for full SOW |
| **NAICS / PSC** | `naics_description`, `product_or_service_code_description` | Categorizes scope |
| **Vehicle Type** | Derived from `parent_award_type_code` | GWAC, IDC/IDIQ, FSS, BOA, BPA |

## Data sources

| Source | What it provides | Access |
|---|---|---|
| [USASpending](https://www.usaspending.gov) bulk archives | Contract transactions (~200 fields) | Free, no key needed |
| [SAM.gov](https://sam.gov) monthly extract | Contractor entity data | Free API key |

## Caveats

- **Ceiling remaining is approximate.** USASpending data may lag real ceiling
  modifications by weeks.
- **Vehicle rollup sums order-level data**, not the vehicle's own ceiling.
  Can overcount if orders share ceiling space.
- **Scope descriptions are often terse** ("IT SERVICES"). For real scope, click
  the SAM.gov solicitation link or check the contract file.
- **Geographic coverage** = primary place of performance, not the vehicle's
  authorized scope.

## GitHub Actions

`fetch_awards.py` runs automatically via GitHub Actions. Uses Cloudflare R2
for checkpoint persistence and auto-chains new runs when IP-blocked.

## License

MIT
