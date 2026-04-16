# dod-contract-vehicles

Dashboard of active DoD contract vehicles -- IDIQs, BPAs, and other awards
with scope, ceiling, and capacity info. Built so OUSW (and others) can
quickly find existing contract vehicles to place orders on.

## Data pipeline (run in order)

### Step 1 -- `fetch_awards.py`
Downloads transaction-level contract records from USASpending bulk archives.
- Downloads ZIPs for DoD (agency 097) by fiscal year
- **Keeps ALL columns** from the source CSV (~200 columns) -- no column filter
- No NAICS filter by default -- captures all contract types
- Checkpoints per agency/FY at `data/bulk_checkpoints/`
- Resume-safe: re-running skips completed downloads
- Output: `data/dod_contracts_bulk.csv`

```bash
python3 fetch_awards.py                       # all DoD, FY2022-FY2026
python3 fetch_awards.py --fy 2025 2026        # specific years
python3 fetch_awards.py --naics-prefix 5415   # IT services only
python3 fetch_awards.py --force               # re-download
```

### Step 2 -- `enrich_sam.py`
Downloads SAM.gov monthly bulk extract, filters to UEIs in the DoD contracts.
- Requires `SAM_API_KEY` in `.env` (free key: sam.gov -> Account Details -> API Keys)
- Downloads ~1-3GB ZIP once, cached at `data/sam_extract_cache.zip`
- Output: `data/sam_lookup.csv`
- Fields: uei, legal_business_name, sam_registration_date, entity_start_date, city, state

### Step 3 -- `build_dashboard.py`
Aggregates transactions to contract level, computes ceiling remaining and
active status, builds vehicle-level rollup, outputs dashboard JSONs.
- Joins SAM data when available (contractor location, entity age)
- Active = PoP end date in the future
- Expiring Soon = PoP end within 6 months
- Ceiling remaining = potential_total_value_of_award - total_dollars_obligated
- Vehicle rollup groups orders by parent_award_id_piid
- Output: `web/data/` (contracts.json, vehicles.json, summary.json, filters.json)

```bash
python3 build_dashboard.py
```

## Dashboard (`web/index.html`)

Static site (Vercel-deployable). Two views:
1. **All Contracts** -- searchable/filterable DataTable of individual awards
2. **Vehicle Rollup** -- grouped by parent IDIQ/BPA PIID

Filters: status, sub-agency, vehicle type, pricing, NAICS.
Uses DataTables + Bootstrap 5 (CDN).

## Key fields

- `ceiling` = `potential_total_value_of_award` (includes all options)
- `obligated` = `total_dollars_obligated` (cumulative)
- `ceiling_remaining` = ceiling - obligated (approximate)
- `pop_end` = `period_of_performance_current_end_date`
- `pop_potential_end` = `period_of_performance_potential_end_date`
- `parent_piid` = `parent_award_id_piid` (links to parent IDIQ/BPA)
- `vehicle_type` = derived from `parent_award_type_code` (GWAC/IDC/FSS/BOA/BPA)

## Caveats

- Ceiling remaining is approximate. USASpending `potential_total_value_of_award`
  reflects the latest modification but may lag real ceiling changes.
- Vehicle rollup sums order-level ceilings, NOT the vehicle's own ceiling.
  This can overcount (if orders share ceiling) or undercount (if ceiling
  hasn't been fully ordered against).
- Geographic coverage = place of performance state, not the vehicle's
  authorized geographic scope.
- `award_description` is often terse ("IT SERVICES"). For real scope
  descriptions, check the solicitation on SAM.gov or the contract file.

## Files

```
fetch_awards.py        -- USASpending bulk download (DoD, all columns)
enrich_sam.py          -- SAM.gov entity enrichment (contractor details)
build_dashboard.py     -- Aggregate + build dashboard JSONs
web/index.html         -- Dashboard (DataTables)
web/data/*.json        -- Dashboard data (committed for Vercel)
data/                  -- Raw data (gitignored)
.env                   -- SAM_API_KEY (gitignored)
vercel.json            -- Routes / -> web/
```
