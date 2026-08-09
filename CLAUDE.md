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
- Output: `web/data/` (vehicles.json, families.json, grouping_audit.json,
  summary.json, filters.json, config.json)

```bash
python3 build_dashboard.py
```

### Payload encoding -- `payload.py`

`vehicles.json` and `families.json` are downloaded in full by every visitor,
from a public R2 endpoint that serves them with **no `content-encoding`**
(verified: requesting `families.json` with `Accept-Encoding: gzip, br` returns
42,639,945 bytes and no encoding header). File size is wire size.

They ship columnar + dictionary-encoded, which took the pair from 127 MB to
14 MB with no change in what the dashboard renders:

| file | before | after |
|---|---|---|
| vehicles.json | 84,433,308 | 8,747,926 (9.7x) |
| families.json | 42,639,945 | 5,280,843 (8.1x) |

Four things paid for the old size: `indent=2`, key names repeated on every
row, values repeated across rows, and 12 vehicle + 10 family fields that no
frontend file referenced. `family_id` and each order's USASpending link are
derived rather than stored; both derivations are checked against the real
value for every row at encode time and raise if they ever stop holding.

**`decode_vehicles()` / `decode_families()` in `payload.py` and
`decodeVehicles()` / `decodeFamilies()` in `web/index.html` are mirrors of
each other -- change them together.** `tests/test_payload.py` compares the two
key sets and fails if they drift; without it, a field added on one side only
renders as a blank column rather than an error. Everything downstream of the
decoders sees plain row objects, so tables, filters, charts, drill-downs and
CSV export needed no changes.

The other four payloads are a few KB each and stay plain JSON, so
`methodology.html` (and anyone reading them by hand) needs no decoder.

`PAYLOAD_BUDGET_BYTES` in `build_dashboard.py` caps each file, plus a combined
cap on `web/data/*.json`. `tests/test_payload.py` enforces it in CI.

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
web/                   -- PUBLISH DIRECTORY (the whole website, ~126 KB)
web/index.html         -- Dashboard (DataTables)
web/404.html           -- Pages has no default 404; without this every
                          unmatched path serves index.html with HTTP 200
web/_redirects         -- Cloudflare Pages redirects (path-only matching)
web/data/*.json        -- Local build output. GITIGNORED and NOT deployed;
                          the browser reads these from R2 in production.
data/                  -- Raw data (gitignored)
.env                   -- R2 credentials (gitignored)
vercel.json            -- outputDirectory: web (interim, pre-migration)
.vercelignore          -- Vercel ignores .gitignore; keep this a superset
```

## Deployment

Static site, hosted on Cloudflare Pages.

- **Publish directory: `web/`.** Nothing outside `web/` is served.
- **Build command: none.** It is plain static files.
- **Production branch: `prod`** (the pipeline fast-forwards it to `main`
  only after the data tests pass).

Two hard constraints:

1. **Cloudflare Pages rejects any single file over 25 MiB (26,214,400 bytes).**
   `web/data/` is gitignored and never deployed; the browser fetches the
   payloads from the public R2 bucket instead (`DATA_BASE` in
   `web/index.html`). Keep it that way. The encoded payloads (8.7 MB / 5.3 MB)
   would now fit under the limit, but serving them from Pages would still be
   wrong: the pipeline promotes data to R2 on its own schedule, and `web/`
   holds only tracked files.

   Because of that split, **`web/index.html` and the R2 payloads must ship
   together.** A decoder change deployed ahead of the data it decodes -- or
   vice versa -- fails the version check in `decodeVehicles()` and the page
   loads empty. `fetch.yml` gets this right today: `promote` copies R2
   staging to prod *before* it deploys Pages.
2. **`_redirects` matches on PATH ONLY.** Cloudflare Pages silently ignores
   any rule whose source is an absolute URL (`https://host/*`). Don't write one.

`tests/test_publish.py` enforces both, plus the publish scope. It runs on
every push via `.github/workflows/publish-check.yml`.

Historical note: the original `vercel.json` had only a `rewrites` key and no
`outputDirectory`, so Vercel published the **repository root**. Every pipeline
script (`build_dashboard.py`, `r2_sync.py`, ...), `config.yaml`, `CLAUDE.md`
and the CI workflow were publicly readable over HTTP. Secrets were not exposed
(`.env` is gitignored and was never deployed), but do not let this regress.
