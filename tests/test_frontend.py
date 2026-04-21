"""Frontend smoke tests. Boot a local http.server serving web/, drive it
with Playwright, verify the critical paths work.

Focus (per prior breakage):
  - Page loads without console errors
  - Stats render non-zero numbers
  - Filter chips: add a multiselect filter, chip appears, rows reduce,
    clearing restores
  - PIID drill-down modal opens with orders
  - Methodology page picks up live config values
"""
import re

import pytest


@pytest.fixture
def console_errors(page):
    errors = []
    page.on("pageerror", lambda err: errors.append(str(err)))
    page.on("console", lambda msg: errors.append(msg.text) if msg.type == "error" else None)
    return errors


def _wait_for_table(page, selector="#familiesTable tbody tr"):
    # Families tab is the landing view now; its table must render first.
    page.wait_for_selector(selector, timeout=30_000)


# -----------------------------------------------------------------------------
# Core page load
# -----------------------------------------------------------------------------

def test_index_loads(page, base_url, console_errors):
    page.goto(base_url + "/", wait_until="networkidle")
    assert "Contract Vehicles" in page.title()
    _wait_for_table(page)
    # Ignore 3rd-party noise (CDN preconnect warnings) but fail on our own errors
    ours = [e for e in console_errors if "localhost" in e or "127.0.0.1" in e]
    assert not ours, f"console errors: {ours}"


def test_stats_render_nonzero(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)
    # Family-tab stats are visible by default.
    for stat_id in ("famStatActive", "famStatMembers", "famStatCeiling", "famStatRemaining", "famStatContractors"):
        text = page.locator(f"#{stat_id}").inner_text()
        assert text not in ("--", ""), f"#{stat_id} is empty"
        assert re.search(r"\d", text), f"#{stat_id} has no number: {text!r}"


def test_banner_present(page, base_url):
    page.goto(base_url + "/")
    text = page.locator(".experimental-banner").inner_text()
    assert "experimental" in text.lower()


# -----------------------------------------------------------------------------
# Filters
# -----------------------------------------------------------------------------

def test_add_filter_button_opens_dialog(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)
    page.click(".add-filter-btn")
    page.wait_for_selector(".filter-popover", timeout=3_000)
    # Dialog should offer filterable columns, including Sub-Agency
    assert page.locator(".filter-option").count() > 3


def test_multiselect_filter_applies_and_clears(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)

    def row_count():
        # "Showing _START_ to _END_ of _TOTAL_" — grab TOTAL
        info = page.locator(".dataTables_info").first.inner_text()
        m = re.search(r"of ([\d,]+)", info)
        assert m, f"couldn't parse row count from {info!r}"
        return int(m.group(1).replace(",", ""))

    before = row_count()
    assert before > 0

    # Open Add Filter, click Sub-Agency (checkbox click auto-advances to the
    # per-filter dialog, so the outer dialog element gets detached -- use
    # .click() not .check(), which would wait to verify post-click state).
    page.click(".add-filter-btn")
    page.wait_for_selector(".filter-popover")
    page.locator(".filter-option", has_text="Sub-Agency").first.locator("input").click()

    # Multiselect dialog opens; pick the first option and apply
    page.wait_for_selector(".filter-popover:has-text('Filter: Sub-Agency')")
    page.locator(".filter-popover:has-text('Filter: Sub-Agency') .filter-option input").first.click()
    page.click(".filter-popover:has-text('Filter: Sub-Agency') .btn-apply")

    # Chip should appear
    page.wait_for_selector(".filter-chip.column-filter-chip", timeout=3_000)
    assert page.locator(".filter-chip.column-filter-chip").count() == 1

    # Filtered row count should be less than the total
    page.wait_for_timeout(300)  # let draw() settle
    filtered = row_count()
    assert filtered < before, f"filter didn't reduce rows: {before} -> {filtered}"
    assert filtered > 0, "filter reduced to zero rows -- probably broken"

    # Clear via the chip's X
    page.locator(".filter-chip-remove").first.click()
    page.wait_for_timeout(300)
    restored = row_count()
    assert restored == before, f"clearing filter didn't restore: before={before} after={restored}"


def test_clear_all_filters(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)

    # Apply a filter first (see note above re .click() vs .check())
    page.click(".add-filter-btn")
    page.wait_for_selector(".filter-popover")
    page.locator(".filter-option", has_text="Status").first.locator("input").click()
    page.wait_for_selector(".filter-popover:has-text('Filter: Status')")
    page.locator(".filter-popover:has-text('Filter: Status') .filter-option input").first.click()
    page.click(".filter-popover:has-text('Filter: Status') .btn-apply")
    page.wait_for_selector(".filter-chip.column-filter-chip")

    # Clear All
    page.click(".clear-filters-btn")
    page.wait_for_timeout(200)
    assert page.locator(".filter-chip.column-filter-chip").count() == 0


# -----------------------------------------------------------------------------
# Drill-down
# -----------------------------------------------------------------------------

def test_family_drill_down_opens_modal(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)
    # Click the first family's description/PIIDs link (opens a sibling drill-down)
    page.locator("#familiesTable a.piid-link").first.click()
    page.wait_for_selector(".orders-popover", timeout=3_000)
    # Drill-down title is the family description or id
    assert page.locator(".orders-popover h3").count() == 1
    has_table = page.locator(".orders-popover table tbody tr").count() > 0
    has_empty = page.locator(".orders-popover .empty").count() > 0
    assert has_table or has_empty


def test_tab_switch_to_vehicles(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)
    # Vehicles tab link exists and switching reveals its table
    page.locator(".tab-link", has_text="Vehicles").click()
    page.wait_for_selector("#tab-vehicles.active", timeout=3_000)
    # The vehicles table should have rows populated (DataTable was built on load)
    page.wait_for_selector("#vehiclesTable tbody tr", timeout=10_000)
    # And vehicle-level stats should now be populated
    for stat_id in ("statActive", "statOrders", "statCeiling"):
        text = page.locator(f"#{stat_id}").inner_text()
        assert text not in ("--", ""), f"#{stat_id} is empty after tab switch"


def test_tab_switch_to_grouping(page, base_url):
    page.goto(base_url + "/", wait_until="networkidle")
    _wait_for_table(page)
    page.locator(".tab-link", has_text="Grouping Methods").click()
    page.wait_for_selector("#tab-grouping.active", timeout=3_000)
    # Audit renders method cards for each grouper
    page.wait_for_selector(".method-card", timeout=5_000)
    assert page.locator(".method-card").count() >= 4


# -----------------------------------------------------------------------------
# Methodology
# -----------------------------------------------------------------------------

def test_methodology_renders_live_config(page, base_url):
    page.goto(base_url + "/methodology.html", wait_until="networkidle")
    # Wait for the config.json fetch to populate the DOM
    page.wait_for_function(
        "document.getElementById('tblEsDays')?.innerText?.match(/^\\d+$/)",
        timeout=5_000,
    )
    es_days = page.locator("#tblEsDays").inner_text().strip()
    assert es_days.isdigit()
    assert int(es_days) > 0

    agency = page.locator("#tblAgency").inner_text().strip()
    assert agency, "agency code missing on methodology page"

    # as_of should be a YYYY-MM-DD
    as_of = page.locator("#asOf").inner_text().strip()
    assert re.match(r"\d{4}-\d{2}-\d{2}$", as_of), f"bad as_of: {as_of!r}"


def test_nav_links_resolve(page, base_url):
    page.goto(base_url + "/")
    # Methodology is still a separate page
    page.locator(".page-nav a", has_text="Methodology").click()
    page.wait_for_url(re.compile(r"methodology\.html$"))
