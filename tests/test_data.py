"""Invariants that every build of web/data/*.json must satisfy.

Runs against whatever is in web/data/ -- the build job writes there before
calling pytest. If anything fails, the workflow aborts before promoting
from the staging R2 prefix to the prod one.
"""
import json
from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

DATA_DIR = Path("web/data")
CONFIG_YAML = Path("config.yaml")


@pytest.fixture(scope="module")
def summary():
    return json.loads((DATA_DIR / "summary.json").read_text())


@pytest.fixture(scope="module")
def vehicles():
    return json.loads((DATA_DIR / "vehicles.json").read_text())


@pytest.fixture(scope="module")
def filters():
    return json.loads((DATA_DIR / "filters.json").read_text())


@pytest.fixture(scope="module")
def config_mirror():
    return json.loads((DATA_DIR / "config.json").read_text())


@pytest.fixture(scope="module")
def config_yaml():
    return yaml.safe_load(CONFIG_YAML.read_text())


# -----------------------------------------------------------------------------
# summary.json
# -----------------------------------------------------------------------------

SUMMARY_FIELDS = {
    "total_contracts", "active_contracts", "expiring_soon",
    "total_ceiling_b", "total_obligated_b", "ceiling_remaining_b",
    "unique_contractors", "unique_vehicles", "unique_offices", "as_of",
}


def test_summary_has_all_fields(summary):
    assert SUMMARY_FIELDS == set(summary.keys()), \
        f"summary.json fields drifted: missing={SUMMARY_FIELDS - summary.keys()}, extra={summary.keys() - SUMMARY_FIELDS}"


def test_summary_as_of_is_parseable(summary):
    datetime.strptime(summary["as_of"], "%Y-%m-%d")


def test_summary_as_of_is_recent(summary):
    as_of = datetime.strptime(summary["as_of"], "%Y-%m-%d").date()
    age_days = (date.today() - as_of).days
    assert 0 <= age_days <= 60, f"summary.as_of is {age_days} days old -- pipeline likely stale"


def test_summary_counts_are_positive(summary):
    assert summary["total_contracts"] > 0
    assert summary["active_contracts"] > 0
    assert summary["unique_contractors"] > 0
    assert summary["unique_vehicles"] > 0


def test_expiring_is_subset_of_active(summary):
    # "Active" headline stat is defined as the union of Active + Expiring Soon,
    # so expiring must be <= active_contracts.
    assert summary["expiring_soon"] <= summary["active_contracts"], \
        "expiring_soon > active_contracts -- Active card would look smaller than its subset"


def test_ceiling_sanity(summary):
    # DoD 5-yr contracts should be on the order of trillions, not quadrillions
    # (the $377T bug happened because IDV masters were double-counted).
    assert 100 <= summary["total_ceiling_b"] <= 50_000, \
        f"total_ceiling_b={summary['total_ceiling_b']} is outside plausible DoD 5yr range ($100B-$50T)"
    assert summary["total_obligated_b"] <= summary["total_ceiling_b"], \
        "obligated > ceiling at the headline level"
    assert abs(summary["ceiling_remaining_b"]
               - (summary["total_ceiling_b"] - summary["total_obligated_b"])) < 1, \
        "ceiling_remaining_b != ceiling - obligated"


# -----------------------------------------------------------------------------
# vehicles.json
# -----------------------------------------------------------------------------

ALLOWED_STATUSES = {"Active", "Expiring Soon"}


def test_vehicles_nonempty(vehicles):
    assert len(vehicles) > 1000, f"only {len(vehicles)} vehicles -- pipeline probably broken"


def test_every_vehicle_has_parent_piid(vehicles):
    bad = [v for v in vehicles if not v.get("parent_piid")]
    assert not bad, f"{len(bad)} vehicles missing parent_piid (first: {bad[0] if bad else None})"


def test_no_expired_or_unknown_vehicles(vehicles):
    # Drop policy: Expired + Unknown are dropped at build time.
    bad = [v for v in vehicles if v.get("status") not in ALLOWED_STATUSES]
    assert not bad, f"{len(bad)} vehicles with disallowed status (first: {bad[0].get('status')})"


def test_vehicle_counts_are_consistent(vehicles):
    for v in vehicles:
        assert v["order_count"] >= v["active_orders"], \
            f"vehicle {v['parent_piid']}: active_orders ({v['active_orders']}) > order_count ({v['order_count']})"
        assert v["active_orders"] >= 0


def test_vehicle_ceiling_arithmetic(vehicles):
    # ceiling_remaining = total_ceiling - total_obligated (within rounding).
    for v in vehicles:
        tc = v.get("total_ceiling") or 0
        to = v.get("total_obligated") or 0
        cr = v.get("ceiling_remaining") or 0
        # allow $1 rounding slop
        assert abs(cr - (tc - to)) <= 1, \
            f"vehicle {v['parent_piid']}: remaining={cr}, ceiling-obligated={tc-to}"


def test_top_orders_invariants(vehicles):
    # Drill-down: each vehicle's top_orders must be <= 10 and Active/Expiring only.
    for v in vehicles:
        orders = v.get("top_orders") or []
        assert len(orders) <= 10, f"{v['parent_piid']}: {len(orders)} top_orders > 10"
        for o in orders:
            assert o.get("status") in ALLOWED_STATUSES, \
                f"{v['parent_piid']} has order {o.get('piid')} with status {o.get('status')}"
        # Sorted by ceiling desc where possible
        ceilings = [o.get("ceiling") or 0 for o in orders]
        assert ceilings == sorted(ceilings, reverse=True), \
            f"{v['parent_piid']} top_orders not sorted by ceiling desc"


def test_no_insane_single_vehicle_ceiling(vehicles):
    # Largest DoD IDV is SEWP-class at ~$150B. A single vehicle row with >$500B
    # total_ceiling is almost certainly double-counting.
    bad = [v for v in vehicles if (v.get("total_ceiling") or 0) > 500_000_000_000]
    assert not bad, \
        f"{len(bad)} vehicles with total_ceiling > $500B (first: {bad[0]['parent_piid']} @ ${bad[0]['total_ceiling']:,.0f})"


# -----------------------------------------------------------------------------
# filters.json (filter-option lists)
# -----------------------------------------------------------------------------

FILTER_KEYS = {
    "statuses", "departments", "sub_agencies", "vehicle_types",
    "pricing_types", "naics_2digit", "states",
}


def test_filters_has_all_keys(filters):
    assert FILTER_KEYS.issubset(filters.keys())


def test_filter_statuses_match_drop_policy(filters, config_mirror):
    if config_mirror["classification"]["drop_expired"]:
        assert "Expired" not in filters["statuses"]
    if config_mirror["classification"]["drop_unknown"]:
        assert "Unknown" not in filters["statuses"]
    # Must contain at least the in-force buckets
    assert "Active" in filters["statuses"] or "Expiring Soon" in filters["statuses"]


def test_filter_lists_nonempty(filters):
    for key in ("departments", "sub_agencies", "vehicle_types", "states"):
        assert filters[key], f"filters.{key} is empty -- will break the filter chip UI"


# -----------------------------------------------------------------------------
# config.json mirrors config.yaml
# -----------------------------------------------------------------------------

def test_config_mirror_matches_yaml(config_mirror, config_yaml):
    assert config_mirror["fetch"]["agency_code"] == str(config_yaml["fetch"]["agency_code"])
    assert config_mirror["fetch"]["fiscal_years_back"] == int(config_yaml["fetch"]["fiscal_years_back"])
    assert config_mirror["classification"]["expiring_soon_days"] == \
        int(config_yaml["classification"]["expiring_soon_days"])
    assert config_mirror["classification"]["effective_end_fields"] == \
        list(config_yaml["classification"]["effective_end_fields"])
    assert config_mirror["classification"]["drop_expired"] == \
        bool(config_yaml["classification"]["drop_expired"])
    assert config_mirror["classification"]["drop_unknown"] == \
        bool(config_yaml["classification"]["drop_unknown"])


# -----------------------------------------------------------------------------
# cross-file consistency
# -----------------------------------------------------------------------------

def test_summary_vehicles_alignment(summary, vehicles):
    # unique_vehicles in summary should equal len(vehicles) (both in-force by construction)
    assert summary["unique_vehicles"] == len(vehicles), \
        f"summary.unique_vehicles ({summary['unique_vehicles']}) != len(vehicles.json) ({len(vehicles)})"
