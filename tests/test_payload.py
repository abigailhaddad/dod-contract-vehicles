"""Guards on the encoded dashboard payloads.

Two things this exists to catch.

1. Size regression. vehicles.json and families.json are downloaded in full by
   every visitor, from a public R2 endpoint that serves them with NO
   content-encoding -- requesting families.json with
   `Accept-Encoding: gzip, br` returned 42,639,945 bytes and no
   content-encoding header. So the file size is the wire size, and the pair
   was 127 MB per page view. Columnar + dictionary encoding took them to
   ~14 MB. A field the frontend never reads getting re-added to a per-row
   record, or `indent=` coming back in build_dashboard.py, would silently
   undo that. PAYLOAD_BUDGET_BYTES fails the build instead.

2. Writer/reader drift. The payload is only readable because
   decodeVehicles() / decodeFamilies() in web/index.html mirror
   decode_vehicles() / decode_families() in payload.py exactly. If the Python
   side starts emitting a field the JS side does not materialize, the browser
   renders a page of blank cells rather than raising -- so the two key sets
   are compared here.

The size checks need a built web/data/ and skip without one; everything else
runs on synthetic rows and so runs on every push via publish-check.yml.
"""
import json
import re
from pathlib import Path

import pytest

import build_dashboard as bd
import payload as pl

DATA_DIR = Path("web/data")
INDEX_HTML = Path("web/index.html")

needs_data = pytest.mark.skipif(
    not (DATA_DIR / "vehicles.json").exists(),
    reason="web/data/ not built (publish-check runs on a bare checkout)",
)


# -----------------------------------------------------------------------------
# Synthetic rows -- no built data needed
# -----------------------------------------------------------------------------

def _vehicle(piid="W91QUZ24D0001", method="solicitation", key="abc1234567"):
    return {
        "family_id": f"FAM_{method}_{key}", "family_method": method,
        "status": "Active", "parent_award_type": "IDC",
        "sub_agency": "Department of the Army", "awarding_office": "ACC-APG",
        "latest_end": "2030-01-31", "parent_piid": piid,
        "contractors": ["ACME INC.", "BETA LLC"], "naics_codes": ["541512"],
        "states": ["MD", "VA"],
        "order_count": 3, "active_orders": 2, "contractor_count": 2,
        "total_ceiling": 1_000_000, "total_obligated": 250_000,
        "ceiling_remaining": 750_000, "pct_ceiling_used": 25.0,
        "top_orders": [{
            "piid": "W91QUZ24F0007", "contractor": "ACME INC.",
            "ceiling": 500_000, "obligated": 100_000, "pop_end": "2028-09-30",
            "status": "Active",
            "link": f"https://www.usaspending.gov/award/CONT_AWD_W91QUZ24F0007_2100_{piid}_2100/",
        }],
    }


def _family(method="solicitation", key="abc1234567"):
    return {
        "family_id": f"FAM_{method}_{key}", "family_method": method,
        "status": "Active", "sub_agency": "Department of the Army",
        "awarding_office": "ACC-APG", "latest_end": "2030-01-31",
        "description": "ENGINEERING SERVICES", "primary_solicitation": "W91QUZ24R0001",
        "multi_award": True, "contractors": ["ACME INC."], "naics_codes": ["541512"],
        "member_piids": ["W91QUZ24D0001", "W91QUZ24D0002"],
        "member_count": 2, "order_count": 3, "contractor_count": 1,
        "family_ceiling": 1_000_000, "total_obligated": 250_000,
        "ceiling_remaining": 750_000, "pct_ceiling_used": 25.0,
    }


def test_vehicle_roundtrip_is_lossless():
    rows = [_vehicle(), _vehicle("W91QUZ24D0002", "singleton", "def7654321")]
    rows[1]["top_orders"] = []
    rows[1]["contractors"] = []
    rows[1]["total_obligated"] = None  # null must survive as null, not 0
    out = pl.decode_vehicles(pl.encode_vehicles(rows))
    assert out == rows


def test_family_roundtrip_is_lossless():
    rows = [_family(), _family("singleton", "def7654321")]
    rows[1]["description"] = None
    rows[1]["multi_award"] = False
    out = pl.decode_families(pl.encode_families(rows))
    assert out == rows


def test_encode_of_decode_is_byte_identical():
    """Pool ordering must be a pure function of row order, or two builds of
    the same data would produce different bytes."""
    p = pl.encode_vehicles([_vehicle(), _vehicle("W91QUZ24D0002")])
    assert pl.encode_vehicles(pl.decode_vehicles(p)) == p


def test_dropped_fields_are_absent_from_the_encoding():
    """The fields removed here were on every row and read by nothing. If one
    comes back, it comes back for every visitor."""
    v = json.dumps(pl.encode_vehicles([_vehicle()]))
    f = json.dumps(pl.encode_families([_family()]))
    for name in pl.DROPPED_VEHICLE_FIELDS + pl.DROPPED_ORDER_FIELDS:
        assert f'"{name}"' not in v, f"{name} is back in the vehicles payload"
    for name in pl.DROPPED_FAMILY_FIELDS:
        assert f'"{name}"' not in f, f"{name} is back in the families payload"


def test_dropped_fields_do_not_overlap_decoded_fields():
    assert not set(pl.DROPPED_VEHICLE_FIELDS) & set(pl.DECODED_VEHICLE_FIELDS)
    assert not set(pl.DROPPED_FAMILY_FIELDS) & set(pl.DECODED_FAMILY_FIELDS)
    assert not set(pl.DROPPED_ORDER_FIELDS) & set(pl.DECODED_ORDER_FIELDS)


def test_unrebuildable_family_id_raises():
    """family_id is not stored -- only its hash tail is. A id that stops
    matching FAM_{method}_{hash} would decode to the wrong string and silently
    break the families drill-down, which joins vehicles on family_id."""
    row = _vehicle()
    row["family_id"] = "SOMETHING_ELSE"
    with pytest.raises(ValueError, match="does not start with"):
        pl.encode_vehicles([row])


def test_unrebuildable_order_link_raises():
    """An order's link is not stored -- only the agency-code pair is. If a
    link stops matching the USASpending template the modal would render a dead
    link, so the build must fail instead."""
    row = _vehicle()
    row["top_orders"][0]["link"] = "https://example.com/whatever"
    with pytest.raises(ValueError, match="not rebuildable"):
        pl.encode_vehicles([row])


def test_order_link_survives_a_non_dod_agency_pair():
    row = _vehicle()
    row["top_orders"][0]["link"] = (
        "https://www.usaspending.gov/award/CONT_AWD_W91QUZ24F0007_9700_W91QUZ24D0001_4732/"
    )
    out = pl.decode_vehicles(pl.encode_vehicles([row]))
    assert out[0]["top_orders"][0]["link"] == row["top_orders"][0]["link"]


# -----------------------------------------------------------------------------
# Writer / reader parity
# -----------------------------------------------------------------------------

def _js_object_keys(func_name: str, literal: str) -> set[str]:
    """Keys of the row object literal a JS decoder builds."""
    html = INDEX_HTML.read_text()
    start = html.index(f"function {func_name}(p) {{")
    end = html.index("\n    function ", start + 1) if "\n    function " in html[start + 1:] \
        else len(html)
    body = html[start:end]
    block = re.search(re.escape(literal) + r"\s*\{(.*?)\n\s*\};", body, re.S)
    assert block, f"could not find the {literal} object literal in {func_name}()"
    return set(re.findall(r"^\s*([a-z_]+):", block.group(1), re.M))


def test_js_vehicle_decoder_matches_python():
    js = _js_object_keys("decodeVehicles", "rows[i] =")
    assert js == set(pl.DECODED_VEHICLE_FIELDS), (
        f"decodeVehicles() in index.html and decode_vehicles() in payload.py "
        f"disagree: only in JS {sorted(js - set(pl.DECODED_VEHICLE_FIELDS))}, "
        f"only in Python {sorted(set(pl.DECODED_VEHICLE_FIELDS) - js)}"
    )


def test_js_order_decoder_matches_python():
    js = _js_object_keys("decodeVehicles", "orders[m] =")
    assert js == set(pl.DECODED_ORDER_FIELDS), (
        f"top_orders decoding disagrees: only in JS "
        f"{sorted(js - set(pl.DECODED_ORDER_FIELDS))}, only in Python "
        f"{sorted(set(pl.DECODED_ORDER_FIELDS) - js)}"
    )


def test_js_family_decoder_matches_python():
    js = _js_object_keys("decodeFamilies", "rows[i] =")
    assert js == set(pl.DECODED_FAMILY_FIELDS), (
        f"decodeFamilies() in index.html and decode_families() in payload.py "
        f"disagree: only in JS {sorted(js - set(pl.DECODED_FAMILY_FIELDS))}, "
        f"only in Python {sorted(set(pl.DECODED_FAMILY_FIELDS) - js)}"
    )


def test_every_column_the_dashboard_declares_is_decoded():
    """COLUMNS / FAM_COLUMNS drive the filter bar, the sort keys and the CSV
    export; the `data:` bindings drive the table cells. A name in any of them
    that the decoder does not produce renders as a blank column, not an
    error."""
    html = INDEX_HTML.read_text()
    declared = set(re.findall(r"field:\s*'([a-z_]+)'", html))
    declared |= set(re.findall(r"data:\s*'([a-z_]+)'", html))
    assert declared, "parsed no column names out of index.html -- check the regex"
    known = set(pl.DECODED_VEHICLE_FIELDS) | set(pl.DECODED_FAMILY_FIELDS)
    missing = sorted(declared - known)
    assert not missing, (
        f"index.html renders {missing}, which neither decoder produces -- "
        f"those columns would be blank"
    )


# -----------------------------------------------------------------------------
# Size budget
# -----------------------------------------------------------------------------

@needs_data
@pytest.mark.parametrize("name", sorted(bd.PAYLOAD_BUDGET_BYTES))
def test_payload_is_within_budget(name):
    size = (DATA_DIR / name).stat().st_size
    budget = bd.PAYLOAD_BUDGET_BYTES[name]
    assert size <= budget, (
        f"{name} is {size:,} bytes, over its {budget:,}-byte budget. Every "
        f"visitor downloads this file uncompressed from r2.dev. Check for a "
        f"field added to the per-row record that nothing renders, or for "
        f"indent= creeping back into build_dashboard.py."
    )


@needs_data
def test_total_payload_is_within_budget():
    total = sum(p.stat().st_size for p in DATA_DIR.glob("*.json"))
    assert total <= bd.TOTAL_PAYLOAD_BUDGET_BYTES, (
        f"web/data/*.json is {total:,} bytes total, over the "
        f"{bd.TOTAL_PAYLOAD_BUDGET_BYTES:,}-byte budget for one cold page load."
    )


@needs_data
def test_check_payload_budget_passes_on_the_real_build():
    assert bd.check_payload_budget(DATA_DIR) == []


def test_check_payload_budget_catches_an_oversized_payload(tmp_path):
    """Negative test: the guard must actually fire."""
    over = bd.PAYLOAD_BUDGET_BYTES["vehicles.json"] + 1
    (tmp_path / "vehicles.json").write_bytes(b"x" * over)
    result = bd.check_payload_budget(tmp_path)
    assert ("vehicles.json", over, bd.PAYLOAD_BUDGET_BYTES["vehicles.json"]) in result


def test_check_payload_budget_catches_an_oversized_total(tmp_path):
    """Negative test: files that are individually fine can still add up."""
    (tmp_path / "vehicles.json").write_bytes(b"x" * (13 * 1024 * 1024))
    (tmp_path / "families.json").write_bytes(b"x" * (8 * 1024 * 1024))
    (tmp_path / "other.json").write_bytes(b"x" * (8 * 1024 * 1024))
    names = [n for n, _, _ in bd.check_payload_budget(tmp_path)]
    assert names == ["TOTAL"], f"expected only the combined budget to fire, got {names}"


@needs_data
def test_shipped_payloads_are_not_pretty_printed():
    """indent=2 was ~30% of the old wire size."""
    for name in ("vehicles.json", "families.json"):
        head = (DATA_DIR / name).read_bytes()[:400]
        assert b"\n" not in head, f"{name} looks pretty-printed -- drop indent="


@needs_data
def test_shipped_payloads_decode_and_reencode_identically():
    v = json.loads((DATA_DIR / "vehicles.json").read_text())
    f = json.loads((DATA_DIR / "families.json").read_text())
    assert pl.encode_vehicles(pl.decode_vehicles(v)) == v
    assert pl.encode_families(pl.decode_families(f)) == f
