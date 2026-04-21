"""
build_families.py -- Group parent IDV rollups ("vehicles") into multi-award
families (MATOCs / sibling IDIQs).

A "family" is a set of parent IDVs that were awarded out of the same
procurement and share a ceiling pool. Summing vehicle_ceiling across sibling
IDVs double- to 100x-counts real capacity, so aggregation needs a dedup layer.

Four methods, tried in priority order; first hit wins (each vehicle ends up in
exactly one family):

  1. solicitation     -- same solicitation_identifier. Strongest signal.
  2. shared_ceiling   -- same awarding_office + same vehicle_ceiling (>= $25M)
                         + overlapping PoP. Catches MATOCs where each sibling
                         redundantly carries the whole pool's $ value.
  3. piid_block       -- same awarding_office + same alpha PIID prefix + same
                         earliest_start + same latest_end, >=2 members. Catches
                         sibling IDVs numbered sequentially.
  4. singleton        -- everything else, family of one (keeps downstream
                         uniform: Families tab is just grouped vehicles).

The family_ceiling = max(vehicle_ceiling across members) -- for true MATOC
siblings these are equal by definition; max is robust to a missing value.
family_obligated/order_count sum across members (that reflects actual pool use).

Outputs:
  - annotated vehicles (each carries family_id + family_method)
  - families list (one record per family, with members)
  - audit dict (per-method stats + examples for SME review)
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter, defaultdict
from typing import Any

# Minimum ceiling to trust the shared_ceiling heuristic. Below this, matching
# dollar values are too plausibly coincidental (e.g. $10K base-award floors
# on every multi-award IDV).
SHARED_CEILING_MIN = 25_000_000

# Minimum family size to actually form a group under the heuristic methods.
# Solicitation method keeps even pairs (high confidence signal); shared_ceiling
# and piid_block require >=2 but we additionally reject pairs where the only
# thing linking them is a round-number ceiling.
MIN_FAMILY_SIZE = 2

_PIID_SPLIT = re.compile(r"^([A-Z0-9]*?)(\d+)$")


def _piid_prefix_num(piid: str) -> tuple[str, int] | None:
    """Split a PIID into (alpha-ish prefix, trailing integer). Returns None if
    it doesn't match that shape."""
    if not piid:
        return None
    m = _PIID_SPLIT.match(piid.strip().upper())
    if not m:
        return None
    return m.group(1), int(m.group(2))


def _hash_short(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()[:10]


def _pops_overlap(a_start: str | None, a_end: str | None,
                  b_start: str | None, b_end: str | None) -> bool:
    """Two date ranges overlap if neither ends before the other starts.
    Missing dates are treated as unbounded on that side."""
    if a_end and b_start and a_end < b_start:
        return False
    if b_end and a_start and b_end < a_start:
        return False
    return True


# ---------------------------------------------------------------------------
# Per-method groupers. Each takes `candidates` (list of vehicle dicts not yet
# assigned) and returns a list of groups (each group = list of vehicle dicts).
# Vehicles that don't cluster are left for the next method.
# ---------------------------------------------------------------------------

def _group_by_solicitation(candidates: list[dict]) -> tuple[list[list[dict]], list[dict]]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    leftover: list[dict] = []
    for v in candidates:
        sol = (v.get("primary_solicitation") or "").strip().upper()
        if sol:
            buckets[sol].append(v)
        else:
            leftover.append(v)
    groups: list[list[dict]] = []
    for sol, members in buckets.items():
        if len(members) >= MIN_FAMILY_SIZE:
            groups.append(members)
        else:
            leftover.extend(members)
    return groups, leftover


def _group_by_shared_ceiling(candidates: list[dict]) -> tuple[list[list[dict]], list[dict]]:
    # Key on (awarding_office, vehicle_ceiling). Only consider vehicles whose
    # vehicle_ceiling >= SHARED_CEILING_MIN -- smaller shared-ceiling matches
    # are coincidence-prone. Members must be flagged multi-award (either via
    # USASpending's IDV flag or via the inferred contractor-count signal) --
    # this cuts out DLA program-ceiling false positives (e.g. DLA Hospital
    # Supply, Noble Supply) where many single-award IDVs share a program's
    # rolled-up ceiling.
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    leftover: list[dict] = []
    for v in candidates:
        if not v.get("multi_award"):
            leftover.append(v)
            continue
        vc = v.get("vehicle_ceiling")
        office = v.get("awarding_office")
        if vc and vc >= SHARED_CEILING_MIN and office:
            buckets[(office, vc)].append(v)
        else:
            leftover.append(v)

    groups: list[list[dict]] = []
    for (_office, _vc), members in buckets.items():
        if len(members) < MIN_FAMILY_SIZE:
            leftover.extend(members)
            continue
        # Require at least 2 distinct contractors across the cluster. Kills
        # same-awardee duplicate-IDIQ collapses (e.g. one vendor holding two
        # separate item-supply IDIQs that happen to share a program ceiling).
        distinct_contractors = set()
        for m in members:
            for c in (m.get("contractors") or []):
                distinct_contractors.add(c)
        if len(distinct_contractors) < 2:
            leftover.extend(members)
            continue
        # Require at least one pairwise PoP overlap within the cluster.
        # (Pool MATOCs run concurrently; two unrelated contracts that happen
        # to share a round-number ceiling are likely on different timelines.)
        # Cheap check: split into connected components by overlapping ranges.
        components = _components_by_pop_overlap(members)
        for comp in components:
            if len(comp) >= MIN_FAMILY_SIZE:
                groups.append(comp)
            else:
                leftover.extend(comp)
    return groups, leftover


def _components_by_pop_overlap(members: list[dict]) -> list[list[dict]]:
    """Connected components where an edge = PoP windows overlap. O(n^2)
    per cluster — fine because buckets are small (office+ceiling keyed)."""
    n = len(members)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        ri, rj = find(i), find(j)
        if ri != rj:
            parent[ri] = rj

    for i in range(n):
        for j in range(i + 1, n):
            if _pops_overlap(
                members[i].get("earliest_start"), members[i].get("latest_end"),
                members[j].get("earliest_start"), members[j].get("latest_end"),
            ):
                union(i, j)

    comps: dict[int, list[dict]] = defaultdict(list)
    for i, v in enumerate(members):
        comps[find(i)].append(v)
    return list(comps.values())


def _group_by_piid_block(candidates: list[dict]) -> tuple[list[list[dict]], list[dict]]:
    """Same office + same alpha PIID prefix + identical (start, end) dates.
    Captures sibling IDVs numbered sequentially (e.g. W912QR24D0054..0063)
    that lack a solicitation_id and whose vehicle_ceiling is missing or too
    small for shared_ceiling to pick up. Gate on multi-award: a block of
    consecutively-numbered IDVs that's all single-award is not a MATOC."""
    buckets: dict[tuple, list[dict]] = defaultdict(list)
    leftover: list[dict] = []
    for v in candidates:
        if not v.get("multi_award"):
            leftover.append(v)
            continue
        office = v.get("awarding_office")
        start = v.get("earliest_start")
        end = v.get("latest_end")
        parsed = _piid_prefix_num(v.get("parent_piid") or "")
        if not (office and start and end and parsed):
            leftover.append(v)
            continue
        prefix, _ = parsed
        buckets[(office, prefix, start, end)].append(v)

    groups: list[list[dict]] = []
    for key, members in buckets.items():
        if len(members) < MIN_FAMILY_SIZE:
            leftover.extend(members)
            continue
        # Require at least 2 distinct contractors across the block.
        distinct_contractors = set()
        for m in members:
            for c in (m.get("contractors") or []):
                distinct_contractors.add(c)
        if len(distinct_contractors) < 2:
            leftover.extend(members)
            continue
        groups.append(members)
    return groups, leftover


# ---------------------------------------------------------------------------
# Family aggregate + annotation
# ---------------------------------------------------------------------------

def _aggregate_family(family_id: str, method: str, members: list[dict]) -> dict:
    # Ceiling: max across members (MATOC siblings share one pool value).
    ceilings = [m.get("vehicle_ceiling") for m in members if m.get("vehicle_ceiling")]
    order_sums = [m.get("order_ceiling_sum") or m.get("total_ceiling") or 0 for m in members]
    family_ceiling = max(ceilings) if ceilings else max(order_sums) if order_sums else None

    obligated = sum(m.get("total_obligated") or 0 for m in members)
    order_count = sum(m.get("order_count") or 0 for m in members)
    active_orders = sum(m.get("active_orders") or 0 for m in members)

    contractors: set[str] = set()
    naics: set[str] = set()
    naics_desc: set[str] = set()
    states: set[str] = set()
    descriptions: set[str] = set()
    sols: set[str] = set()
    offices: set[str] = set()
    sub_agencies: set[str] = set()
    departments: set[str] = set()
    vehicle_types: set[str] = set()
    for m in members:
        for c in (m.get("contractors") or []):
            contractors.add(c)
        for n in (m.get("naics_codes") or []):
            naics.add(n)
        for n in (m.get("naics_descriptions") or []):
            naics_desc.add(n)
        for s in (m.get("states") or []):
            states.add(s)
        for d in (m.get("descriptions") or []):
            descriptions.add(d)
        if m.get("primary_solicitation"):
            sols.add(m["primary_solicitation"])
        if m.get("awarding_office"):
            offices.add(m["awarding_office"])
        if m.get("sub_agency"):
            sub_agencies.add(m["sub_agency"])
        if m.get("department"):
            departments.add(m["department"])
        if m.get("vehicle_type"):
            vehicle_types.add(m["vehicle_type"])

    earliest_start = min((m.get("earliest_start") for m in members if m.get("earliest_start")), default=None)
    latest_end = max((m.get("latest_end") for m in members if m.get("latest_end")), default=None)
    latest_potential_end = max((m.get("latest_potential_end") for m in members if m.get("latest_potential_end")), default=None)
    latest_effective_end = max((m.get("latest_effective_end") for m in members if m.get("latest_effective_end")), default=None)

    # Status: priority Active > Expiring Soon > Unknown > Expired (member
    # statuses can differ when some siblings have been fully delivered).
    status_rank = {"Active": 0, "Expiring Soon": 1, "Unknown": 2, "Expired": 3}
    best_status = min((m.get("status", "Unknown") for m in members),
                      key=lambda s: status_rank.get(s, 9))

    # Always write remaining so downstream arithmetic checks out even when
    # family_ceiling is missing (e.g. SeaPort-NxG-style pools with no
    # published pool ceiling in USASpending).
    ceiling_remaining = (family_ceiling or 0) - (obligated or 0)
    pct_used = round(obligated / family_ceiling * 100, 1) if family_ceiling else None

    # Preferred display description: longest among member descriptions.
    desc_list = sorted(descriptions, key=len, reverse=True)
    display_desc = desc_list[0] if desc_list else None

    any_multi_award = any(m.get("multi_award") for m in members)
    return {
        "family_id":            family_id,
        "family_method":        method,
        "member_count":         len(members),
        "member_piids":         sorted(m.get("parent_piid") for m in members if m.get("parent_piid")),
        "multi_award":          any_multi_award,
        "primary_solicitation": sorted(sols)[0] if sols else None,
        "solicitations":        sorted(sols),
        "description":          display_desc,
        "department":           sorted(departments)[0] if departments else None,
        "sub_agency":           sorted(sub_agencies)[0] if sub_agencies else None,
        "awarding_office":      sorted(offices)[0] if offices else None,
        "office_count":         len(offices),
        "vehicle_type":         sorted(vehicle_types)[0] if vehicle_types else None,
        "contractor_count":     len(contractors),
        "contractors":          sorted(contractors)[:20],
        "naics_codes":          sorted(naics)[:10],
        "naics_descriptions":   sorted(naics_desc)[:5],
        "states":               sorted(states)[:10],
        "order_count":          order_count,
        "active_orders":        active_orders,
        "family_ceiling":       round(family_ceiling) if family_ceiling else None,
        "total_obligated":      round(obligated) if obligated else None,
        "ceiling_remaining":    round(ceiling_remaining),
        "pct_ceiling_used":     pct_used,
        "earliest_start":       earliest_start,
        "latest_end":           latest_end,
        "latest_potential_end": latest_potential_end,
        "latest_effective_end": latest_effective_end,
        "status":               best_status,
    }


def _sample(items: list, n: int) -> list:
    """Deterministic 'sample' — take first, last, and evenly-spaced middle.
    No randomness so re-runs produce a stable audit file."""
    if len(items) <= n:
        return list(items)
    step = max(1, len(items) // n)
    picked = items[::step][:n]
    return picked


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_families(vehicles: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """Group vehicles into families. Mutates each vehicle dict to add
    family_id + family_method. Returns (vehicles, families, audit)."""
    remaining = list(vehicles)
    all_groups: list[tuple[str, list[dict]]] = []

    for method_name, fn in (
        ("solicitation", _group_by_solicitation),
        ("shared_ceiling", _group_by_shared_ceiling),
        ("piid_block", _group_by_piid_block),
    ):
        groups, remaining = fn(remaining)
        for g in groups:
            all_groups.append((method_name, g))

    # Singletons — each remaining vehicle becomes its own family.
    for v in remaining:
        all_groups.append(("singleton", [v]))

    families: list[dict] = []
    method_counts: Counter = Counter()
    method_examples: dict[str, list[dict]] = defaultdict(list)

    for method, members in all_groups:
        key_parts = sorted(m.get("parent_piid") or "" for m in members)
        family_id = f"FAM_{method}_{_hash_short('|'.join(key_parts))}"
        fam = _aggregate_family(family_id, method, members)
        families.append(fam)
        method_counts[method] += 1
        for m in members:
            m["family_id"] = family_id
            m["family_method"] = method

    # Audit: for each non-singleton method, record stats + example families
    # so an SME can eyeball whether the heuristic is pulling together what
    # it should. Sample deterministically.
    audit: dict[str, Any] = {"methods": {}}
    for method in ("solicitation", "shared_ceiling", "piid_block", "singleton"):
        fams_in_method = [f for f in families if f["family_method"] == method]
        member_totals = [f["member_count"] for f in fams_in_method]
        audit_entry: dict[str, Any] = {
            "family_count":       len(fams_in_method),
            "vehicle_count":      sum(member_totals),
            "median_members":     sorted(member_totals)[len(member_totals) // 2] if member_totals else 0,
            "max_members":        max(member_totals) if member_totals else 0,
            "total_ceiling":      sum(f.get("family_ceiling") or 0 for f in fams_in_method),
            "total_obligated":    sum(f.get("total_obligated") or 0 for f in fams_in_method),
        }
        if method != "singleton":
            # Examples: bias toward families with more members (likelier to
            # matter) and include one small family for contrast.
            multi = [f for f in fams_in_method if f["member_count"] >= 3]
            small = [f for f in fams_in_method if f["member_count"] == 2]
            examples_src = sorted(multi, key=lambda f: -f["member_count"])[:8] + _sample(small, 2)
            audit_entry["examples"] = [
                {
                    "family_id":         f["family_id"],
                    "member_count":      f["member_count"],
                    "member_piids":      f["member_piids"][:12],
                    "description":       f.get("description"),
                    "awarding_office":   f.get("awarding_office"),
                    "sub_agency":        f.get("sub_agency"),
                    "family_ceiling":    f.get("family_ceiling"),
                    "primary_solicitation": f.get("primary_solicitation"),
                    "contractors":       f.get("contractors", [])[:10],
                }
                for f in examples_src
            ]
        audit["methods"][method] = audit_entry

    audit["totals"] = {
        "families":    len(families),
        "vehicles":    len(vehicles),
        "total_family_ceiling":   sum(f.get("family_ceiling") or 0 for f in families),
        "total_family_obligated": sum(f.get("total_obligated") or 0 for f in families),
    }

    return vehicles, families, audit
