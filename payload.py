"""payload.py -- columnar encoding for web/data/vehicles.json + families.json.

Why this exists
---------------
Every visitor to the dashboard downloads these two files in full. Served from
`*.r2.dev` they arrive with no `content-encoding` at all, so the wire size IS
the file size. Before this module they were 84.4 MB + 42.6 MB = 127 MB of
pretty-printed JSON per page view.

Three things were paying for that, in order of cost:

  1. `indent=2`. A pretty-printed array of 59,171 objects with a nested
     array of order objects spends ~30% of its bytes on newlines and spaces.
  2. Repeated key names. "awarding_office" was written 59,171 times in
     vehicles.json and 40,919 times in families.json.
  3. Repeated values. "Department of Defense" appeared on every vehicle row;
     "MISSILE DEFENSE AGENCY (MDA)" on thousands; contractor names recur
     across both the vehicle-level list and the order drill-down.

Plus fields nothing read. Twelve vehicle fields and ten family fields were
written on every row and referenced nowhere in web/index.html,
web/methodology.html or web/shared/shared.js -- see DROPPED_* below.

The encoding
------------
Columnar: one array per field, in row order, instead of one object per row.
Low-cardinality strings become an index into a shared pool (`dicts[pool]`),
with -1 for null. Numbers are stored as-is so null stays null. Arrays of
strings become arrays of pool indices.

`top_orders` is flattened CSR-style: `orders.counts[i]` says how many orders
row i owns, and the order columns are one long flat array read in sequence.
That removes the per-order `{` `}` and eight repeated key names.

Two values are derived rather than stored:

  * `family_id` is always "FAM_{family_method}_{hash}" and family_method is
    already a column, so only the hash is stored (`fam_key`).
  * an order's `link` is always
    "https://www.usaspending.gov/award/CONT_AWD_{piid}_{a1}_{parent_piid}_{a2}/"
    and piid / parent_piid are already present, so only the (a1, a2) agency
    pair is stored -- 18 distinct pairs across 57,480 orders.

Both derivations are checked against the real value for every row at encode
time; a mismatch raises rather than shipping a blank cell or a dead link.

Pool ordering is first-seen, which is deterministic for a given row order.
`encode_vehicles(decode_vehicles(p)) == p` is asserted in tests.

Result: 84.4 MB -> 9.2 MB and 42.6 MB -> 5.7 MB, with the browser decoding
back to exactly the row objects the rest of index.html already expects.
decodeVehicles() / decodeFamilies() in web/index.html are the mirrors of
decode_vehicles() / decode_families() here -- change them together.
"""

PAYLOAD_VERSION = 1

FAMILY_ID_PREFIX = "FAM_"
ORDER_LINK = (
    "https://www.usaspending.gov/award/CONT_AWD_{piid}_{a1}_{parent}_{a2}/"
)

# ---------------------------------------------------------------------------
# vehicles.json
# ---------------------------------------------------------------------------

# field -> dict pool. Pools are shared where the vocabulary is shared.
V_DICT = {
    "status":            "status",
    "family_method":     "method",
    "parent_award_type": "award_type",
    "sub_agency":        "sub_agency",
    "awarding_office":   "office",
    "latest_end":        "date",
    "fam_key":           "fam_key",
}
# field -> dict pool, stored as a list of indices per row.
V_LIST = {
    "contractors": "contractor",
    "naics_codes": "naics",
    "states":      "state",
}
# Stored as-is. null is preserved; the UI distinguishes null from 0.
V_NUM = [
    "order_count", "active_orders", "contractor_count",
    "total_ceiling", "total_obligated", "ceiling_remaining", "pct_ceiling_used",
]
# Stored verbatim (one distinct value per row -- a dict would not pay).
V_RAW = ["parent_piid"]

# Per-order columns inside `orders`.
O_DICT = {"contractor": "contractor", "pop_end": "date", "status": "status"}
O_NUM = ["ceiling", "obligated"]
O_RAW = ["piid"]

# What decode_vehicles() materializes -- i.e. what index.html may reference.
DECODED_VEHICLE_FIELDS = sorted(
    list(V_DICT) + list(V_LIST) + V_NUM + V_RAW + ["family_id", "top_orders"]
)
DECODED_VEHICLE_FIELDS.remove("fam_key")  # internal; family_id replaces it
DECODED_ORDER_FIELDS = sorted(list(O_DICT) + O_NUM + O_RAW + ["link"])

# Written on every row before this change, referenced by no frontend file.
# Kept here so the removal is reviewable and a re-add is deliberate.
DROPPED_VEHICLE_FIELDS = [
    "department",            # constant "Department of Defense" on every row
    "descriptions",          # always [] -- built from a key that is never set
    "earliest_start",
    "latest_effective_end",
    "latest_potential_end",
    "multi_award",           # vehicles only; families still carries it
    "naics_descriptions",
    "order_ceiling_sum",
    "primary_solicitation",  # families' copy is the one the UI links to
    "vehicle_ceiling",
    "vehicle_obligated",
    "vehicle_type",          # the UI's "Type" column is parent_award_type
]
DROPPED_ORDER_FIELDS = ["pop_potential_end"]  # modal renders pop_end only

# ---------------------------------------------------------------------------
# families.json
# ---------------------------------------------------------------------------

F_DICT = {
    "status":               "status",
    "family_method":        "method",
    "sub_agency":           "sub_agency",
    "awarding_office":      "office",
    "latest_end":           "date",
    "description":          "description",
    "primary_solicitation": "solicitation",
    # Not rendered, but tests/test_data.py asserts the multi-award gate on it
    # and a bool column is ~80 KB. Cheaper to keep than to lose the invariant.
    "multi_award":          "flag",
}
F_LIST = {"contractors": "contractor", "naics_codes": "naics"}
F_NUM = [
    "member_count", "order_count", "contractor_count",
    "family_ceiling", "total_obligated", "ceiling_remaining", "pct_ceiling_used",
]
F_RAW = ["fam_key", "member_piids"]

DECODED_FAMILY_FIELDS = sorted(
    list(F_DICT) + list(F_LIST) + F_NUM + F_RAW + ["family_id"]
)
DECODED_FAMILY_FIELDS.remove("fam_key")

DROPPED_FAMILY_FIELDS = [
    "active_orders",         # vehicles tab shows this; families tab does not
    "department",
    "earliest_start",
    "latest_effective_end",
    "latest_potential_end",
    "naics_descriptions",
    "office_count",
    "solicitations",         # primary_solicitation is the one that is linked
    "states",                # FAM_COLUMNS has no States column
    "vehicle_type",
]


# ---------------------------------------------------------------------------
# Encoding helpers
# ---------------------------------------------------------------------------

class _Pool:
    """First-seen string interner. None encodes as -1."""

    def __init__(self):
        self.values = []
        self._index = {}

    def add(self, value):
        if value is None:
            return -1
        j = self._index.get(value)
        if j is None:
            j = len(self.values)
            self._index[value] = j
            self.values.append(value)
        return j


def _pools(*names):
    return {name: _Pool() for name in names}


def fam_key(family_id, family_method):
    """The variable tail of a family_id. build_families.py builds every id as
    f"FAM_{method}_{hash}", so the prefix and the method are redundant with
    the family_method column."""
    prefix = f"{FAMILY_ID_PREFIX}{family_method}_"
    if not family_id or not family_id.startswith(prefix):
        raise ValueError(
            f"family_id {family_id!r} does not start with {prefix!r}. "
            f"fam_key()/family_id_from() and their JS mirrors assume it does."
        )
    return family_id[len(prefix):]


def family_id_from(key, family_method):
    return f"{FAMILY_ID_PREFIX}{family_method}_{key}"


def order_link(piid, parent_piid, agency_pair):
    """Rebuild an order's USASpending permalink from its parts."""
    if not agency_pair:
        return None
    a1, a2 = agency_pair.split("_")
    return ORDER_LINK.format(piid=piid, a1=a1, parent=parent_piid, a2=a2)


def _agency_pair(link, piid, parent_piid):
    """Inverse of order_link(). Returns "a1_a2", or None if `link` is not the
    template we think it is."""
    if not link:
        return None
    head = ORDER_LINK.split("{piid}")[0]
    if not link.startswith(head) or not link.endswith("/"):
        return None
    body = link[len(head):-1]
    parts = body.split("_")
    # {piid}_{a1}_{parent}_{a2}; no PIID in this dataset contains "_", which
    # is asserted by the caller via the round-trip check below.
    if len(parts) != 4:
        return None
    if parts[0] != piid or parts[2] != parent_piid:
        return None
    return f"{parts[1]}_{parts[3]}"


# ---------------------------------------------------------------------------
# vehicles
# ---------------------------------------------------------------------------

def encode_vehicles(rows):
    pools = _pools("status", "method", "award_type", "sub_agency", "office",
                   "date", "fam_key", "contractor", "naics", "state", "agency")
    cols = {}

    keys = [fam_key(r.get("family_id"), r.get("family_method")) for r in rows]
    for field, pool in V_DICT.items():
        source = keys if field == "fam_key" else [r.get(field) for r in rows]
        cols[field] = [pools[pool].add(v) for v in source]
    for field, pool in V_LIST.items():
        cols[field] = [
            [pools[pool].add(v) for v in (r.get(field) or [])] for r in rows
        ]
    for field in V_NUM + V_RAW:
        cols[field] = [r.get(field) for r in rows]

    orders = {"counts": []}
    for field in list(O_DICT) + O_NUM + O_RAW + ["agency"]:
        orders[field] = []
    bad_links = []
    for r in rows:
        rows_orders = r.get("top_orders") or []
        orders["counts"].append(len(rows_orders))
        for o in rows_orders:
            pair = _agency_pair(o.get("link"), o.get("piid"), r.get("parent_piid"))
            if order_link(o.get("piid"), r.get("parent_piid"), pair) != o.get("link"):
                bad_links.append((r.get("parent_piid"), o.get("piid"), o.get("link")))
            orders["agency"].append(pools["agency"].add(pair))
            for field, pool in O_DICT.items():
                orders[field].append(pools[pool].add(o.get(field)))
            for field in O_NUM + O_RAW:
                orders[field].append(o.get(field))

    if bad_links:
        raise ValueError(
            f"{len(bad_links)} order link(s) are not rebuildable from "
            f"(piid, parent_piid, agency pair), e.g. {bad_links[0]!r}. "
            f"The drill-down modal would render a dead or missing link. "
            f"Update order_link()/_agency_pair() and their JS mirrors."
        )

    return {
        "v": PAYLOAD_VERSION,
        "n": len(rows),
        "dicts": {name: p.values for name, p in pools.items()},
        "cols": cols,
        "orders": orders,
    }


def decode_vehicles(payload):
    """Python mirror of decodeVehicles() in web/index.html."""
    if payload.get("v") != PAYLOAD_VERSION:
        raise ValueError(f"unexpected vehicles payload version {payload.get('v')!r}")
    d, cols, orders, n = payload["dicts"], payload["cols"], payload["orders"], payload["n"]

    def pick(pool, j):
        return d[pool][j] if j >= 0 else None

    rows = []
    k = 0
    for i in range(n):
        method = pick("method", cols["family_method"][i])
        parent_piid = cols["parent_piid"][i]
        top_orders = []
        for _ in range(orders["counts"][i]):
            piid = orders["piid"][k]
            top_orders.append({
                "piid":       piid,
                "contractor": pick("contractor", orders["contractor"][k]),
                "ceiling":    orders["ceiling"][k],
                "obligated":  orders["obligated"][k],
                "pop_end":    pick("date", orders["pop_end"][k]),
                "status":     pick("status", orders["status"][k]),
                "link":       order_link(piid, parent_piid,
                                         pick("agency", orders["agency"][k])),
            })
            k += 1
        row = {
            "family_id":  family_id_from(pick("fam_key", cols["fam_key"][i]), method),
            "top_orders": top_orders,
        }
        for field, pool in V_DICT.items():
            if field != "fam_key":
                row[field] = pick(pool, cols[field][i])
        for field, pool in V_LIST.items():
            row[field] = [d[pool][j] for j in cols[field][i]]
        for field in V_NUM + V_RAW:
            row[field] = cols[field][i]
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# families
# ---------------------------------------------------------------------------

def encode_families(rows):
    pools = _pools("status", "method", "sub_agency", "office", "date",
                   "description", "solicitation", "flag", "contractor", "naics")
    cols = {}

    keys = [fam_key(r.get("family_id"), r.get("family_method")) for r in rows]
    for field, pool in F_DICT.items():
        cols[field] = [pools[pool].add(r.get(field)) for r in rows]
    for field, pool in F_LIST.items():
        cols[field] = [
            [pools[pool].add(v) for v in (r.get(field) or [])] for r in rows
        ]
    for field in F_NUM:
        cols[field] = [r.get(field) for r in rows]
    cols["fam_key"] = keys
    cols["member_piids"] = [r.get("member_piids") or [] for r in rows]

    return {
        "v": PAYLOAD_VERSION,
        "n": len(rows),
        "dicts": {name: p.values for name, p in pools.items()},
        "cols": cols,
    }


def decode_families(payload):
    """Python mirror of decodeFamilies() in web/index.html."""
    if payload.get("v") != PAYLOAD_VERSION:
        raise ValueError(f"unexpected families payload version {payload.get('v')!r}")
    d, cols, n = payload["dicts"], payload["cols"], payload["n"]

    def pick(pool, j):
        return d[pool][j] if j >= 0 else None

    rows = []
    for i in range(n):
        method = pick("method", cols["family_method"][i])
        row = {"family_id": family_id_from(cols["fam_key"][i], method)}
        for field, pool in F_DICT.items():
            row[field] = pick(pool, cols[field][i])
        for field, pool in F_LIST.items():
            row[field] = [d[pool][j] for j in cols[field][i]]
        for field in F_NUM:
            row[field] = cols[field][i]
        row["member_piids"] = cols["member_piids"][i]
        rows.append(row)
    return rows
