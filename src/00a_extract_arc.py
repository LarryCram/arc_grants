"""
00a_extract_arc.py -- the only ARC loader, and the only place an ARC name is parsed.

PURPOSE:
    Read every grant in the raw ARC CSV (a JSON blob per row in 'single_grant'), clean the
    investigator names, and write only the in-scope grants and records. The source is not modified.

INPUT:
    DATA_ROOT/raw/raw_json.csv
    data_persisted/arc_name_overrides.csv   -- hand-kept name decisions (see NameOverrides):
                                               correct / no / add rows, keyed on grant or ORCID plus
                                               ARC's raw first and family names. A row that matches
                                               nothing stops the run.

OUTPUT (in-scope only: KEEP_SCHEMES grant, HEP administering organisation, KEEP_ROLES final role):
    DATA_ROOT/processed/grants_flat.parquet       -- flattened grants (+ primary_for_name)
    DATA_ROOT/processed/investigators_raw.parquet -- one row per investigator id
    DATA_ROOT/processed/arc_names.parquet         -- NameParser() output per id: the ONLY parsed ARC
                                                     names. full_name_keys = keys of every form joined
                                                     into the id + keys_via_orcid
    DATA_ROOT/processed/arc_name_renames.parquet  -- announcement ids joined into a current id, with
                                                     the rule that joined them
    DATA_ROOT/processed/arc_name_changes.csv      -- every join made and every candidate not made,
                                                     same grant and ORCID, with apply yes/no, reason,
                                                     rule and kind of difference -- the list to review
    OUTPUT_ROOT/profiles/grant_profile.txt        -- human-readable summary

NAME CLEANING (2026-09-30):
    1. 'correct' overrides rewrite a raw name before parsing (e.g. "AW Snyder" -> "A W Snyder").
    2. On one grant, an announcement name missing from the current list joins a current name
       missing from the announcement list: hand 'add', then the same ARC ORCID, then overlapping
       full_name_keys, then same first or family name -- each one-to-one, never guessed; a 'no'
       override blocks a pair.
       See extract_investigators().
    3. Records under one ARC ORCID share their full_name_keys (ids unchanged), unless a 'no'
       override under that ORCID blocks the pair. See orcid_name_links().

OTHER DECISIONS ENCODED HERE:
    - title/role/is_fellowship/first and family name prefer the current list; ORCID prefers the
      announcement list, backfilled from current
    - ORCIDs trimmed of whitespace on extraction
    - Both administering-organisation and announcement-administering-organisation retained
"""

import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from itertools import combinations
from typing import NamedTuple
import pandas as pd
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import ARC_GRANTS_CSV, GRANT_SUMMARIES_CSV, PROFILES_OUT, PROCESSED_DATA
from src.utils.paths import ensure_dirs
from src.utils.io import setup_stdout_utf8
from config.scope import KEEP_ROLES, admin_orgs_canonical, grant_in_scope
from src.utils.names import HumanNameParser, ParsedName
from src.utils.name_differences import difference_label, keys_relation

_NAME_PARSER = HumanNameParser()
ANNOUNCEMENT, CURRENT = "announcement", "current"
NAME_OVERRIDES_CSV = Path(__file__).resolve().parents[1] / "data_persisted" / "arc_name_overrides.csv"


def safe_str(val) -> str:
    # handles explicit JSON null (None) as well as missing keys
    return (val or "").strip()


# ── Parsing ──────────────────────────────────────────────────────────────────

def parse_row(row_json: str, row_index: int) -> dict | None:
    """
    Parse a single JSON blob from the single_grant column.
    Returns None and logs if parsing fails.
    """
    try:
        obj = json.loads(row_json)
        return obj.get("data", {}).get("attributes", {})
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"  WARNING: Row {row_index} failed to parse: {e}")
        return None


# ── Hand-kept name overrides ────────────────────────────────────────────────

@dataclass
class NameOverrides:
    """data_persisted/arc_name_overrides.csv, keyed on ARC's raw names (never on an id, so parser or
    id changes can't make a row stale). Actions:
      correct -- (grant_code, first_name, family_name) is rewritten to (first_name_2, family_name_2)
                 before parsing, e.g. "AW Snyder" -> "A W Snyder"
      no      -- the two names are not to be joined: on grant_code (same-grant join) or under orcid
                 (ORCID key sharing)
      add     -- the two names on grant_code are one person, joined although no rule finds them
    `used` collects every row that matched something; a row that matches nothing is an error."""
    corrections: dict = field(default_factory=dict)  # (grant, first, family) -> (first, family)
    no_grant: dict = field(default_factory=dict)     # (grant, frozenset{form, form}) -> note
    no_orcid: dict = field(default_factory=dict)     # (orcid, frozenset{form, form}) -> note
    add_grant: dict = field(default_factory=dict)    # (grant, frozenset{form, form}) -> note
    used: set = field(default_factory=set)

    def lookup(self, table: str, scope: str, forms_a, forms_b) -> str | None:
        """Note of the first `table` row joining any form in forms_a to any form in forms_b."""
        rows = getattr(self, table)
        for fa in sorted(forms_a):
            for fb in sorted(forms_b):
                k = (scope, frozenset((fa, fb)))
                if k in rows:
                    self.used.add((table, k))
                    return rows[k]
        return None

    def unused(self) -> list[str]:
        out = [f"correct {g} {f!r} {l!r}" for (g, f, l) in self.corrections
               if ("corrections", (g, f, l)) not in self.used]
        for table in ("no_grant", "no_orcid", "add_grant"):
            out += [f"{table} {k[0]} {sorted(k[1])}" for k in getattr(self, table)
                    if (table, k) not in self.used]
        return out


def load_name_overrides(path: Path = NAME_OVERRIDES_CSV) -> NameOverrides:
    ov = NameOverrides()
    if not path.exists():
        return ov
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    for i, r in df.iterrows():
        where = f"{path.name} row {i + 2}"
        form_a = (r["first_name"], r["family_name"])
        form_b = (r["first_name_2"], r["family_name_2"])
        grant, orcid = r["grant_code"].strip(), r["orcid"].strip()
        if r["action"] == "correct":
            if not grant or orcid:
                raise ValueError(f"{where}: 'correct' needs grant_code and no orcid")
            ov.corrections[(grant, *form_a)] = form_b
        elif r["action"] in ("no", "add"):
            if form_a == form_b:
                raise ValueError(f"{where}: the two names are identical")
            pair = frozenset((form_a, form_b))
            if r["action"] == "add":
                if not grant or orcid:
                    raise ValueError(f"{where}: 'add' needs grant_code and no orcid")
                ov.add_grant[(grant, pair)] = r["notes"]
            elif bool(grant) == bool(orcid):
                raise ValueError(f"{where}: 'no' needs exactly one of grant_code / orcid")
            elif grant:
                ov.no_grant[(grant, pair)] = r["notes"]
            else:
                ov.no_orcid[(orcid, pair)] = r["notes"]
        else:
            raise ValueError(f"{where}: unknown action {r['action']!r}")
    return ov


# ── Investigators: parse, then join announcement and current forms ──────────

class _Entry(NamedTuple):
    """One investigator entry in one of a grant's two lists. raw_* are ARC's own strings;
    first/family are what was parsed (after any 'correct' override)."""
    inv: dict
    source: str
    raw_first: str
    raw_family: str
    first: str
    family: str
    parsed: ParsedName


class GrantNames(NamedTuple):
    investigators: list[dict]           # one row per unique_id
    names: list[dict]                   # arc_names rows
    renames: list[dict]                 # announcement id merged into a current id
    pairs: list[dict]                   # same-grant candidate pairs, applied or not
    entries: dict[str, list[_Entry]]    # unique_id -> its entries (for the ORCID step)


def _parse_investigator_list(inv_list: list, grant_code: str, source: str,
                             overrides: NameOverrides) -> dict[str, list[_Entry]]:
    """unique_id -> entries for one list, in list order. Every name is parsed here, once, by
    NameParser(), after any 'correct' override; the id's name part is the parser's full_name_key
    (full_name_key_raw when the ASCII key is empty, per ParsedName's own contract)."""
    out: dict[str, list[_Entry]] = {}
    for inv in inv_list:
        raw_first = safe_str(inv.get("firstName"))
        raw_family = safe_str(inv.get("familyName"))
        k = (grant_code, raw_first, raw_family)
        first, family = overrides.corrections.get(k, (raw_first, raw_family))
        if k in overrides.corrections:
            overrides.used.add(("corrections", k))
        parsed = _NAME_PARSER.parse((first, family))
        # A record with no given name has no full_name_key; its id is the family name alone.
        key = parsed.full_name_key or parsed.full_name_key_raw or parsed.family_name_main
        if key is None:
            raise ValueError(f"{grant_code}: NameParser returned no key for {first!r} {family!r}")
        out.setdefault(f"{grant_code}_{key}", []).append(
            _Entry(inv, source, raw_first, raw_family, first, family, parsed))
    return out


def _display_name(first: str, family: str) -> str:
    return f"{first} {family}".strip()


def _forms(entries: list[_Entry]) -> set[tuple[str, str]]:
    return {(e.raw_first, e.raw_family) for e in entries}


def _keys(entries: list[_Entry]) -> set[str]:
    return {k for e in entries for k in e.parsed.full_name_keys}


def _same_name(a_entries, c_entries) -> bool:
    """Overlapping full_name_keys; or, when one side has no given name recorded at all (ARC
    DP0210314: blank at announcement, "P Yeadon" current), the same family name."""
    if _keys(a_entries) & _keys(c_entries):
        return True
    no_given = (not any(e.parsed.given_tokens for e in a_entries)
                or not any(e.parsed.given_tokens for e in c_entries))
    families = lambda es: {f for e in es for f in e.parsed.family_names}
    return no_given and bool(families(a_entries) & families(c_entries))


def _same_orcid(a_entries, c_entries) -> bool:
    """Both sides carry the same ARC ORCID."""
    orcids = lambda es: {(e.inv.get("orcidIdentifier") or "").strip() for e in es} - {""}
    return bool(orcids(a_entries) & orcids(c_entries))


def _shares_first_or_last(a_entries, c_entries) -> bool:
    """Same first name (first_name_canonical) or same family name (family_name_main)."""
    for a in a_entries:
        for c in c_entries:
            pa, pc = a.parsed, c.parsed
            if pa.first_name_canonical and pa.first_name_canonical == pc.first_name_canonical:
                return True
            if pa.family_name_main and pa.family_name_main == pc.family_name_main:
                return True
    return False


def _one_to_one(edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Edges whose announcement end and current end each have exactly one edge."""
    deg_a = Counter(a for a, _ in edges)
    deg_c = Counter(c for _, c in edges)
    return {(a, c) for a, c in edges if deg_a[a] == 1 and deg_c[c] == 1}


def extract_investigators(attrs: dict, grant_code: str,
                          overrides: NameOverrides | None = None) -> GrantNames:
    """
    Extract investigators from a grant's attributes dict -- the only place an ARC name is parsed.

    Unions investigators-at-announcement and investigators-current. Every name is parsed once by
    NameParser() (after any 'correct' override); the id is grant_code + "_" + the parser's
    full_name_key. Whole lists are compared, whatever the roles (filtering each list by role first
    turned a role swap between two people into a false rename -- LP100100367, Taylor/Gray). An
    announcement name that is not in the current list joins a current name that is not in the
    announcement list, into the current id, by the first of these that applies:
      1. hand_add      -- an 'add' row in arc_name_overrides.csv
      2. same_orcid    -- both carry the same ARC ORCID (DP0772887 "Shu Ng" / "Shu-Kay Angus Ng";
                          DP0209969 "Kotagiri Ramamohanarao" / "Ramamohanarao Kotagiri"); one-to-one
                          only (2026-09-30)
      3. automatic     -- their full_name_keys overlap (DE120101452 "Mahmuda Akhtar" / "M. Shumi
                          Akhtar" share m_akhtar), or one side has no given name and the family
                          names agree (DP0210314 blank / "P Yeadon"); one-to-one only
      4. first_or_last -- same first name or same family name (Karen Ford / Karen Marsh, Tom Davis /
                          Thomas Davis); one-to-one only (2026-09-30)
    A 'no' row in arc_name_overrides.csv blocks 2-4 for that pair. Anything left is an addition
    or deletion. (2026-09-29: replaces a "one dropped + one added = rename" rule that ignored the
    names and merged different people, e.g. "Jessica Hyles" / "Ben Trevaskis".)

    title/role_code/role_name/is_fellowship/first_name/family_name prefer the CURRENT record (ARC
    corrects these over time -- FT100100761/FT100100627, "CI" at announcement, "FT" current),
    falling back to the announcement record only when the id is not in the current list.
    is_fellowship is derived from role_code in ARC's data (0 cases where role_code matches and
    is_fellowship differs). ORCID keeps announcement-first precedence, backfilled from current (1
    conflict in 78,571 matched pairs).

    Returns GrantNames: investigator rows; arc_names rows (parser fields for the display form,
    full_name_keys unioned over every form merged into the id, name_forms, corrected_from,
    in_announcement / in_current, renamed_from, rename_rule); rename rows; candidate pair rows
    (every join made, plus every first_or_last candidate not made, with the reason); and each id's
    entries for the ORCID step in main().
    """
    ov = overrides if overrides is not None else NameOverrides()
    ann = _parse_investigator_list(attrs.get("investigators-at-announcement") or [], grant_code, ANNOUNCEMENT, ov)
    curr = _parse_investigator_list(attrs.get("investigators-current") or [], grant_code, CURRENT, ov)

    def blocked(a, c):
        return ov.lookup("no_grant", grant_code, _forms(ann[a]), _forms(curr[c]))

    renamed: dict[str, str] = {}
    rule: dict[str, str] = {}
    pairs: dict[tuple[str, str], dict] = {}

    def pair_row(a, c, how, apply, reason=""):
        ea, ec = ann[a][0], curr[c][0]
        return {
            "apply": apply, "reason": reason, "rule": how, "evidence": "same grant",
            "grant_code": grant_code, "orcid": None,
            "source_first": ea.raw_first, "source_family": ea.raw_family,
            "target_first": ec.raw_first, "target_family": ec.raw_family,
            "kind": difference_label(ea.parsed, ec.parsed),
            "keys_relation": keys_relation(ea.parsed, ec.parsed),
            "source_unique_id": a, "target_unique_id": c,
            "source_role": safe_str(ea.inv.get("roleCode")), "target_role": safe_str(ec.inv.get("roleCode")),
        }

    def rest():
        done_c = set(renamed.values())
        return ([a for a in sorted(set(ann) - set(curr)) if a not in renamed],
                [c for c in sorted(set(curr) - set(ann)) if c not in done_c])

    # 1. hand_add
    rest_a, rest_c = rest()
    for a in rest_a:
        for c in rest_c:
            note = ov.lookup("add_grant", grant_code, _forms(ann[a]), _forms(curr[c]))
            if note is None:
                continue
            if a in renamed or c in renamed.values():
                raise ValueError(f"{grant_code}: 'add' rows join {a} or {c} twice")
            renamed[a], rule[a] = c, "hand_add"
            pairs[(a, c)] = pair_row(a, c, "hand_add", "yes", note)

    # 2. same_orcid, 3. automatic, 4. first_or_last -- each one-to-one among what is still unjoined
    for how, test in (("same_orcid", _same_orcid), ("automatic", _same_name),
                      ("first_or_last", _shares_first_or_last)):
        rest_a, rest_c = rest()
        edges, notes = set(), {}
        for a in rest_a:
            for c in rest_c:
                if not test(ann[a], curr[c]):
                    continue
                note = blocked(a, c)
                if note is not None:
                    pairs.setdefault((a, c), pair_row(a, c, how, "no", note))
                else:
                    edges.add((a, c))
        joined = _one_to_one(edges)
        for a, c in joined:
            renamed[a], rule[a] = c, how
            pairs[(a, c)] = pair_row(a, c, how, "yes")
        if how in ("same_orcid", "first_or_last"):
            for a, c in edges - joined:
                pairs[(a, c)] = pair_row(a, c, how, "no", "not one-to-one")

    seen: dict[str, dict] = {}
    forms: dict[str, dict[str, list[_Entry]]] = {}

    def _process(by_id, source):
        for raw_id, entries in by_id.items():
            unique_id = renamed.get(raw_id, raw_id)
            forms.setdefault(unique_id, {ANNOUNCEMENT: [], CURRENT: []})[source].extend(entries)
            e = entries[0]
            inv = e.inv
            orcid_clean = (inv.get("orcidIdentifier") or "").strip() or None
            fields = {
                "title":         safe_str(inv.get("title")),
                "first_name":    e.first,
                "family_name":   e.family,
                "role_code":     safe_str(inv.get("roleCode")),
                "role_name":     safe_str(inv.get("roleName")),
                "is_fellowship": inv.get("isFellowship", False),
            }
            if unique_id not in seen:
                seen[unique_id] = {"unique_id": unique_id, "grant_code": grant_code, **fields,
                                   "orcid": orcid_clean, "inv_source": source}
                continue
            # Present in both lists (or joined): ORCID keeps announcement-first precedence,
            # backfilled from current; everything else prefers current.
            row = seen[unique_id]
            if orcid_clean and not row["orcid"]:
                row["orcid"] = orcid_clean
            if source == CURRENT:
                row.update(fields)
                row["inv_source"] = source

    _process(ann, ANNOUNCEMENT)
    _process(curr, CURRENT)

    announcement_id_for = {c: a for a, c in renamed.items()}
    name_rows, entries_by_id = [], {}
    for unique_id, by_source in forms.items():
        all_entries = by_source[CURRENT] + by_source[ANNOUNCEMENT]
        entries_by_id[unique_id] = all_entries
        display = all_entries[0].parsed
        name_row = {k: (list(v) if isinstance(v, tuple) else v) for k, v in asdict(display).items()}
        name_row["full_name_keys"] = list(dict.fromkeys(k for e in all_entries for k in e.parsed.full_name_keys))
        a = announcement_id_for.get(unique_id)
        name_rows.append({
            "unique_id": unique_id,
            "grant_code": grant_code,
            "in_announcement": bool(by_source[ANNOUNCEMENT]),
            "in_current": bool(by_source[CURRENT]),
            "renamed_from": a,
            "rename_rule": rule.get(a) if a else None,
            "name_forms": list(dict.fromkeys(_display_name(e.first, e.family) for e in all_entries)),
            "corrected_from": list(dict.fromkeys(
                _display_name(e.raw_first, e.raw_family) for e in all_entries
                if (e.raw_first, e.raw_family) != (e.first, e.family))),
            **name_row,
        })

    rename_rows = [
        {
            "grant_code": grant_code,
            "announcement_unique_id": a,
            "current_unique_id": c,
            "announcement_name": _display_name(ann[a][0].raw_first, ann[a][0].raw_family),
            "current_name": _display_name(curr[c][0].raw_first, curr[c][0].raw_family),
            "rule": rule[a],
            "shared_keys": sorted(_keys(ann[a]) & _keys(curr[c])),
        }
        for a, c in renamed.items()
    ]
    return GrantNames(list(seen.values()), name_rows, rename_rows, list(pairs.values()), entries_by_id)


# ── ORCID: name forms under one ORCID share their keys ──────────────────────

def orcid_name_links(inv: pd.DataFrame, entries: dict[str, list[_Entry]],
                     year_by_grant: dict[str, float],
                     overrides: NameOverrides) -> tuple[list[dict], dict[str, set[str]]]:
    """Records (rows of `inv`, already in scope) carrying the same ARC ORCID are one person's, so
    each record gains the full_name_keys of every other record under that ORCID -- Karen Ford /
    Karen Marsh, Tom Davis / Thomas Davis -- unless a 'no' row under that ORCID blocks the pair
    (Chien Ming Wang / Wenhui Duan: the ORCID is Duan's). Ids do not change.

    Returns (pair rows for arc_name_changes.csv: one per pair of distinct name forms under an
    ORCID, target = the form on the later grant; keys added per unique_id)."""
    rows: list[dict] = []
    added: dict[str, set[str]] = {}
    with_orcid = inv[inv.orcid.notna()]
    for orcid, uids in with_orcid.groupby("orcid").unique_id:
        uids = sorted(set(uids))
        own = {u: _keys(entries[u]) for u in uids}
        forms: dict[tuple[str, str], dict] = {}
        for u in uids:
            year = year_by_grant.get(u.split("_", 1)[0])
            for e in entries[u]:
                f = forms.setdefault((e.raw_first, e.raw_family), {"ids": set(), "parsed": e.parsed, "year": None})
                f["ids"].add(u)
                if year is not None and not pd.isna(year):
                    f["year"] = year if f["year"] is None else max(f["year"], year)
        for fa, fb in combinations(sorted(forms), 2):
            note = overrides.lookup("no_orcid", orcid, {fa}, {fb})
            ya, yb = forms[fa]["year"] or 0, forms[fb]["year"] or 0
            src, tgt = (fb, fa) if ya > yb else (fa, fb)
            rows.append({
                "apply": "no" if note is not None else "yes", "reason": note or "",
                "rule": "orcid", "evidence": "ORCID",
                "grant_code": ";".join(sorted({u.split("_", 1)[0] for f in (fa, fb) for u in forms[f]["ids"]})),
                "orcid": orcid,
                "source_first": src[0], "source_family": src[1],
                "target_first": tgt[0], "target_family": tgt[1],
                "kind": difference_label(forms[src]["parsed"], forms[tgt]["parsed"]),
                "keys_relation": keys_relation(forms[src]["parsed"], forms[tgt]["parsed"]),
                "source_unique_id": ";".join(sorted(forms[src]["ids"])),
                "target_unique_id": ";".join(sorted(forms[tgt]["ids"])),
                "source_role": None, "target_role": None,
            })
        for u in uids:
            for v in uids:
                if u == v:
                    continue
                if overrides.lookup("no_orcid", orcid, _forms(entries[u]), _forms(entries[v])) is not None:
                    continue
                extra = own[v] - own[u]
                if extra:
                    added.setdefault(u, set()).update(extra)
    return rows, added


# Removed extract_for_codes as we are now using primary_field_of_research from grant_summaries


def extract_grant_flat(attrs: dict, grant_code: str) -> dict:
    """Extract flat grant-level fields.

    eligible_roles / eligible_names (2026-08-16): ARC's own organisations-at-announcement list
    carries 7 distinct roleName values, not just the 2 originally handled here. Investigated all
    7 directly against real in-scope (KEEP_SCHEMES) grants before finalizing this set:
      - Administering Organisation (30,551, exactly 1/grant) -- include
      - Other Eligible Organisation (5,036) -- include
      - Collaborating Organisation (701) -- include (added 2026-08-16): real examples are genuine
        Australian universities (Melbourne, Monash, Macquarie, ACU, Griffith), same character as
        Other Eligible Organisation
      - Partner Organisation (15,250) -- excluded (user-directed): formally a weaker/different
        relationship to the grant than the funded team itself
      - Host Organisation (809, mostly on FT/Future Fellowship grants) -- excluded: real examples
        overwhelmingly foreign universities or industry (Cambridge, Lund, Caltech, Boeing, Dyson),
        rarely an Australian HEP; represents where a fellow is hosted, not part of the funded team
      - Other Organisation (8,126) and bare Other (889) -- excluded: real examples overwhelmingly
        foreign universities or non-HEP bodies (Auckland, Illinois, UCL, museums, private companies)
    `eligible_names` (the original 2-role set) was previously computed and then discarded -- only
    its count survived into grants_flat.parquet (n_eligible_orgs). n_eligible_orgs is load-bearing
    downstream (01_prepare_arc.py/awards_cif.py's _merge_same_grant_coinvestigators,
    04_resolve_links.py's institution-overlap check all treat n_eligible_orgs==1 as "single-org
    grant" and rely on that exact 2-role definition) -- so it keeps its original 2-role scope
    unchanged here. The new eligible_orgs column below is a *separate*, deliberately wider 3-role
    set (adds Collaborating Organisation) for HEP-code resolution -- see
    src/utils/awards_cif.py's HEP-code aggregation, which consumes it. Do not fold eligible_orgs's
    role set back into n_eligible_orgs's -- that would silently change which grants count as
    "single-org" for the merge/disambiguation logic above.
    """
    orgs = attrs.get("organisations-at-announcement", []) or []
    n_eligible_roles = {"Administering Organisation", "Other Eligible Organisation"}
    n_eligible_names = {o["organisationName"] for o in orgs
                        if o.get("roleName") in n_eligible_roles and o.get("organisationName")}
    eligible_orgs_roles = n_eligible_roles | {"Collaborating Organisation"}
    eligible_orgs_names = {o["organisationName"] for o in orgs
                           if o.get("roleName") in eligible_orgs_roles and o.get("organisationName")}
    return {
        "grant_code":           grant_code,
        "scheme_name":          safe_str(attrs.get("scheme-name")),
        "grant_status":         safe_str(attrs.get("grant-status")),
        "funding_commence_year":attrs.get("funding-commencement-year"),
        "years_funded":         attrs.get("years-funded"),
        "funding_announced":    attrs.get("funding-at-announcement"),
        "funding_current":      attrs.get("funding-current"),
        "admin_org":            safe_str(attrs.get("administering-organisation") or
                                         attrs.get("announcement-administering-organisation")),
        # 2026-08-25: the module docstring above has claimed "both retained" since before this
        # field existed -- admin_org itself only ever kept ONE value (current, falling back to
        # announcement only when current is missing), silently discarding the announcement-time
        # value whenever both are present and differ. Confirmed real at scale: 13.03% of grants
        # the pipeline treats as "single institution" via n_eligible_orgs==1 actually have a
        # DIFFERENT admin org between snapshots (e.g. DP110100989: Wollongong at announcement,
        # Australian Catholic University current, same investigators throughout). Persisted as
        # its own explicit field, not folded anonymously into eligible_orgs below -- keeping the
        # announcement-vs-current distinction visible is itself valuable evidence (the specific
        # transfer story), not just set membership.
        "announcement_admin_org": safe_str(attrs.get("announcement-administering-organisation")),
        "grant_summary":        safe_str(attrs.get("grant-summary")),
        "n_eligible_orgs":      len(n_eligible_names),
        "eligible_orgs":        sorted(eligible_orgs_names),
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()
    ensure_dirs()

    print(f"Reading: {ARC_GRANTS_CSV}")
    df_raw = pd.read_csv(ARC_GRANTS_CSV, dtype=str)
    print(f"  Rows in CSV: {len(df_raw)}")

    # Normalise column names to lowercase stripped
    df_raw.columns = [c.strip().lower() for c in df_raw.columns]

    if "single_grant" not in df_raw.columns:
        print(f"ERROR: 'single_grant' column not found. Columns: {list(df_raw.columns)}")
        sys.exit(1)

    overrides = load_name_overrides()
    print(f"Name overrides: {NAME_OVERRIDES_CSV} -- {len(overrides.corrections)} correct, "
          f"{len(overrides.no_grant) + len(overrides.no_orcid)} no, {len(overrides.add_grant)} add")

    # ── Parse all rows (every grant and role is read; only in-scope records are written) ──
    grants_flat     = []
    investigators   = []
    arc_names       = []
    renames         = []
    pairs           = []
    entries         = {}
    parse_failures  = []

    for idx, row in df_raw.iterrows():
        attrs = parse_row(row["single_grant"], idx)
        if attrs is None:
            parse_failures.append(idx)
            continue

        grant_code = attrs.get("code", f"UNKNOWN_{idx}")

        grants_flat.append(extract_grant_flat(attrs, grant_code))
        gn = extract_investigators(attrs, grant_code, overrides)
        investigators.extend(gn.investigators)
        arc_names.extend(gn.names)
        renames.extend(gn.renames)
        pairs.extend(gn.pairs)
        entries.update(gn.entries)

    df_grants_all = pd.DataFrame(grants_flat)
    df_inv_all    = pd.DataFrame(investigators)

    # ── Scope: KEEP_SCHEMES grant, HEP administering organisation, KEEP_ROLES final role ──
    hep_admin_orgs, _, _ = admin_orgs_canonical()
    grant_ok = {g for g, a in zip(df_grants_all.grant_code, df_grants_all.admin_org)
                if grant_in_scope(g, a, hep_admin_orgs)}
    df_grants = df_grants_all[df_grants_all.grant_code.isin(grant_ok)].reset_index(drop=True)
    df_inv = df_inv_all[df_inv_all.grant_code.isin(grant_ok)
                        & df_inv_all.role_code.isin(KEEP_ROLES)].reset_index(drop=True)
    kept_ids = set(df_inv.unique_id)
    df_names = pd.DataFrame(arc_names)
    df_names = df_names[df_names.unique_id.isin(kept_ids)].reset_index(drop=True)
    df_renames = pd.DataFrame(renames)
    df_renames = df_renames[df_renames.current_unique_id.isin(kept_ids)].reset_index(drop=True)
    df_pairs = pd.DataFrame(pairs)
    df_pairs = df_pairs[df_pairs.grant_code.isin(grant_ok)
                        & (df_pairs.source_role.isin(KEEP_ROLES) | df_pairs.target_role.isin(KEEP_ROLES))]

    # ── ORCID: records under one ARC ORCID share their full_name_keys ──
    year_by_grant = dict(zip(df_grants.grant_code, pd.to_numeric(df_grants.funding_commence_year, errors="coerce")))
    orcid_rows, keys_via_orcid = orcid_name_links(df_inv, entries, year_by_grant, overrides)
    df_names["keys_via_orcid"] = [sorted(keys_via_orcid.get(u, ())) for u in df_names.unique_id]
    df_names["full_name_keys"] = [list(k) + v for k, v in zip(df_names.full_name_keys, df_names.keys_via_orcid)]

    unused = overrides.unused()
    if unused:
        raise SystemExit("arc_name_overrides.csv rows that matched nothing (fix or remove them):\n  "
                         + "\n  ".join(unused))

    df_changes = pd.concat([df_pairs, pd.DataFrame(orcid_rows)], ignore_index=True)
    df_changes = df_changes.sort_values(["apply", "evidence", "grant_code", "source_family"],
                                        key=lambda s: s.fillna("")).reset_index(drop=True)

    # ── Enrich grants with primary_field_of_research from summaries ──────────
    print(f"\nEnriching grants with {GRANT_SUMMARIES_CSV}")
    summaries = pd.read_csv(GRANT_SUMMARIES_CSV, usecols=['grant_id', 'primary_field_of_research'])
    summaries = summaries.rename(columns={'grant_id': 'grant_code'})
    
    # Strip the leading 4-digit code and hyphen (e.g., '4605 - Data Management' -> 'Data Management')
    summaries['primary_for_name'] = summaries['primary_field_of_research'].str.replace(r'^[0-9]+\s*-\s*', '', regex=True)
    
    # Merge onto df_grants
    df_grants = df_grants.merge(summaries[['grant_code', 'primary_for_name']], on='grant_code', how='left')

    # ── Save Parquet outputs ─────────────────────────────────────────────────
    grants_path  = PROCESSED_DATA / "grants_flat.parquet"
    inv_path     = PROCESSED_DATA / "investigators_raw.parquet"
    names_path   = PROCESSED_DATA / "arc_names.parquet"
    renames_path = PROCESSED_DATA / "arc_name_renames.parquet"
    changes_path = PROCESSED_DATA / "arc_name_changes.csv"

    df_grants.to_parquet(grants_path, index=False)
    df_inv.to_parquet(inv_path, index=False)
    df_names.to_parquet(names_path, index=False)
    df_renames.to_parquet(renames_path, index=False)
    df_changes.to_csv(changes_path, index=False)

    print(f"\n  Saved: {grants_path}")
    print(f"  Saved: {inv_path}")
    print(f"  Saved: {names_path}")
    print(f"  Saved: {renames_path}")
    print(f"  Saved: {changes_path}")

    # ── Profile ──────────────────────────────────────────────────────────────
    profile_lines = []
    p = profile_lines.append  # shorthand

    p("=" * 60)
    p("ARC GRANTS DATA PROFILE")
    p("=" * 60)

    p(f"\n── Source ──────────────────────────────────────────")
    p(f"  CSV rows:              {len(df_raw):>8,}")
    p(f"  Parse failures:        {len(parse_failures):>8,}")
    p(f"  Grants parsed:         {len(df_grants):>8,}")

    p(f"\n── Grants ──────────────────────────────────────────")
    p(f"  Year range:            {df_grants.funding_commence_year.min()} "
      f"– {df_grants.funding_commence_year.max()}")
    p(f"  Unique schemes:        {df_grants.scheme_name.nunique():>8,}")
    p(f"  Missing admin org:     {df_grants.admin_org.eq('').sum():>8,}")
    p(f"  Null funding amount:   {df_grants.funding_announced.isna().sum():>8,}")

    p(f"\n  Grant status counts:")
    for status, cnt in df_grants.grant_status.value_counts().items():
        p(f"    {status:<40} {cnt:>6,}")

    p(f"\n  Grants per year (sample):")
    year_counts = df_grants.funding_commence_year.value_counts().sort_index()
    for year, cnt in year_counts.items():
        p(f"    {year}  {cnt:>5,}")

    p(f"\n── Investigators (raw) ─────────────────────────────")
    p(f"  Total investigator rows:     {len(df_inv):>8,}")
    p(f"  Unique family names:         {df_inv.family_name.nunique():>8,}")
    p(f"  Unique first names:          {df_inv.first_name.nunique():>8,}")
    p(f"  Unique name combinations:    "
      f"{df_inv[['first_name','family_name']].drop_duplicates().shape[0]:>8,}")

    p(f"\n  Role code distribution:")
    for role, cnt in df_inv.role_code.value_counts().items():
        p(f"    {role:<10} {cnt:>8,}")

    p(f"\n  ORCID coverage:")
    p(f"    Has ORCID:             {df_inv.orcid.notna().sum():>8,}  "
      f"({100*df_inv.orcid.notna().mean():.1f}%)")
    p(f"    No ORCID:              {df_inv.orcid.isna().sum():>8,}  "
      f"({100*df_inv.orcid.isna().mean():.1f}%)")

    p(f"\n  Investigators sourced from 'current' (not announcement):")
    p(f"    {df_inv[df_inv.inv_source=='current'].grant_code.nunique():>8,} grants")

    p(f"\n  Grants per investigator (by family+first name):")
    grants_per_inv = df_inv.groupby(
        ["family_name", "first_name"])["grant_code"].nunique()
    p(f"    1 grant:               "
      f"{(grants_per_inv == 1).sum():>8,}")
    p(f"    2–5 grants:            "
      f"{((grants_per_inv >= 2) & (grants_per_inv <= 5)).sum():>8,}")
    p(f"    6–10 grants:           "
      f"{((grants_per_inv >= 6) & (grants_per_inv <= 10)).sum():>8,}")
    p(f"    >10 grants:            "
      f"{(grants_per_inv > 10).sum():>8,}")
    p(f"    Max grants one person: {grants_per_inv.max():>8,}")

    # FOR section removed from profile since we rely on the primary_for_name now.

    p(f"\n── Scope (only in-scope records are written) ───────")
    p(f"  Grants read / in scope:      {len(df_grants_all):>8,} / {len(df_grants):,}")
    p(f"  Records read / in scope:     {len(df_inv_all):>8,} / {len(df_inv):,}")

    p(f"\n── Announcement vs current (parsed names, arc_names.parquet) ──")
    both = df_names.in_announcement & df_names.in_current
    p(f"  In both lists:               {both.sum():>8,}  (includes {len(df_renames):,} joined forms)")
    p(f"  Only at announcement:        {(df_names.in_announcement & ~df_names.in_current).sum():>8,}  (deletions)")
    p(f"  Only in current:             {(~df_names.in_announcement & df_names.in_current).sum():>8,}  (additions)")
    for how, n in df_renames["rule"].value_counts().items():
        p(f"    joined by {how:<16} {n:>8,}")
    not_joined = df_pairs[df_pairs["apply"] == "no"]
    for why, n in not_joined.reason.map(lambda r: "not one-to-one" if r == "not one-to-one" else "override 'no'").value_counts().items():
        p(f"    candidate not joined ({why}): {n:,}")
    p(f"\n── ORCID (records under one ORCID share keys) ──────")
    orc = pd.DataFrame(orcid_rows)
    p(f"  ORCIDs with 2+ name forms:   {orc.orcid.nunique() if len(orc) else 0:>8,}")
    p(f"  Name-form pairs:             {len(orc):>8,}  ({(orc['apply'] == 'no').sum() if len(orc) else 0} blocked by override 'no')")
    p(f"  Records gaining keys:        {len(keys_via_orcid):>8,}")
    p(f"\n  Name overrides: {len(overrides.corrections)} correct, "
      f"{len(overrides.no_grant) + len(overrides.no_orcid)} no, {len(overrides.add_grant)} add -- all matched")
    p(f"  Full list of joins and candidates: {changes_path}")

    p(f"\n── Data Quality Flags ──────────────────────────────")
    # Names with only initials
    initial_only = df_inv[df_inv.first_name.str.match(r'^[A-Z]\.?$', na=False)]
    p(f"  Initial-only first names:    {len(initial_only):>8,}")

    # Empty names
    p(f"  Empty family names:          "
      f"{df_inv.family_name.eq('').sum():>8,}")
    p(f"  Empty first names:           "
      f"{df_inv.first_name.eq('').sum():>8,}")

    # Malformed ORCIDs (should be 19 chars: 0000-0000-0000-0000)
    has_orcid = df_inv[df_inv.orcid.notna()]
    bad_orcid = has_orcid[~has_orcid.orcid.str.match(
        r'^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$', na=False)]
    p(f"  Malformed ORCIDs:            {len(bad_orcid):>8,}")
    if len(bad_orcid) > 0:
        p(f"  Sample malformed:")
        for val in bad_orcid.orcid.head(5):
            p(f"    '{val}'")

    # Encoding issues in grant summaries
    mojibake = df_grants[df_grants.grant_summary.str.contains(
        'â€', na=False, regex=False)]
    p(f"  Grants with encoding issues: {len(mojibake):>8,}")

    if parse_failures:
        p(f"\n  Parse failure row indices: {parse_failures}")

    p("\n" + "=" * 60)

    # ── Write and print profile ──────────────────────────────────────────────
    profile_text = "\n".join(profile_lines)
    profile_path = PROFILES_OUT / "grant_profile.txt"
    profile_path.write_text(profile_text, encoding="utf-8")

    print("\n" + profile_text)
    print(f"\nProfile saved to: {profile_path}")


if __name__ == "__main__":
    main()