"""
Statistics of ARC investigator names, read exactly as ARC provides them (raw_json.csv), in-scope
only (KEEP_SCHEMES grant code, HEP administering organisation, KEEP_ROLES on the entry's own role).

Diagnostic only: nothing here feeds ids, keys or matching. Every name is parsed with NameParser()
(HumanNameParser); the comparisons below classify the difference between two parsed names for
reporting, they never produce a name form used anywhere else.

A. potential anomalies inside one name record (whitespace, punctuation, case, content in the wrong
   field, non-ASCII) -- flags, not established errors.
B. name differences ARC's own data exposes, each pair labelled "<family relation>|<given relation>":
   B1  announcement vs current list on the same grant
   B2  one ARC ORCID carrying more than one name form
   B3  a rare name form close to a common one with the same given (or family) name
"""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from itertools import combinations

import pandas as pd

from config.scope import KEEP_ROLES, KEEP_SCHEMES
from config.settings import ARC_GRANTS_CSV
from src.acif.build import _admin_orgs_canonical
from src.utils.names import HumanNameParser, ParsedName

ANNOUNCEMENT, CURRENT = "announcement", "current"
RARE_MAX, COMMON_MIN = 2, 5  # B3: a form on <= 2 grants vs one on >= 5 grants

_PARTICLE_RUN_IN = re.compile(r"^(?:[Dd]e|[Dd]el|[Dd]ella|[Dd]er|[Dd]en|[Dd]i|[Dd]a|[Dd]u|[Ll]a|[Ll]e|"
                              r"[Ss]t|[Tt]en|[Tt]er|[Vv]an|[Vv]on)(?=[A-Z])")
_MAC = re.compile(r"^Ma?c(?=[A-Z])")
_OTHER_SYMBOL = re.compile(r"[^\w\s\-'’.,()\"]", re.UNICODE)
_NEE_WORD = re.compile(r"\bn[ée]e\b", re.IGNORECASE)
_COMPACT = re.compile(r"[\s\-'’_.]")
_TOKEN_SPLIT = re.compile(r"[\s\-]+")


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _compact(s: str) -> str:
    return _COMPACT.sub("", s.lower())


# ---------------------------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------------------------

def load_in_scope_entries(csv_path=ARC_GRANTS_CSV) -> pd.DataFrame:
    """One row per investigator entry in both of ARC's lists, for in-scope grants (KEEP_SCHEMES by
    grant code, HEP administering organisation). Every entry is kept, whatever its role --
    `role_in_scope` marks KEEP_ROLES, so whole lists can still be compared per grant. Names are
    ARC's own fields, only strip()'d."""
    hep_admin_orgs, _, _ = _admin_orgs_canonical()
    rows = []
    for _, r in pd.read_csv(csv_path).iterrows():
        try:
            rec = json.loads(r["single_grant"])
        except (TypeError, ValueError):
            continue
        data = rec.get("data", {})
        attrs = data.get("attributes", {})
        grant = data.get("id") or attrs.get("code")
        admin = (attrs.get("administering-organisation")
                 or attrs.get("announcement-administering-organisation") or "").strip()
        if not grant or grant[:2] not in KEEP_SCHEMES or admin not in hep_admin_orgs:
            continue
        for source, key in ((ANNOUNCEMENT, "investigators-at-announcement"),
                            (CURRENT, "investigators-current")):
            for inv in attrs.get(key) or []:
                role = (inv.get("roleCode") or "").strip()
                rows.append({
                    "grant_code": grant,
                    "source": source,
                    "first": (inv.get("firstName") or "").strip(),
                    "family": (inv.get("familyName") or "").strip(),
                    "role": role,
                    "arc_title": (inv.get("title") or "").strip(),
                    "role_in_scope": role in KEEP_ROLES,
                    "orcid": (inv.get("orcidIdentifier") or "").strip() or None,
                })
    return pd.DataFrame(rows)


class _Parsed:
    """Parse each distinct (first, family) once with NameParser()."""

    def __init__(self):
        self.parser = HumanNameParser()
        self._cache: dict[tuple[str, str], ParsedName] = {}

    def __call__(self, first: str, family: str) -> ParsedName:
        k = (first, family)
        if k not in self._cache:
            self._cache[k] = self.parser.parse(k)
        return self._cache[k]

    def key(self, first: str, family: str) -> str:
        p = self(first, family)
        return p.full_name_key or p.full_name_key_raw or p.family_name_main or f"raw:{first}|{family}"


# ---------------------------------------------------------------------------------------------
# A. Potential anomalies inside one record
# ---------------------------------------------------------------------------------------------

def potential_anomalies(first: str, family: str, parser: HumanNameParser) -> list[str]:
    """Flags for one (first, family) record, both fields already strip()'d."""
    flags = []
    both = f"{first} {family}"
    if "  " in first or "  " in family:
        flags.append("doubled_space")
    if re.search(r"['’]\s", both):
        flags.append("space_after_apostrophe")
    if re.search(r"\s-|-\s", both):
        flags.append("space_around_hyphen")
    if "_" in both:
        flags.append("underscore")
    if re.search(r"\d", both):
        flags.append("digit")
    if "." in family:
        flags.append("period_in_family")
    if "," in family:
        flags.append("comma_in_family")
    if re.search(r"[()]", both):
        flags.append("brackets")
    if '"' in both:
        flags.append("quotes")
    if _OTHER_SYMBOL.search(both):
        flags.append("other_symbol")
    fam_letters = [c for c in family if c.isalpha()]
    if len(fam_letters) > 1 and family.isupper():
        flags.append("family_all_caps")
    if len(fam_letters) > 1 and family.islower():
        flags.append("family_all_lower")
    first_letters = [c for c in first if c.isalpha()]
    if len(first_letters) > 1 and first.isupper():
        flags.append("first_all_caps")
    if len(first_letters) > 1 and first.islower():
        flags.append("first_all_lower")
    if _PARTICLE_RUN_IN.search(family):
        flags.append("particle_run_in")
    elif re.search(r"[a-z][A-Z]", family) and not _MAC.search(family):
        flags.append("internal_capital")
    if not first:
        flags.append("empty_first")
    if not family:
        flags.append("empty_family")
    tokens = [t for t in re.split(r"[\s.\-]+", first) if t]
    if tokens and all(len(t) == 1 for t in tokens):
        flags.append("initials_only_given")
    if any(ord(c) > 127 for c in both):
        flags.append("non_ascii")
    hn = parser._structural((first, family))
    if hn is not None:
        if hn.title:
            flags.append("title_in_field")
        if hn.suffix:
            flags.append("postnominal_in_field")
        maiden = parser._maiden_name(hn)
        if maiden:
            flags.append("nee_bracketed")
        elif hn.nickname:
            flags.append("nickname_bracketed")
    if _NEE_WORD.search(both) and "nee_bracketed" not in flags:
        flags.append("nee_unbracketed")
    return flags


# ---------------------------------------------------------------------------------------------
# B. Classifying the difference between two parsed names
# ---------------------------------------------------------------------------------------------

def family_relation(pa: ParsedName, pb: ParsedName) -> str:
    A, B = set(pa.family_names), set(pb.family_names)
    if not A or not B:
        return "missing"
    if A & B:
        return "same"
    ca, cb = {_compact(x) for x in A}, {_compact(x) for x in B}
    if ca & cb:
        return "separator"
    ta = set(_TOKEN_SPLIT.split(pa.family_name_main or ""))
    tb = set(_TOKEN_SPLIT.split(pb.family_name_main or ""))
    if ta < tb or tb < ta:
        return "compound"
    d = min(levenshtein(x, y) for x in ca for y in cb)
    if d == 1:
        return "edit1"
    if d == 2:
        return "edit2"
    return "different"


def given_relation(pa: ParsedName, pb: ParsedName) -> str:
    if not pa.given_tokens or not pb.given_tokens:
        return "missing"
    ma = {t for t in pa.given_tokens if len(t) > 1}
    mb = {t for t in pb.given_tokens if len(t) > 1}
    ia = {t[0] for t in pa.given_tokens}
    ib = {t[0] for t in pb.given_tokens}
    if not ma or not mb:
        if not ma and not mb:
            return "same" if ia == ib else ("initials_overlap" if ia & ib else "different")
        return "initial_vs_full" if ia & ib else "different"
    ga, gb = pa.first_name_canonical or "", pb.first_name_canonical or ""
    if ga == gb or ma == mb:
        return "same"
    if ma < mb or mb < ma:
        return "contained"
    if ma & mb:
        return "overlap"
    fa, fb = pa.given_tokens[0], pb.given_tokens[0]  # first-name position ("M. Shumi" -> "m")
    if (len(fa) == 1) != (len(fb) == 1) and fa[0] == fb[0]:
        return "initial_vs_full"
    d = levenshtein(ga, gb)
    if d == 1:
        return "edit"
    if ga.startswith(gb) or gb.startswith(ga) or (min(len(ga), len(gb)) <= 5 and ga[:2] == gb[:2]):
        return "nickname_shaped"
    if d == 2:
        return "edit"
    return "different"


def is_swap(pa: ParsedName, pb: ParsedName) -> bool:
    return bool(pa.first_name_canonical and pb.first_name_canonical
                and pa.first_name_canonical in set(pb.family_names)
                and pb.first_name_canonical in set(pa.family_names))


def difference_label(pa: ParsedName, pb: ParsedName) -> str:
    fam = family_relation(pa, pb)
    if fam != "same" and is_swap(pa, pb):
        return "swap"
    return f"{fam}|{given_relation(pa, pb)}"


# ---------------------------------------------------------------------------------------------
# B1. Announcement vs current on one grant
# ---------------------------------------------------------------------------------------------

def compare_grant_lists(grant: pd.DataFrame, parsed: _Parsed) -> tuple[list[dict], list[dict], bool]:
    """Statuses for every entry of one grant, plus the pairs worth labelling.

    Whole lists are compared (role ignored) -- filtering each list by role first turned a role
    swap between two people into a false rename (LP100100367, Taylor/Gray). Same parsed key in
    both lists = same; keys overlapping one-to-one = rename; otherwise only_announcement /
    only_current; an empty list on one side = no_current_list / no_announcement_list."""
    ann = grant[grant.source == ANNOUNCEMENT]
    curr = grant[grant.source == CURRENT]

    def by_key(df):
        out = {}
        for r in df.itertuples():
            out.setdefault(parsed.key(r.first, r.family), r)
        return out

    a, c = by_key(ann), by_key(curr)
    statuses, pairs = [], []
    if ann.empty or curr.empty:
        status = "no_current_list" if curr.empty else "no_announcement_list"
        for r in (ann if curr.empty else curr).itertuples():
            statuses.append({"grant_code": r.grant_code, "first": r.first, "family": r.family,
                             "role_in_scope": r.role_in_scope, "status": status})
        return statuses, pairs, False

    only_a, only_c = set(a) - set(c), set(c) - set(a)
    def same_name(ra, rc):
        # overlapping keys; or one side has no given name and the family names agree (same rule
        # as 00a_extract_arc.py -- ARC DP0210314, blank at announcement, "P Yeadon" current)
        pa, pc = parsed(ra.first, ra.family), parsed(rc.first, rc.family)
        if set(pa.full_name_keys) & set(pc.full_name_keys):
            return True
        return ((not pa.given_tokens or not pc.given_tokens)
                and bool(set(pa.family_names) & set(pc.family_names)))

    overlaps = {k: {j for j in only_c if same_name(a[k], c[j])} for k in only_a}
    renamed = {}
    ambiguous = False
    for k, js in overlaps.items():
        if len(js) == 1 and sum(next(iter(js)) in o for o in overlaps.values()) == 1:
            renamed[k] = next(iter(js))
        elif js:
            ambiguous = True

    def status_row(r, status):
        return {"grant_code": r.grant_code, "first": r.first, "family": r.family,
                "role_in_scope": r.role_in_scope, "status": status}

    for k in set(a) & set(c):
        statuses.append(status_row(c[k], "same"))
    for k, j in renamed.items():
        statuses.append(status_row(c[j], "rename"))
    rest_a = [a[k] for k in only_a if k not in renamed]
    rest_c = [c[j] for j in only_c if j not in renamed.values()]
    statuses += [status_row(r, "only_announcement") for r in rest_a]
    statuses += [status_row(r, "only_current") for r in rest_c]

    def pair_row(kind, ra, rc):
        pa, pc = parsed(ra.first, ra.family), parsed(rc.first, rc.family)
        return {"source": kind, "grant_code": ra.grant_code, "orcid": None,
                "name_a": f"{ra.first} {ra.family}", "name_b": f"{rc.first} {rc.family}",
                "label": difference_label(pa, pc),
                "role_in_scope": ra.role_in_scope or rc.role_in_scope}

    for k, j in renamed.items():
        pairs.append(pair_row("B1_rename", a[k], c[j]))
    for ra in rest_a:
        for rc in rest_c:
            pairs.append(pair_row("B1_unmatched", ra, rc))
    return statuses, pairs, ambiguous


# ---------------------------------------------------------------------------------------------
# B2. One ORCID, several name forms
# ---------------------------------------------------------------------------------------------

def orcid_name_forms(entries: pd.DataFrame, parsed: _Parsed) -> list[dict]:
    e = entries[entries.role_in_scope & entries.orcid.notna()]
    pairs = []
    for orcid, g in e.groupby("orcid"):
        forms = sorted(set(zip(g["first"], g["family"])))
        for (fa, la), (fb, lb) in combinations(forms, 2):
            pairs.append({"source": "B2_same_orcid", "grant_code": None, "orcid": orcid,
                          "name_a": f"{fa} {la}", "name_b": f"{fb} {lb}",
                          "label": difference_label(parsed(fa, la), parsed(fb, lb)),
                          "role_in_scope": True})
    return pairs


# ---------------------------------------------------------------------------------------------
# B3. Rare form close to a common one
# ---------------------------------------------------------------------------------------------

def rare_near_common(entries: pd.DataFrame, parsed: _Parsed) -> list[dict]:
    """Same given name, family forms: one on <= RARE_MAX grants within 1 edit (or a separator /
    case difference) of one on >= COMMON_MIN grants. Then the same the other way round (same
    family, given names). No identity claim -- possible misspellings."""
    e = entries[entries.role_in_scope][["grant_code", "first", "family"]].drop_duplicates()
    e = e.assign(
        given=[parsed(f, l).first_name_canonical or "" for f, l in zip(e["first"], e["family"])],
        fam=[parsed(f, l).family_name_main or "" for f, l in zip(e["first"], e["family"])],
    )
    pairs = []
    for fixed, varied, which in (("given", "family", "family"), ("fam", "first", "given")):
        counts = e.groupby([fixed, varied])["grant_code"].nunique()
        for key, g in counts.groupby(level=0):
            if not key:
                continue
            forms = g.droplevel(0)
            rare = forms[forms <= RARE_MAX]
            common = forms[forms >= COMMON_MIN]
            for r_form, r_n in rare.items():
                for c_form, c_n in common.items():
                    if not r_form or not c_form:
                        continue
                    cr, cc = _compact(r_form), _compact(c_form)
                    if cr == cc:
                        kind = "separator_or_case"
                    elif levenshtein(cr, cc) == 1:
                        kind = "edit1"
                    else:
                        continue
                    pairs.append({"source": f"B3_{which}", "grant_code": None, "orcid": None,
                                  "name_a": f"{key} | {r_form} ({r_n} grants)",
                                  "name_b": f"{key} | {c_form} ({c_n} grants)",
                                  "label": kind, "role_in_scope": True})
    return pairs


# ---------------------------------------------------------------------------------------------
# Survey
# ---------------------------------------------------------------------------------------------

def run_survey(entries: pd.DataFrame | None = None) -> dict:
    entries = load_in_scope_entries() if entries is None else entries
    parsed = _Parsed()

    in_scope = entries[entries.role_in_scope]
    distinct = in_scope[["first", "family"]].drop_duplicates()
    anomaly_rows = []
    for f, l in zip(distinct["first"], distinct["family"]):
        for flag in potential_anomalies(f, l, parsed.parser):
            anomaly_rows.append({"first": f, "family": l, "flag": flag})
    anomalies = pd.DataFrame(anomaly_rows, columns=["first", "family", "flag"])
    entry_counts = in_scope.groupby(["first", "family"]).size().rename("entries").reset_index()
    anomalies = anomalies.merge(entry_counts, on=["first", "family"], how="left")

    statuses, pairs, n_ambiguous = [], [], 0
    for _, g in entries.groupby("grant_code"):
        s, p, amb = compare_grant_lists(g, parsed)
        statuses += s
        pairs += p
        n_ambiguous += amb
    statuses = pd.DataFrame(statuses)
    pairs += orcid_name_forms(entries, parsed)
    pairs += rare_near_common(entries, parsed)
    differences = pd.DataFrame(pairs)
    differences = differences[differences.role_in_scope].drop(columns="role_in_scope")

    return {
        "entries": entries,
        "anomalies": anomalies,
        "statuses": statuses,
        "differences": differences,
        "n_ambiguous_grants": n_ambiguous,
        "n_distinct_names": len(distinct),
    }


def _md_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "(none)\n"
    cols = list(df.columns)
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for row in df.itertuples(index=False):
        lines.append("| " + " | ".join(str(v).replace("|", "/") for v in row) + " |")
    return "\n".join(lines) + "\n"


def render_report(res: dict, n_examples: int = 6) -> str:
    entries, anomalies, statuses, diffs = res["entries"], res["anomalies"], res["statuses"], res["differences"]
    in_scope = entries[entries.role_in_scope]
    out = ["# ARC name survey", "",
           "Names exactly as ARC provides them (raw_json.csv), in-scope grants and roles. "
           "Diagnostic only.", "",
           "## Population", "",
           f"- in-scope grants: {entries.grant_code.nunique():,}",
           f"- investigator entries on them (both lists, all roles): {len(entries):,}",
           f"- entries with an in-scope role: {len(in_scope):,} "
           f"(announcement {int((in_scope.source == ANNOUNCEMENT).sum()):,}, "
           f"current {int((in_scope.source == CURRENT).sum()):,})",
           f"- distinct (first, family) name records: {res['n_distinct_names']:,}", "",
           "## A. Potential anomalies inside one name record", ""]
    if not anomalies.empty:
        a = (anomalies.groupby("flag")
             .agg(distinct_names=("first", "size"), entries=("entries", "sum"))
             .sort_values("distinct_names", ascending=False).reset_index())
        a["% of distinct names"] = (100 * a.distinct_names / res["n_distinct_names"]).round(2)
        a["examples"] = [
            "; ".join(f"{f!r} {l!r}" for f, l in
                      anomalies[anomalies.flag == fl][["first", "family"]].head(n_examples).itertuples(index=False))
            for fl in a.flag]
        out.append(_md_table(a))
    out += ["## B1. Announcement vs current, same grant", "",
            "Status of each in-scope-role entry:", ""]
    st = statuses[statuses.role_in_scope].status.value_counts().rename_axis("status").reset_index(name="entries")
    out.append(_md_table(st))
    out += [f"Grants where an overlap was not one-to-one (left unmerged): {res['n_ambiguous_grants']:,}", ""]
    for source, title in (("B1_rename", "Renames (keys overlap)"),
                          ("B1_unmatched", "Unmatched announcement x current pairs on the same grant"),
                          ("B2_same_orcid", "B2. One ORCID, more than one name form"),
                          ("B3_family", "B3. Rare family form near a common one (same given name)"),
                          ("B3_given", "B3. Rare given form near a common one (same family name)")):
        d = diffs[diffs.source == source]
        out += [f"### {title}", "", f"pairs: {len(d):,}"
                + (f"; ORCIDs: {d.orcid.nunique():,}" if source == "B2_same_orcid" else ""), ""]
        if d.empty:
            continue
        t = d.label.value_counts().rename_axis("label").reset_index(name="pairs")
        t["examples"] = [
            "; ".join(f"{x.name_a} ~ {x.name_b}"
                      + (f" [{x.grant_code}]" if isinstance(x.grant_code, str) else "")
                      for x in d[d.label == lb].head(n_examples).itertuples())
            for lb in t.label]
        out.append(_md_table(t))
    return "\n".join(out)
