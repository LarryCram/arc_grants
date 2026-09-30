"""
analysis/15_orcid_preparation.py -- working towards preparing ORCID records for use in the pipeline.

Step 1 (2026-09-30): report the contents of a few cached ORCID /record responses, section by
section, so we can see what an ORCID record actually holds before deciding what to extract.

Source: the ORCID /record cache, a diskcache at DISKCACHE_DIR/orcid_records_authenticated -- one
entry per ORCID (key = bare ORCID, value = the full ORCID Public API v3.0 /record JSON). It was
filled by the ORCID client now archived at ZARCHIVE/src_archive_20260930/utils/orcid_client.py; this
script only reads it.

Usage:
    .venv/bin/python analysis/15_orcid_preparation.py                 # the first 4 ORCIDs in the cache (sorted)
    .venv/bin/python analysis/15_orcid_preparation.py 0000-0002-... ...   # these ORCIDs

Output: OUTPUT_ROOT/orcid_preparation/orcid_record_contents.md (the path is printed).
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import diskcache

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import DISKCACHE_DIR, OUTPUT_ROOT

CACHE_DIR = DISKCACHE_DIR / "orcid_records_authenticated"
OUT_DIR = OUTPUT_ROOT / "orcid_preparation"
N_DEFAULT = 4

AFFILIATION_SECTIONS = ["employments", "educations", "qualifications", "distinctions",
                        "invited-positions", "memberships", "services"]


# ── small readers for ORCID's nested JSON ───────────────────────────────────

def _v(d, *path):
    """Follow path through nested dicts; None as soon as anything is missing."""
    for p in path:
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    return d


def _date_ms(d) -> str | None:
    """ORCID {"value": epoch-ms} -> YYYY-MM-DD."""
    ms = _v(d, "value")
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d") if ms else None


def _fuzzy_date(d) -> str:
    """ORCID {"year": {"value"}, "month": ..., "day": ...} -> YYYY[-MM[-DD]], '' if absent."""
    parts = [_v(d, k, "value") for k in ("year", "month", "day")]
    return "-".join(p for p in parts if p)


# ── one record -> a plain summary ───────────────────────────────────────────

def summarize_record(rec: dict) -> dict:
    person = rec.get("person") or {}
    acts = rec.get("activities-summary") or {}
    hist = rec.get("history") or {}

    affiliations = {}
    for section in AFFILIATION_SECTIONS:
        rows = []
        for group in _v(acts, section, "affiliation-group") or []:
            for s in group.get("summaries") or []:
                a = next(iter(s.values()))  # {"employment-summary": {...}} etc.
                org = a.get("organization") or {}
                addr = org.get("address") or {}
                rows.append({
                    "organisation": org.get("name"),
                    "department": a.get("department-name"),
                    "role": a.get("role-title"),
                    "start": _fuzzy_date(a.get("start-date")),
                    "end": _fuzzy_date(a.get("end-date")),
                    "place": ", ".join(x for x in (addr.get("city"), addr.get("region"), addr.get("country")) if x),
                    "source": _v(a, "source", "source-name", "value"),
                })
        affiliations[section] = rows

    works = []
    for group in _v(acts, "works", "group") or []:
        ws = group.get("work-summary") or []
        w = ws[0] if ws else {}
        doi = next((e.get("external-id-value") for e in _v(w, "external-ids", "external-id") or []
                    if e.get("external-id-type") == "doi"), None)
        works.append({
            "title": _v(w, "title", "title", "value"),
            "year": _v(w, "publication-date", "year", "value"),
            "type": w.get("type"),
            "journal": _v(w, "journal-title", "value"),
            "doi": doi,
            "source": _v(w, "source", "source-name", "value"),
            "versions": len(ws),
        })

    fundings = []
    for group in _v(acts, "fundings", "group") or []:
        for f in group.get("funding-summary") or []:
            fundings.append({
                "title": _v(f, "title", "title", "value"),
                "type": f.get("type"),
                "organisation": _v(f, "organization", "name"),
                "start": _fuzzy_date(f.get("start-date")),
                "end": _fuzzy_date(f.get("end-date")),
            })

    return {
        "orcid": _v(rec, "orcid-identifier", "path"),
        "history": {
            "created": _date_ms(hist.get("submission-date")),
            "completed": _date_ms(hist.get("completion-date")),
            "last_modified": _date_ms(hist.get("last-modified-date")),
            "creation_method": hist.get("creation-method"),
            "claimed": hist.get("claimed"),
            "verified_email": hist.get("verified-email"),
            "deactivated": _date_ms(hist.get("deactivation-date")),
        },
        "name": {
            "given_names": _v(person, "name", "given-names", "value"),
            "family_name": _v(person, "name", "family-name", "value"),
            "credit_name": _v(person, "name", "credit-name", "value"),
            "visibility": _v(person, "name", "visibility"),
        },
        "other_names": [o.get("content") for o in _v(person, "other-names", "other-name") or []],
        "biography": _v(person, "biography", "content"),
        "keywords": [k.get("content") for k in _v(person, "keywords", "keyword") or []],
        "countries": [_v(a, "country", "value") for a in _v(person, "addresses", "address") or []],
        "emails": [e.get("email") for e in _v(person, "emails", "email") or []],
        "researcher_urls": [(_v(u, "url-name"), _v(u, "url", "value"))
                            for u in _v(person, "researcher-urls", "researcher-url") or []],
        "external_ids": [(e.get("external-id-type"), e.get("external-id-value"))
                         for e in _v(person, "external-identifiers", "external-identifier") or []],
        "affiliations": affiliations,
        "fundings": fundings,
        "works": works,
        "peer_review_groups": len(_v(acts, "peer-reviews", "group") or []),
        "research_resource_groups": len(_v(acts, "research-resources", "group") or []),
    }


# ── rendering ───────────────────────────────────────────────────────────────

def _table(rows: list[dict], cols: list[str]) -> list[str]:
    if not rows:
        return ["(none)", ""]
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        out.append("| " + " | ".join(str(r.get(c) or "").replace("|", "/").replace("\n", " ") for c in cols) + " |")
    return out + [""]


def render_markdown(summaries: list[dict]) -> str:
    lines = ["# ORCID record contents", "",
             f"Source: `{CACHE_DIR}` (cached ORCID Public API /record responses).", ""]
    for s in summaries:
        n, h = s["name"], s["history"]
        lines += [f"## {s['orcid']} -- {n['given_names'] or ''} {n['family_name'] or ''}".rstrip(), ""]
        lines += ["**Name**", "",
                  f"- given names: {n['given_names']!r}; family name: {n['family_name']!r}; "
                  f"credit name: {n['credit_name']!r}; visibility: {n['visibility']}",
                  f"- other names: {s['other_names'] or '(none)'}", ""]
        lines += ["**Record history**", "",
                  f"- created {h['created']}, completed {h['completed']}, last modified {h['last_modified']}; "
                  f"created via {h['creation_method']}; claimed {h['claimed']}; verified email {h['verified_email']}; "
                  f"deactivated {h['deactivated']}", ""]
        lines += ["**Person**", "",
                  f"- biography: {s['biography'] or '(none)'}",
                  f"- keywords: {s['keywords'] or '(none)'}",
                  f"- countries: {s['countries'] or '(none)'}",
                  f"- emails (public): {s['emails'] or '(none)'}",
                  f"- researcher URLs: {s['researcher_urls'] or '(none)'}",
                  f"- external identifiers: {s['external_ids'] or '(none)'}", ""]
        for section, rows in s["affiliations"].items():
            lines += [f"**{section.capitalize()}** ({len(rows)})", ""]
            lines += _table(rows, ["organisation", "department", "role", "start", "end", "place", "source"])
        lines += [f"**Fundings** ({len(s['fundings'])})", ""]
        lines += _table(s["fundings"], ["title", "type", "organisation", "start", "end"])
        lines += [f"**Works** ({len(s['works'])})", ""]
        lines += _table(s["works"], ["year", "type", "title", "journal", "doi", "source", "versions"])
        lines += [f"**Other activity**: peer-review groups {s['peer_review_groups']}, "
                  f"research-resource groups {s['research_resource_groups']}", ""]
    return "\n".join(lines)


# ── main ────────────────────────────────────────────────────────────────────

def main(argv: list[str]) -> None:
    cache = diskcache.Cache(str(CACHE_DIR))
    try:
        orcids = argv or sorted(k for k in cache if isinstance(k, str))[:N_DEFAULT]
        missing = [o for o in orcids if o not in cache]
        if missing:
            raise SystemExit(f"not in the ORCID cache: {missing}")
        summaries = [summarize_record(cache[o]) for o in orcids]
    finally:
        cache.close()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / "orcid_record_contents.md"
    out.write_text(render_markdown(summaries), encoding="utf-8")
    print(f"{len(summaries)} ORCID records reported: {out}")


if __name__ == "__main__":
    main(sys.argv[1:])
