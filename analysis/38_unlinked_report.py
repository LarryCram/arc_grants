"""
analysis/38_unlinked_report.py -- the kept ACIFs the linker left without an OpenAlex record (2026-10-24, user:
"report unlinked acifs. number of grants and years, count by type, count by fellowship. summarise count of
reasons for not linking").

Reads acifs_arc / acif_arc_records (01) and the linker's per-stage decisions (02, processed/oax_link/).
Reason per ACIF, first that applies:
  hand_unlinked            a manual_links.csv unlink row (checked by hand: not in OpenAlex)
  orcid_not_in_openalex    the ACIF has an ORCID that no OpenAlex record carries
  no_same_name_record      no OpenAlex record with a compatible name for any stage
  refused                  a stage found a record but refused it (name differs, record taken, other ORCID, ...)
  ambiguous                a stage found several equally good records
  no_evidence              same-name records exist, none with enough evidence
Writes processed/unlinked_report.md.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.scope import FELLOWSHIP_TIER
from config.settings import ACIF_ARC_RECORDS, ACIFS_ARC, OAX_LINK_DIR, PROCESSED_DATA
from src.oax import manual_links as ml

OUT = PROCESSED_DATA / "unlinked_report.md"
STAGES = [("name", "name_decisions"), ("works", "works_decisions"), ("scopus", "scopus_decisions"),
          ("orcid_hint", "orcid_hint_decisions"), ("rare_name", "rare_name_decisions"),
          ("orcid_doi", "orcid_doi_decisions"), ("grant_ack", "grant_ack_decisions")]
REFUSED = {"record_name_differs", "record_linked_elsewhere", "pass_other_orcid", "record_other_orcid",
           "weak_shared_record", "several_acifs"}
AMBIGUOUS = {"several_pass", "several_university", "several_coinvestigator", "several_profiles",
             "several_records", "several_candidates", "tied"}
NO_CANDIDATE = {"no_candidate", "no_orcid_candidate", "no_profile", "not_rare", "no_evidence"}


def table(s: pd.Series, head: str, total: int) -> list[str]:
    L = [f"| {head} | ACIFs | share |", "|---|---|---|"]
    L += [f"| {k} | {v:,} | {v / total:.1%} |" for k, v in s.items()]
    return L


def reason(row, hand_unlinked, unmatched) -> str:
    if row.cluster_id in hand_unlinked:
        return "hand_unlinked"
    if row.cluster_id in unmatched:
        return "orcid_not_in_openalex"
    st = {row[s] for s, _ in STAGES if isinstance(row[s], str)}
    if not st or (row["name"] == "no_candidate" and row["rare_name"] in ("no_candidate", None)):
        return "no_same_name_record"
    if st & REFUSED:
        return "refused"
    if st & AMBIGUOUS:
        return "ambiguous"
    if st <= NO_CANDIDATE:
        return "no_same_name_record"
    return "no_evidence"


def main():
    a = pd.read_parquet(ACIFS_ARC)
    a = a[~a.excluded]
    acc = pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet")
    u = a[~a.cluster_id.isin(set(acc.cluster_id))].copy()
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    rec = rec[rec.cluster_id.isin(set(u.cluster_id))]

    for s, f in STAGES:
        if not (OAX_LINK_DIR / f"{f}.parquet").exists():
            u[s] = None
            continue
        d = pd.read_parquet(OAX_LINK_DIR / f"{f}.parquet")[["cluster_id", "status"]].drop_duplicates("cluster_id")
        u = u.merge(d.rename(columns={"status": s}), on="cluster_id", how="left")
    _, unl, _, _ = ml.resolve(ml.load(), set(a.cluster_id))
    unmatched = set(pd.read_parquet(OAX_LINK_DIR / "orcid_unmatched.parquet").cluster_id)
    u["reason"] = [reason(r, unl, unmatched) for _, r in u.iterrows()]
    n = len(u)

    L = ["# Unlinked ACIFs (2026-10-24)", "",
         f"Kept ACIFs: {len(a):,}; linked: {a.cluster_id.isin(set(acc.cluster_id)).sum():,}; "
         f"**unlinked: {n:,} ({n / len(a):.1%})**", "", "## Grants and years", "",
         "Grants per ACIF:", ""]
    L += table(u.n_grants.clip(upper=5).map(lambda x: "5+" if x >= 5 else str(int(x))).value_counts().sort_index(),
               "grants", n)
    band = pd.cut(u.last_year, [2000, 2005, 2010, 2015, 2020, 2030],
                  labels=["2001-05", "2006-10", "2011-15", "2016-20", "2021-26"])
    L += ["", "Last grant year:", ""] + table(band.value_counts().sort_index(), "last grant", n)
    L += ["", f"First grant year: median {int(u.first_year.median())}; last grant year: median "
          f"{int(u.last_year.median())}; career span (last - first) 0 years: {(u.last_year == u.first_year).sum():,}"]

    L += ["", "## Grant type (scheme of each grant; an ACIF counts once per scheme it holds)", ""]
    sch = rec.assign(scheme=rec.grant_code.str[:2]).groupby("scheme").cluster_id.nunique().sort_values(ascending=False)
    L += table(sch, "scheme", n)

    L += ["", "## Fellowships (an ACIF counts once per fellowship role it holds)", ""]
    fel = rec[rec.is_fellowship.fillna(False)]
    L += [f"ACIFs holding any fellowship: {fel.cluster_id.nunique():,} of {n:,}", ""]
    rc = fel.groupby("role_code").cluster_id.nunique().sort_values(ascending=False)
    L += ["| role | tier | ACIFs |", "|---|---|---|"]
    L += [f"| {k} | {FELLOWSHIP_TIER.get(k, '?')} | {v:,} |" for k, v in rc.items()]
    tier = fel.assign(tier=fel.role_code.map(FELLOWSHIP_TIER).fillna("?")).groupby("tier").cluster_id.nunique()
    L += ["", "By tier: " + ", ".join(f"{k} {v:,}" for k, v in tier.items())]

    L += ["", "## Reasons for not linking", "",
          "First that applies: hand unlink; ORCID on no OpenAlex record; no same-name OpenAlex record; a stage "
          "refused the record it found; several equally good records; same-name records without enough evidence.",
          ""]
    L += table(u.reason.value_counts(), "reason", n)
    L += ["", "By reason and last grant year:", "", pd.crosstab(u.reason, band).to_markdown(), ""]
    L += ["## Each stage's verdict on the unlinked ACIFs", ""]
    for s, _ in STAGES:
        L += [f"**{s}**: " + ", ".join(f"{k} {v:,}" for k, v in u[s].fillna("not run").value_counts().items()), ""]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    u[["cluster_id", "n_grants", "first_year", "last_year", "reason"] + [s for s, _ in STAGES]].to_csv(
        PROCESSED_DATA / "unlinked_acifs.csv", index=False)
    print(OUT)


if __name__ == "__main__":
    main()
