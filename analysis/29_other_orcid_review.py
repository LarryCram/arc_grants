"""
analysis/29_other_orcid_review.py -- review list for the Scopus bridge's 'record_other_orcid' refusals
(2026-10-22, user: yes, prepare the list). Each case: an unlinked ACIF whose own ORCID is on no OpenAlex
record, and whose one Scopus profile's DOIs point at an OpenAlex record carrying a different ORCID that
no other ACIF holds. Usually one person with two ORCID accounts (or a co-author's ORCID on the record);
sometimes the Scopus profile is a namesake's.

Per case, side by side: the ACIF's grants (years, university, primary field); its ORCID and the
record's ORCID (name, aliases, first employers, from the ORCID bulk file); the Scopus profile (DOIs,
DOIs on the record, top sources, sample titles); the OpenAlex record (name, works, top fields, top
institutions). Writes processed/other_orcid_review.md -- for a decision per case (accept the record /
leave unlinked); how accepted cases are recorded is decided with the review.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
import pandas as pd

from config.settings import (ACIF_ARC_RECORDS, ACIFS_ARC, OAX_LINK_DIR, OPENALEX_COMPACT_DIR, ORCID_BULK_PARQUET,
                             PROCESSED_DATA, SCOPUS_EXTRACT_DIR)

OUT = PROCESSED_DATA / "other_orcid_review.md"


def main():
    con = duckdb.connect()
    a = pd.read_parquet(ACIFS_ARC).set_index("cluster_id")
    acc = set(pd.read_parquet(OAX_LINK_DIR / "accepted_links.parquet").cluster_id)
    d = pd.read_parquet(OAX_LINK_DIR / "scopus_decisions.parquet")
    d = d[(d.status == "record_other_orcid") & ~d.cluster_id.isin(acc) & d.cluster_id.isin(a.index[~a.excluded])]
    d = d.astype({"author_idx": "int64"})
    ev = pd.read_parquet(OAX_LINK_DIR / "scopus_evidence.parquet")
    rec = pd.read_parquet(ACIF_ARC_RECORDS)
    g = pd.read_parquet(PROCESSED_DATA / "grants_flat.parquet", columns=["grant_code", "primary_for_name", "admin_org"])
    docs = pd.read_parquet(SCOPUS_EXTRACT_DIR / "scopus_profile_documents.parquet")
    con.register("ids", pd.DataFrame({"author_idx": sorted(set(d.author_idx))}))
    prep = con.execute(f"""SELECT author_idx, full_name, orcid, works_count_global FROM read_parquet('{PROCESSED_DATA}/openalex_authors_prep.parquet')
                           WHERE author_idx IN (SELECT author_idx FROM ids)""").df().set_index("author_idx")
    au = con.execute(f"""SELECT author_idx, work_idx, institution_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/authorships/*.parquet')
                         WHERE author_idx IN (SELECT author_idx FROM ids)""").df()
    con.register("aw", au[["work_idx"]].drop_duplicates())
    fields = con.execute(f"""SELECT work_idx, field_name FROM read_parquet('{OPENALEX_COMPACT_DIR}/work_topics/*.parquet')
                             WHERE work_idx IN (SELECT work_idx FROM aw) QUALIFY row_number() OVER (PARTITION BY work_idx ORDER BY score DESC) = 1""").df()
    au = au.merge(fields, on="work_idx", how="left")
    orcids = sorted({a.loc[c, "orcids"][0] for c in d.cluster_id} | set(prep.orcid.dropna()))
    con.register("os", pd.DataFrame({"orcid": orcids}))
    bulk = con.execute(f"""SELECT orcid, name, aliases, [e.name || coalesce(' ' || e.start_year::VARCHAR, '') FOR e IN employments][1:4] AS emp
                           FROM read_parquet('{ORCID_BULK_PARQUET}') WHERE orcid IN (SELECT orcid FROM os)""").df().set_index("orcid")

    def orc(o):
        if o not in bulk.index:
            return f"{o} (not in the ORCID bulk file)"
        b = bulk.loc[o]
        al = f"; aliases {', '.join(b.aliases)}" if b.aliases is not None and len(b.aliases) else ""
        return f"{o} {b['name']}{al}; employers: {', '.join(b.emp) if b.emp is not None and len(b.emp) else '-'}"

    L = [f"# Scopus bridge refusals: record carries another ORCID ({len(d)} unlinked ACIFs)", "",
         "Decide per case: **accept** (the record is the person: add an accept_link row) or **leave**.", ""]
    for k, x in enumerate(d.sort_values("cluster_id").itertuples(), 1):
        r = a.loc[x.cluster_id]
        gr = rec[rec.cluster_id == x.cluster_id].merge(g, on="grant_code", suffixes=("", "_g")).sort_values("funding_commence_year")
        p = prep.loc[x.author_idx] if x.author_idx in prep.index else None
        e = ev[(ev.cluster_id == x.cluster_id) & (ev.author_idx == x.author_idx)] if "author_idx" in ev else ev.iloc[0:0]
        pd_ = docs[docs.scopus_id == str(x.scopus_id)]
        w = au[au.author_idx == x.author_idx]
        L += [f"## {k}. {list(r.full_names)[0]} -- {x.cluster_id}", "",
              "- ARC: " + "; ".join(f"{t.grant_code} {int(t.funding_commence_year)} {t.admin_org_g} ({t.primary_for_name})"
                                    for t in gr.itertuples()),
              f"- ACIF ORCID ({r.orcid_sources[0]}): {orc(r.orcids[0])}",
              f"- record A{x.author_idx} {p.full_name if p is not None else '?'} ({int(p.works_count_global) if p is not None else '?'} works); "
              f"its ORCID: {orc(p.orcid) if p is not None and isinstance(p.orcid, str) else '-'}",
              f"- record's fields: " + ", ".join(f"{f} {n}" for f, n in w.drop_duplicates('work_idx').field_name.value_counts().head(3).items())
              + "; institutions: " + ", ".join(f"{i} {n}" for i, n in w.institution_name.value_counts().head(3).items()),
              f"- Scopus profile {x.scopus_id}: {len(pd_)} documents ({pd_.doi.notna().sum()} with DOI)"
              + (f"; on the record: {int(e.iloc[0]['shared_dois'])} of {int(e.iloc[0]['profile_dois_in_openalex'])} in OpenAlex" if len(e) else "")
              + "; sources: " + ", ".join(f"{s} {n}" for s, n in pd_.source.value_counts().head(3).items()),
              "- titles: " + " | ".join(t[:80] for t in pd_.sort_values("year", ascending=False).title.dropna().head(3)), ""]
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT, len(d))


if __name__ == "__main__":
    main()
