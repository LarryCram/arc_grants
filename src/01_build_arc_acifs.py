"""
src/01_build_arc_acifs.py -- runs the ACIF build (src/acif/build.py::build_acifs()) and persists the
ARC-stage list of people (2026-10-06). The OpenAlex additions will be a later persisted stage.

Outputs (config.settings):
    ACIFS_ARC             acifs_arc.parquet           one row per ACIF (src/acif/output.py::acif_rows)
    ACIF_ARC_RECORDS      acif_arc_records.parquet    one row per record: unique_id -> cluster_id
    ACIF_ARC_NAME_GROUPS  acif_arc_name_groups.parquet  the name stage's per-group report
    PROCESSED_DATA/acif_arc_build_report.md           counts per stage

Usage: .venv/bin/python src/01_build_arc_acifs.py
"""

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import ACIF_ARC_NAME_GROUPS, ACIF_ARC_RECORDS, ACIFS_ARC, PROCESSED_DATA
from src.acif.build import build_acifs
from src.acif.output import acif_rows, record_rows

REPORT = PROCESSED_DATA / "acif_arc_build_report.md"


def render(report, acifs_df) -> str:
    kept = acifs_df[~acifs_df.excluded]
    L = ["# ARC-stage ACIF build", "",
         "| stage | ACIFs |", "|---|---|",
         f"| in-scope records | {report['n_seed']:,} |",
         f"| ARC ORCID merge | {report['n_arc_orcid']:,} |",
         f"| Scopus pass one | {report['n_scopus_pass_one']:,} |",
         f"| Scopus pass two | {report['n_scopus_pass_two']:,} |",
         f"| hand stage | {report['n_hand']:,} |",
         f"| ORCID bulk pass | {report['n_orcid_bulk']:,} |",
         f"| name stage | {report['n_names']:,} |", "",
         f"Set aside as Indigenous-focused research (excluded=True): {report['n_excluded_indigenous']:,}; "
         f"kept: {len(kept):,}.", "",
         "Groups left unmerged, by stage and reason:", ""]
    for stage, key in [("ARC ORCID merge", "arc_orcid_mismatches"), ("Scopus pass one", "pass_one_mismatches"),
                       ("Scopus pass two", "pass_two_mismatches"), ("ORCID bulk pass", "orcid_bulk_mismatches")]:
        c = Counter(m["reason"] for m in report.get(key, []))
        L.append(f"- {stage}: " + (", ".join(f"{r} {n:,}" for r, n in sorted(c.items())) or "none"))
    h = report["hand"]
    L.append(f"- hand stage: refused {len(h['refused_groups']):,}, ORCID conflicts {len(h['orcid_conflicts']):,}")
    L.append("- ORCID bulk pass decisions (ACIFs without an ORCID): " + ", ".join(
        f"{k} {v:,}" for k, v in report["orcid_bulk_decisions"].decision.value_counts().items()))
    L.append("- name stage: " + ", ".join(f"{k} {v:,}" for k, v in sorted(report["names"]["status_counts"].items())))
    L += ["", "Kept ACIFs:", "",
          f"- with an ORCID: {int((kept.orcids.map(len) > 0).sum()):,} "
          f"(ORCID sources: " + ", ".join(f"{k} {v:,}" for k, v in sorted(
              Counter(s for ss in kept.orcid_sources for s in ss).items())) + ")",
          f"- with 2+ ORCIDs: {int((kept.orcids.map(len) > 1).sum()):,}",
          f"- records per ACIF: " + ", ".join(f"{k}: {v:,}" for k, v in sorted(Counter(
              min(n, 10) for n in kept.n_records).items())) + " (10 = 10+)",
          f"- with no FOR2020 code: {int((kept.for2020_codes.map(len) == 0).sum()):,}",
          f"- with a single-organisation university: {int((kept.single_org_universities.map(len) > 0).sum()):,}",
          f"- with a co-investigator: {int((kept.coawardee_acif_ids.map(len) > 0).sum()):,}"]
    return "\n".join(L) + "\n"


def main():
    acifs, _, report = build_acifs()
    a = acif_rows(acifs)
    r = record_rows(acifs)
    assert a.cluster_id.is_unique and r.unique_id.is_unique
    assert len(r) == report["n_seed"], "a record was lost"
    assert not (a.orcids.map(len) > 1).any(), "an ACIF holds two ORCIDs"
    a.to_parquet(ACIFS_ARC, index=False)
    r.to_parquet(ACIF_ARC_RECORDS, index=False)
    report["names"]["groups"].to_parquet(ACIF_ARC_NAME_GROUPS, index=False)
    text = render(report, a)
    REPORT.write_text(text, encoding="utf-8")
    print(text)
    print(f"Wrote {ACIFS_ARC.name} ({len(a):,}), {ACIF_ARC_RECORDS.name} ({len(r):,}), "
          f"{ACIF_ARC_NAME_GROUPS.name}, {REPORT.name}")


if __name__ == "__main__":
    main()
