"""
analysis/25_second_round.py -- does the ACIF build's one-pass stage chain reach a fixed point?
(2026-10-09, user: the plan's build was a loop -- rounds of merge tests on the state the previous
round left, repeated until a round merges nothing -- but build_acifs() runs each stage once.)

Runs build_acifs() as in production, then runs the same stage chain again on its output (same
UnionFind, same functions, same inputs): ARC ORCID merge -> Scopus pass one -> Scopus pass two ->
hand stage -> ORCID bulk pass -> name stage, and repeats until a whole round merges nothing (at
most MAX_ROUNDS).

Scopus pass one decides from 00d's Author Search, which was run once per ACIF of the stage after
the ARC ORCID merge (name keys + universities of THAT ACIF). An ACIF that has grown since is not a
searched unit: re-searching it would cost new Scopus queries, so here pass one is re-run only for
ACIFs that are still exactly a searched unit, and the number of grown, unsearched ACIFs is reported.
Nothing is written back; reports how many ACIFs each stage of each later round
merged, and which ACIFs, to processed/second_round.md.

Usage: .venv/bin/python analysis/25_second_round.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import PROCESSED_DATA
from src.acif.build import build_acifs, merge_by_key, merge_by_orcid
from src.acif.hand import hand_stage
from src.acif.name_merge import load_name_merge_inputs, name_merge
from src.acif.orcid_bulk import load_orcid_bulk_extract, orcid_bulk_pass
from src.acif.scopus import (_single_orcid, _with_scopus_orcid, drop_unlinked_scopus_orcids, load_scopus_extract,
                              scopus_orcid_decisions, scopus_pass_two)

OUT = PROCESSED_DATA / "second_round.md"
MAX_ROUNDS = 5


def names_of(a) -> str:
    return ", ".join(sorted({it.full_name for it in a.items}))


def searched_units(acifs, ext):
    """The ACIFs that are exactly one of 00d's searched units (same id, same records)."""
    lk = ext.lookup
    return [a for a in acifs if a.cluster_id in lk.index
            and sorted(lk.loc[a.cluster_id, "unique_ids"]) == sorted(it.unique_id for it in a.items)]


def pass_one_on_searched(acifs, uf, ext, note):
    """scopus_pass_one()'s steps, deciding only for ACIFs still equal to a searched unit."""
    units = searched_units(acifs, ext)
    note.append(len(acifs) - len(units))
    d = scopus_orcid_decisions(units, ext)
    acc = dict(zip(d.loc[d.decision == "accepted", "cluster_id"], d.loc[d.decision == "accepted", "scopus_orcid"]))
    acifs = [_with_scopus_orcid(a, acc[a.cluster_id]) if a.cluster_id in acc else a for a in acifs]
    merged, mm = merge_by_key(acifs, uf, _single_orcid)
    merged, dropped = drop_unlinked_scopus_orcids(merged, mm, ext.name_keys)
    if dropped:
        merged, mm = merge_by_key(merged, uf, _single_orcid)
    return merged


def main():
    acifs, uf, rep = build_acifs()
    L = ["# Second round of the ACIF build's stage chain", "",
         f"Round 1 (the production build): {rep['n_seed']:,} records -> {len(acifs):,} ACIFs.", ""]
    ext_s, ext_b, inp = load_scopus_extract(), load_orcid_bulk_extract(), load_name_merge_inputs()
    unsearched = []
    stages = [
        ("ARC ORCID merge", lambda a: merge_by_orcid(a, uf)[0]),
        ("Scopus pass one", lambda a: pass_one_on_searched(a, uf, ext_s, unsearched)),
        ("Scopus pass two", lambda a: scopus_pass_two(a, uf, ext_s)[0]),
        ("hand stage", lambda a: hand_stage(a, uf)[0]),
        ("ORCID bulk pass", lambda a: orcid_bulk_pass(a, uf, ext_b)[0]),
        ("name stage", lambda a: name_merge(a, uf, inp)[0]),
    ]
    for rnd in range(2, MAX_ROUNDS + 1):
        start = len(acifs)
        L += [f"## Round {rnd}", "", "| stage | ACIFs before | after | merged away |", "|---|---|---|---|"]
        examples = []
        for name, fn in stages:
            before = {a.cluster_id: a for a in acifs}
            acifs = fn(acifs)
            L.append(f"| {name} | {len(before):,} | {len(acifs):,} | {len(before) - len(acifs):,} |")
            for a in acifs:
                parts = [c for c in before if uf.find(c) == a.cluster_id] if a.cluster_id not in before or \
                    len(a.items) != len(before[a.cluster_id].items) else []
                if len(parts) > 1:
                    examples.append(f"- {name}: {a.cluster_id} ({names_of(a)}) <- " + "; ".join(
                        f"{p} ({names_of(before[p])}, {len(before[p].items)} records)" for p in sorted(parts)))
        L += ["", f"Round {rnd}: {start:,} -> {len(acifs):,} ACIFs. Scopus pass one skipped {unsearched[-1]:,} "
                  "ACIFs that have grown since 00d searched them (a new Author Search each would be needed).", ""]
        if examples:
            L += ["Merges made in this round:", ""] + examples + [""]
        if len(acifs) == start:
            L += [f"**Fixed point: round {rnd} merged nothing.**", ""]
            break
    OUT.write_text("\n".join(L) + "\n", encoding="utf-8")
    print("\n".join(L[:40]))
    print(OUT)


if __name__ == "__main__":
    main()
