"""
00c_probe_nameparser_family.py

For every unique (first_name, family_name) pair in investigators.parquet,
concatenate them, pass through HumanName, and report cases where
nameparser's `last` differs from the ARC family_name field.

OUTPUT: stdout report + PROCESSED_DATA/nameparser_family_mismatches.csv
"""

import sys
from pathlib import Path

import pandas as pd
from nameparser import HumanName

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import PROCESSED_DATA
from src.utils.io import setup_stdout_utf8


def main():
    setup_stdout_utf8()

    df_inv = pd.read_parquet(PROCESSED_DATA / "investigators.parquet",
                             columns=["first_name", "family_name"])
    pairs = df_inv.drop_duplicates().reset_index(drop=True)
    print(f"Unique (first, family) pairs: {len(pairs):,}")

    rows = []
    for _, r in pairs.iterrows():
        arc_first  = str(r["first_name"])
        arc_family = str(r["family_name"])
        full       = f"{arc_first} {arc_family}"
        n          = HumanName(full)
        np_last    = n.last
        if np_last != arc_family:
            rows.append({
                "arc_first":  arc_first,
                "arc_family": arc_family,
                "full_input": full,
                "np_title":   n.title,
                "np_first":   n.first,
                "np_middle":  n.middle,
                "np_last":    np_last,
                "np_suffix":  n.suffix,
                "np_nickname":n.nickname,
            })

    df = pd.DataFrame(rows)
    n_total    = len(pairs)
    n_mismatch = len(df)
    print(f"Mismatches (np.last != arc_family): {n_mismatch:,}  ({100*n_mismatch/n_total:.1f}%)")

    if n_mismatch == 0:
        print("No mismatches found.")
        return

    # Categorise mismatches
    # Case A: np.last is empty — nameparser absorbed family into first/middle
    case_a = df[df["np_last"] == ""]
    # Case B: np.last is a prefix/subset of arc_family (compound name split)
    case_b = df[(df["np_last"] != "") & (df["np_last"] != df["arc_family"])]

    print(f"\n  Case A — np.last empty (family absorbed into given names): {len(case_a):,}")
    print(f"  Case B — np.last differs but non-empty (compound/split):    {len(case_b):,}")

    print(f"\n{'─'*80}")
    print("Case A sample (np.last is empty):")
    print(f"{'arc_first':<25} {'arc_family':<25} {'np_first':<20} {'np_middle'}")
    print(f"{'─'*25} {'─'*25} {'─'*20} {'─'*20}")
    for _, r in case_a.head(30).iterrows():
        print(f"{r['arc_first']:<25} {r['arc_family']:<25} {r['np_first']:<20} {r['np_middle']}")

    print(f"\n{'─'*80}")
    print("Case B sample (np.last non-empty but != arc_family):")
    print(f"{'arc_first':<25} {'arc_family':<25} {'np_last':<25} {'np_first':<15} {'np_middle'}")
    print(f"{'─'*25} {'─'*25} {'─'*25} {'─'*15} {'─'*20}")
    for _, r in case_b.head(40).iterrows():
        print(f"{r['arc_first']:<25} {r['arc_family']:<25} {r['np_last']:<25} {r['np_first']:<15} {r['np_middle']}")

    out = PROCESSED_DATA / "nameparser_family_mismatches.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved: {out}  ({len(df):,} rows)")


if __name__ == "__main__":
    main()
