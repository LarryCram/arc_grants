"""
05_match_layer2_names.py

Layer 2 disambiguation: name matching against OAX AU-affiliated authors.

Applies to investigators not resolved in Layer 1 (no_orcid + not_in_oax).
Searches OAX AU authors by family name, then scores each candidate on two
independent signals:

    first_score  — given-name match quality
        1.0  any arc given-name token found in any OAX name alternative
             (handles "Fred" matching "C. Fred Bloggy", "Fred C. Bloggy", etc.
              because all alpha tokens are extracted regardless of position)
        0.8  initial match, bidirectional:
               ARC initial  → any OAX alt has a full token starting with it
               ARC full     → OAX never records that full token in any alt
                              (initial-only representation, e.g. OAX "D. Bloggy")
        0.0  no given-name overlap

    institution_match  — True if any arc OAX institution ID intersects the
                         OAX author's AU institution IDs.

No score threshold is applied here — all family-name candidates are kept.
Script 06 uses first_score and institution_match to assign or defer to Layer 3.

OUTPUT:
    PROCESSED_DATA/layer2_name_candidates.parquet
        arc_first_name, arc_family_name, arc_orcid, arc_sub_case,
        arc_admin_orgs, arc_oax_inst_ids, n_arc_grants,
        oax_id, oax_display_name, oax_orcid,
        first_score, institution_match, n_candidates

INPUT:
    PROCESSED_DATA/layer1_residual.parquet
    PROCESSED_DATA/grants.parquet
    PROCESSED_DATA/institution_concordance.parquet
    OAX_AUTHORS/*.parquet
"""

import re
import sys
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import OAX_AUTHORS, PROCESSED_DATA
from src.utils.io import setup_stdout_utf8

OAX_GLOB = str(OAX_AUTHORS / "*.parquet")


# ── Name normalisation ────────────────────────────────────────────────────────

# Unicode hyphens that appear in OAX names but are not ASCII '-'
_EXOTIC_HYPHENS = re.compile(
    r"[­‐‑‒–—―−－]"
)


def _strip_diacriticals(s: str) -> str:
    s = _EXOTIC_HYPHENS.sub("-", s)          # non-breaking / em-dash hyphens → ASCII
    s = s.replace("ı", "i").replace("İ", "I")   # Turkish dotless-ı → i
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def _tokens(name: str) -> list[str]:
    return re.findall(r"[a-z]+", _strip_diacriticals(name).lower())


def _family_tokens(family_name: str) -> list[str]:
    """Tokens from family name after stripping parenthetical content (née, aliases)."""
    cleaned = re.sub(r"\s*\(.*?\)", "", family_name)
    return _tokens(cleaned)


def _is_initial(first_name: str) -> bool:
    return bool(re.match(r"^[A-Za-z]\.?$", first_name.strip()))


def first_score(arc_first: str, oax_names: list[str]) -> float:
    """
    Score given-name similarity between an ARC first name and a list of OAX
    name alternatives. Returns 1.0, 0.8, or 0.0.

    Token extraction is positional-agnostic: "C. Fred Bloggy" yields tokens
    ["c", "fred", "bloggy"], so "Fred" matches regardless of where it sits.

    Initial matching is bidirectional:
        ARC initial → OAX has a multi-char token starting with that initial (0.8)
        ARC full    → OAX never records the full token anywhere; has an initial
                      matching the first character                             (0.8)
    """
    arc_initial = _is_initial(arc_first)
    arc_given   = _tokens(arc_first)
    if not arc_given:
        return 0.0

    # All tokens appearing anywhere across all OAX name alternatives
    oax_all_toks = set(t for n in oax_names for t in _tokens(n))

    best = 0.0
    for t in arc_given:
        if t in oax_all_toks:
            return 1.0                           # full match — stop immediately

        if arc_initial and len(t) == 1:
            # ARC is an initial: credit if OAX has a full token starting with it
            if any(s.startswith(t) for s in oax_all_toks if len(s) > 1):
                best = max(best, 0.8)
        elif len(t) > 1:
            # ARC is a full name: credit if OAX is initial-only for this person
            # (i.e. full token never appears but the matching initial does)
            if t[0] in oax_all_toks and t not in oax_all_toks:
                best = max(best, 0.8)

    return best


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()
    con = duckdb.connect()

    # ── Load inputs ───────────────────────────────────────────────────────────
    df_res  = pd.read_parquet(PROCESSED_DATA / "layer1_residual.parquet")
    df_grts = pd.read_parquet(PROCESSED_DATA / "grants.parquet",
                              columns=["grant_code", "admin_org"])
    df_conc = pd.read_parquet(PROCESSED_DATA / "institution_concordance.parquet")

    print(f"Residual investigators:     {len(df_res):>8,}")
    print(f"  no_orcid:                 {(df_res['sub_case']=='no_orcid').sum():>8,}")
    print(f"  not_in_oax:               {(df_res['sub_case']=='not_in_oax').sum():>8,}")

    # ── Build grant → OAX institution IDs map ─────────────────────────────────
    inst_map = (
        df_grts.merge(df_conc[["arc_admin_org", "institution_id"]],
                      left_on="admin_org", right_on="arc_admin_org", how="left")
        .groupby("grant_code")["institution_id"]
        .apply(lambda x: sorted(set(v for v in x if pd.notna(v))))
        .reset_index()
        .rename(columns={"institution_id": "oax_inst_ids"})
    )

    # ── Deduplicate residual to unique persons ────────────────────────────────
    df_res_inst = (
        df_res
        .merge(df_grts[["grant_code", "admin_org"]], on="grant_code", how="left")
        .merge(inst_map, on="grant_code", how="left")
    )

    persons = (
        df_res_inst.groupby(["first_name", "family_name", "sub_case"], dropna=False)
        .agg(
            arc_orcid=("orcid", "first"),
            arc_admin_orgs=("admin_org", lambda x: "|".join(sorted(set(str(v) for v in x if pd.notna(v))))),
            arc_oax_inst_ids=("oax_inst_ids", lambda x: sorted(set(
                iid for lst in x if isinstance(lst, list) for iid in lst
            ))),
            n_arc_grants=("grant_code", "nunique"),
        )
        .reset_index()
    )
    print(f"\nUnique persons to match:    {len(persons):>8,}")
    print(f"  with initial-only first:  {persons['first_name'].apply(_is_initial).sum():>8,}")

    # ── Load OAX AU authors ───────────────────────────────────────────────────
    print(f"\nLoading OAX AU authors...")
    df_oax = con.execute(f"""
        SELECT
            id,
            display_name,
            display_name_alternatives,
            regexp_replace(orcid, '^https://orcid\\.org/', '') AS orcid,
            list_distinct(list_transform(
                list_filter(affiliations, x -> x.institution.country_code = 'AU'),
                x -> x.institution.id
            )) AS au_inst_ids
        FROM read_parquet('{OAX_GLOB}')
        WHERE list_contains(
            list_transform(affiliations, x -> x.institution.country_code), 'AU'
        )
    """).df()
    print(f"  OAX AU authors (ever AU): {len(df_oax):>8,}")

    # ── Build family-name index on OAX ───────────────────────────────────────
    def last_norm_token(name: str) -> str:
        toks = _tokens(name)
        return toks[-1] if toks else ""

    df_oax["_fam_idx"] = df_oax["display_name"].apply(last_norm_token)
    fam_index = df_oax.groupby("_fam_idx").indices

    # ── Match persons against OAX ─────────────────────────────────────────────
    print(f"Matching persons by family name...")
    records = []

    for _, arc in persons.iterrows():
        fam_toks = _family_tokens(arc["family_name"])
        if not fam_toks:
            continue

        # Try every token from the family name and union results.
        # Needed for compound names: "Del Mar" → try "del" and "mar";
        # without this, max(key=len) on equal-length tokens picks wrong one.
        idxs_set: set[int] = set()
        for tok in fam_toks:
            idxs_set.update(fam_index.get(tok, []))
        idxs = list(idxs_set)
        if not idxs:
            continue

        arc_inst_set = set(arc["arc_oax_inst_ids"])

        for oax in df_oax.iloc[idxs].itertuples(index=False):
            alts      = list(oax.display_name_alternatives) if oax.display_name_alternatives is not None else []
            all_names = [oax.display_name] + alts

            fscore       = first_score(arc["first_name"], all_names)
            oax_inst_ids = oax.au_inst_ids if oax.au_inst_ids is not None else []
            inst_match   = bool(arc_inst_set & set(oax_inst_ids))

            records.append({
                "arc_first_name":    arc["first_name"],
                "arc_family_name":   arc["family_name"],
                "arc_orcid":         arc["arc_orcid"],
                "arc_sub_case":      arc["sub_case"],
                "arc_admin_orgs":    arc["arc_admin_orgs"],
                "arc_oax_inst_ids":  "|".join(sorted(arc_inst_set)),
                "n_arc_grants":      arc["n_arc_grants"],
                "oax_id":            oax.id,
                "oax_display_name":  oax.display_name,
                "oax_orcid":         oax.orcid if oax.orcid else None,
                "first_score":       fscore,
                "institution_match": inst_match,
            })

    df_cands = pd.DataFrame(records)
    print(f"  Candidate pairs found:    {len(df_cands):>8,}")

    if len(df_cands) == 0:
        print("No matches found.")
        return

    # ── Add n_candidates per ARC person ──────────────────────────────────────
    df_cands["n_candidates"] = (
        df_cands.groupby(["arc_first_name", "arc_family_name"], dropna=False)["oax_id"]
        .transform("count")
    )

    # ── Summary ───────────────────────────────────────────────────────────────
    n_persons_any = df_cands.groupby(
        ["arc_first_name", "arc_family_name"], dropna=False
    ).ngroups
    n_first_match = df_cands[df_cands["first_score"] > 0].groupby(
        ["arc_first_name", "arc_family_name"], dropna=False
    ).ngroups

    print(f"\n{'─'*52}")
    print(f"LAYER 2 CANDIDATE SUMMARY")
    print(f"{'─'*52}")
    print(f"  Persons with ≥1 family-name hit:     {n_persons_any:>6,}  of {len(persons):,}")
    print(f"  Persons with first_score > 0:        {n_first_match:>6,}")
    print(f"  Persons with no family-name hit:     {len(persons)-n_persons_any:>6,}")
    print(f"  Total candidate pairs:               {len(df_cands):>6,}")
    n_inst = df_cands["institution_match"].sum()
    print(f"  Institution match pairs:             {n_inst:>6,}  ({100*n_inst/len(df_cands):.1f}%)")

    print(f"\n  first_score distribution (persons / pairs):")
    for sc, label in [(1.0, "full match"), (0.8, "initial match"), (0.0, "no first match")]:
        sub = df_cands[df_cands["first_score"] == sc]
        n_p = sub.groupby(["arc_first_name", "arc_family_name"], dropna=False).ngroups
        print(f"    {sc:.1f}  {label:<18} {n_p:>6,} persons   {len(sub):>8,} pairs")

    # ── Save ──────────────────────────────────────────────────────────────────
    out_path = PROCESSED_DATA / "layer2_name_candidates.parquet"
    df_cands.to_parquet(out_path, index=False)
    print(f"\nSaved: {out_path}  ({len(df_cands):,} rows)")


if __name__ == "__main__":
    main()
