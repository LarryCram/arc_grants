"""
17_oax_search.py  —  Layer 5: Live OAX REST API author disambiguation

For UNDECIDABLE clusters, query the OpenAlex /authors search endpoint by name
(affiliations.institution.country_code:au filter) then apply a strict gate:

  Name match    — OAX display name first token consistent with ARC first name
  Uniqueness    — exactly one candidate survives all filters
  Field match   — candidate OAX topic fields overlap with cluster FoR fields
  Institution   — candidate OAX affiliations include ≥1 certain cluster HEP

Both field AND institution must fire, AND uniqueness required.
A "best match" without uniqueness is never accepted.

Each search costs 10 OAX credits ($0.001). Free tier: ~1,000 queries/day.
The script resumes from where it stopped (skips clusters with oax_candidates
already set) and checkpoints every 250 clusters.

Usage:
  python 17_oax_search.py              # all UNDECIDABLE clusters
  python 17_oax_search.py --limit 128  # first N only (spot-check)
  python 17_oax_search.py --limit 128 --dry-run  # report only, no save

INPUT:  PROCESSED_DATA/clusters.jsonl
        PROCESSED_DATA/institution_concordance.parquet
        DATA_ROOT/for_oax_concordance.csv
OUTPUT: PROCESSED_DATA/clusters.jsonl              (updated unless --dry-run)
        PROCESSED_DATA/clusters_after_layer5.jsonl (checkpoint)
        PROCESSED_DATA/layer5_candidates.csv       (all gate-passing candidates)
"""

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import FOR_OAX_CSV, OPENALEX_EMAIL, OPENALEX_API_KEY, PROCESSED_DATA
from src.pipeline.models import Cluster, ClusterStatus, load_clusters, save_clusters
from src.utils.io import setup_stdout_utf8

OAX_API      = "https://api.openalex.org/authors"
API_DELAY    = 0.12   # slightly over 10 req/s to stay within key limit
RESULTS_CAP  = 15     # max candidates to fetch per query

# ARC scheme codes that are solo-award fellowships (one person, one HEP, certain)
FELLOWSHIP_CODES = {"FT", "FL", "DE", "DECRA", "APD", "APF", "ARF", "QEII", "FF", "APDI", "IRF", "ARFI", "LE"}


# ── HEP certainty ─────────────────────────────────────────────────────────────

def _build_grant_ci_count(clusters: list) -> dict[str, int]:
    """Grant code → number of distinct clusters (= number of CIs) sharing that grant."""
    from collections import defaultdict
    grant_to_cids: dict[str, set] = defaultdict(set)
    for c in clusters:
        for r in c.records:
            m = re.search(r"grant_code='([^']+)'", str(r))
            if m:
                grant_to_cids[m.group(1)].add(c.cluster_id)
    return {gc: len(cids) for gc, cids in grant_to_cids.items()}


def _certain_heps(cluster: Cluster, grant_ci_count: dict[str, int]) -> set[str]:
    """
    HEPs where this person's presence is unambiguous.

    Certain when any of:
      - Fellowship record (solo award by ARC scheme definition)
      - Solo CI (only cluster sharing that grant_code)
      - Single-HEP grant (all CIs at the same institution)
    Falls back to empty set if only multi-CI multi-HEP records exist.
    """
    certain: set[str] = set()
    for r in cluster.records:
        rs = str(r)
        m_code = re.search(r"grant_code='([^']+)'", rs)
        m_role = re.search(r"role_code='([^']+)'", rs)
        m_heps = re.search(r"grant_heps=\[([^\]]*)\]", rs)
        if not (m_code and m_role and m_heps):
            continue
        grant_code = m_code.group(1)
        role_code  = m_role.group(1)
        grant_heps = [h.strip("' ") for h in m_heps.group(1).split(",") if h.strip("' ")]

        is_fellowship  = role_code in FELLOWSHIP_CODES
        is_solo_ci     = grant_ci_count.get(grant_code, 1) == 1
        is_single_hep  = len(grant_heps) == 1

        if is_fellowship or is_solo_ci or is_single_hep:
            certain.update(grant_heps)
    return certain


# ── OAX live API ──────────────────────────────────────────────────────────────

def _oax_search(name: str) -> list[dict]:
    """Search OAX for AU authors matching name. Returns raw author dicts."""
    # Build URL manually: urlencode would encode the colon in country_code:au as %3A
    encoded_name = urllib.parse.quote(name, safe="")
    encoded_mail = urllib.parse.quote(OPENALEX_EMAIL, safe="@.")
    url = (
        f"{OAX_API}?search={encoded_name}"
        f"&filter=affiliations.institution.country_code:au"
        f"&per_page={RESULTS_CAP}"
        f"&select=id,display_name,orcid,works_count,cited_by_count,affiliations,topics"
        f"&mailto={encoded_mail}"
    )
    if OPENALEX_API_KEY:
        url += f"&api_key={urllib.parse.quote(OPENALEX_API_KEY, safe='')}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return data.get("results", [])
    except Exception as exc:
        print(f"    API error for '{name}': {exc}")
        return []


# ── Candidate filtering ───────────────────────────────────────────────────────

def _candidate_heps(author: dict, inst_id_to_hep: dict[str, str]) -> set[str]:
    """Map all OAX affiliation institution IDs to HEP codes."""
    heps: set[str] = set()
    for aff in author.get("affiliations") or []:
        inst = aff.get("institution") or {}
        iid  = inst.get("id") or ""
        h    = inst_id_to_hep.get(iid)
        if h:
            heps.add(h)
    return heps


def _candidate_fields(author: dict) -> set[str]:
    """Extract OAX topic field display names for this author."""
    fields: set[str] = set()
    for topic in author.get("topics") or []:
        f = (topic.get("field") or {}).get("display_name")
        if f:
            fields.add(f)
    return fields


def _name_consistent(arc_first: str, oax_display_name: str) -> bool:
    """True if the OAX display name's first token is consistent with the ARC first name.

    Allows: exact match, either side is an initial (single letter), or one is a
    prefix of the other (Ben/Benjamin).  Rejects clearly different first names
    (Jan vs John, Daphne vs Timothy, Xi vs Sean).
    """
    # First token of OAX display name is the first name in Western-order names.
    # East-Asian order (family name first, e.g. "NI Bing-jie") will fail the
    # check and be correctly excluded — the properly-ordered record will match.
    oax_tokens = oax_display_name.split()
    if not oax_tokens:
        return False
    oax_f = oax_tokens[0].strip(".,").lower().replace("‐", "-")  # normalise Unicode hyphen
    arc_f = arc_first.strip().lower().replace("‐", "-")

    if not arc_f or not oax_f:
        return False
    if arc_f == oax_f:
        return True
    # Either side is a single-letter initial
    if len(arc_f) == 1 or len(oax_f) == 1:
        return arc_f[0] == oax_f[0]
    # Prefix match: Ben/Benjamin, Bing-Jie/Bing
    if arc_f.startswith(oax_f) or oax_f.startswith(arc_f):
        return True
    return False


def _passes_gate(
    cluster: Cluster,
    author: dict,
    inst_id_to_hep: dict[str, str],
    for2d_to_fields: dict[str, set[str]],
    certain_heps: set[str],
) -> bool:
    """Return True only if BOTH institution AND field signals fire.

    Institution signal uses certain_heps (unambiguous HEP-person links) when
    available, falling back to the full cluster institution set otherwise.
    """
    cand_heps   = _candidate_heps(author, inst_id_to_hep)
    inst_filter = certain_heps if certain_heps else set(cluster.institutions)
    if not (cand_heps & inst_filter):
        return False

    # Field: ≥1 OAX field matches cluster FoR-mapped fields
    expected: set[str] = set()
    for code in cluster.for_2d:
        expected.update(for2d_to_fields.get(code, set()))
    if not expected:
        return True   # institution already passed; no FoR evidence to check
    cand_fields = _candidate_fields(author)
    return bool(cand_fields & expected)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_stdout_utf8()

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",   type=int, default=None, help="Process only first N clusters")
    parser.add_argument("--dry-run", action="store_true",    help="Report only; do not save")
    args = parser.parse_args()

    path = PROCESSED_DATA / "clusters.jsonl"
    print("Loading clusters...")
    clusters = load_clusters(path)
    targets  = [c for c in clusters if c.status == ClusterStatus.UNDECIDABLE.value]
    print(f"  Total: {len(clusters):,}  |  UNDECIDABLE: {len(targets):,}")

    if args.limit:
        targets = targets[:args.limit]
        print(f"  (limited to first {args.limit})")

    using_key = bool(OPENALEX_API_KEY)
    print(f"  API key: {'yes' if using_key else 'no (polite anonymous)'}")

    # Concordances
    df_conc = pd.read_parquet(
        PROCESSED_DATA / "institution_concordance.parquet",
        columns=["institution_id", "hep_code"],
    )
    inst_id_to_hep: dict[str, str] = {
        row.institution_id: row.hep_code
        for row in df_conc[df_conc["institution_id"].notna() & df_conc["hep_code"].notna()].itertuples()
    }

    df_for = pd.read_csv(FOR_OAX_CSV, sep=";")
    df_for["for_2d"] = df_for["for_2digit"].apply(lambda x: f"{int(x):02d}")
    for2d_to_fields: dict[str, set[str]] = (
        df_for.groupby("for_2d")["oax_field"].apply(set).to_dict()
    )

    # HEP certainty: grant_code → CI count across all clusters
    print("Building HEP certainty index...")
    grant_ci_count = _build_grant_ci_count(clusters)

    # ── Resume support ────────────────────────────────────────────────────────
    # Skip clusters already processed in a prior partial run (have oax_candidates
    # set, or are already MATCHED/ABSENT from this layer).
    already_done = sum(1 for c in targets if c.oax_candidates or c.status != ClusterStatus.UNDECIDABLE.value)
    targets = [c for c in targets if not c.oax_candidates and c.status == ClusterStatus.UNDECIDABLE.value]
    if already_done:
        print(f"  Resuming: skipping {already_done} already-processed clusters")

    # ── Query and filter ──────────────────────────────────────────────────────
    print(f"\nQuerying OAX live API for {len(targets):,} clusters...\n")
    n_matched = n_ambiguous = n_no_inst = n_no_field = n_zero = 0
    rows: list[dict] = []
    ambiguous_rows: list[dict] = []

    for i, cluster in enumerate(targets, 1):
        nf   = cluster.name_forms[0]
        name = f"{nf.arc.first} {nf.arc.last}".strip()
        cert_heps = _certain_heps(cluster, grant_ci_count)
        time.sleep(API_DELAY)
        results = _oax_search(name)

        if not results:
            n_zero += 1
            outcome = "zero results"
        else:
            arc_first = cluster.name_forms[0].arc.first
            name_ok   = [r for r in results if _name_consistent(arc_first, r["display_name"])]
            passing   = [r for r in name_ok  if _passes_gate(cluster, r, inst_id_to_hep, for2d_to_fields, cert_heps)]

            if len(passing) == 1:
                r       = passing[0]
                oax_id  = r["id"]
                orcid   = (r.get("orcid") or "").replace("https://orcid.org/", "") or None
                if not args.dry_run:
                    cluster.status = ClusterStatus.MATCHED.value
                    cluster.oax_id = oax_id
                    cluster.tier   = 6
                n_matched += 1
                outcome = f"MATCHED → {oax_id}  orcid={orcid}  name={r['display_name']}"
            elif len(passing) > 1:
                n_ambiguous += 1
                names_same = len({r["display_name"].lower() for r in passing}) == 1
                tag = " [same-name fragment?]" if names_same else ""
                outcome = f"ambiguous ({len(passing)} pass gate of {len(name_ok)} name-ok / {len(results)} AU results){tag}"
                if i % 100 == 0:
                    for r in passing:
                        orcid = (r.get("orcid") or "").replace("https://orcid.org/", "") or None
                        print(f"           {r['id'].split('/')[-1]:<14} "
                              f"works={r.get('works_count'):>4}  cited={r.get('cited_by_count'):>5}  "
                              f"orcid={orcid}  name={r['display_name']}")
            else:
                # Gate fired on none — report which signal failed
                inst_ok  = [r for r in name_ok if _candidate_heps(r, inst_id_to_hep) & set(cluster.institutions)]
                if not inst_ok:
                    n_no_inst += 1
                    outcome = f"no inst match ({len(name_ok)} name-ok / {len(results)} AU results)"
                else:
                    n_no_field += 1
                    outcome = f"no field match ({len(name_ok)} name-ok / {len(results)} AU results, {len(inst_ok)} pass inst)"

            # Store all gate-passing candidates on the cluster for future analysis
            if passing and not args.dry_run:
                cluster.oax_candidates = [
                    {
                        "oax_id":        r["id"].replace("https://openalex.org/", ""),
                        "display_name":  r["display_name"],
                        "orcid":         (r.get("orcid") or "").replace("https://orcid.org/", "") or None,
                        "works_count":   r.get("works_count"),
                        "cited_by_count": r.get("cited_by_count"),
                    }
                    for r in passing
                ]
            # Build ambiguous CSV rows (includes matched for completeness)
            for r in passing:
                orcid_short = (r.get("orcid") or "").replace("https://orcid.org/", "") or None
                names_same  = len({p["display_name"].lower() for p in passing}) == 1 if len(passing) > 1 else None
                ambiguous_rows.append({
                    "cluster_id":     cluster.cluster_id,
                    "arc_name":       name,
                    "heps":           "|".join(cluster.institutions),
                    "for_2d":         "|".join(cluster.for_2d),
                    "oax_id":         r["id"].replace("https://openalex.org/", ""),
                    "oax_name":       r["display_name"],
                    "orcid":          orcid_short,
                    "works_count":    r.get("works_count"),
                    "cited_by_count": r.get("cited_by_count"),
                    "n_passing":      len(passing),
                    "same_name_frag": names_same,
                    "outcome":        "MATCHED" if len(passing) == 1 else "AMBIGUOUS",
                })

        if i % 100 == 0:
            heps = cluster.institutions
            fors = cluster.for_2d
            cert_tag = f"  certain={sorted(cert_heps)}" if cert_heps != set(heps) else ""
            print(f"  [{i:>4}] {name:<30} heps={heps}{cert_tag}  for={fors}")
            print(f"         {outcome}")
        rows.append({"cluster_id": cluster.cluster_id, "name": name, "outcome": outcome})

        if i % 250 == 0:
            print(f"\n  ── progress {i}/{len(targets)} ──  "
                  f"matched={n_matched}  ambiguous={n_ambiguous}  "
                  f"no_inst={n_no_inst}  no_field={n_no_field}  zero={n_zero}")
            if not args.dry_run and not args.limit:
                save_clusters(clusters, path)
                print(f"  checkpoint saved → {path}\n")
            else:
                print()

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"LAYER 5 SUMMARY  {'(DRY RUN — not saved)' if args.dry_run else ''}")
    print(f"{'─'*60}")
    print(f"  Matched (tier 6):        {n_matched:>6,}")
    print(f"  Ambiguous (>1 pass):     {n_ambiguous:>6,}")
    print(f"  No institution match:    {n_no_inst:>6,}")
    print(f"  No field match:          {n_no_field:>6,}")
    print(f"  Zero AU results:         {n_zero:>6,}")

    # Write all gate-passing candidates (matched + ambiguous) for inspection
    if ambiguous_rows:
        detail_path = PROCESSED_DATA / "layer5_candidates.csv"
        df_amb = pd.DataFrame(ambiguous_rows)
        df_amb.to_csv(detail_path, index=False)
        n_with_candidates = df_amb["cluster_id"].nunique()
        print(f"\nAll gate-passing candidates: {detail_path}  ({len(ambiguous_rows)} rows, {n_with_candidates} clusters)")

    if not args.dry_run and not args.limit:
        save_clusters(clusters, path)
        checkpoint = PROCESSED_DATA / "clusters_after_layer5.jsonl"
        save_clusters(clusters, checkpoint)
        print(f"\nSaved: {path}")
        print(f"       {checkpoint}")
    elif not args.dry_run and args.limit:
        print(f"\n(--limit set: changes applied in memory but not saved)")


if __name__ == "__main__":
    main()
