"""
src/utils/orcid_processor.py

OrcidProcessor -- consolidates ORCID processing that was scattered across
00b_enrich_orcid.py, orcid_bulk_lookup.py, and ad hoc scratch scripts during the 2026-09-01
NO_ORCID-population investigation (see docs/pipeline_todo.md #19 for the full incident this
replaces, and the "oeuvre QA: cleaning order" plan's item 12).

Two jobs, cleanly separated as methods on one class, both local-data-only -- no live ORCID
API calls (2026-09-01 project direction: "Ensure that the API and its cache are not accessed
again. use orcid.db."):

  discover()           -- PRE-LINK: find a corroborated ORCID for an ARC cluster that has
                           none, using the broad orcid.db population (17.15M people, one name,
                           one current org via `ror`, no dated history) for name+institution
                           matching, falling back to the narrower orcid_persons.parquet/
                           orcid_affiliations.parquet HQ subset (~4.8M people, full aliases +
                           DATED employment/education history) only to corroborate an
                           institution-ambiguous tie by ORCID (the Simon Kelly / Brian Wilson
                           pattern from that session: two institution-matched candidates, only
                           one has a role at the wanted institution in their real dated history).

  collapse_candidates() -- POST-LINK: an ACIF already resolved via ORCID (whether through
                           04_resolve_links.py's own `orcid` step or trivially because there
                           was only one high-confidence candidate to begin with) often still
                           carries its full, unpruned oax_candidates pool. Given the ACIF's
                           recorded orcid and a author_idx -> orcid lookup for its candidate
                           pool, collapse to the single ORCID-confirmed winner when there is
                           exactly one -- never silently resolves a genuine conflict (0 or 2+
                           matches leaves the pool untouched).

Both broad-population methods are conservative by construction, matching every other
local-ORCID lookup in this project: no ranking, no "closest match" -- a candidate set only
resolves when it narrows to exactly one person, otherwise it's left for review.
"""
import duckdb

from src.utils.orcid_bulk_lookup import _name_matches

ORCID_DB = "/home/lc/s/orcid/orcid.db"
HQ_AFFIL = "/home/lc/s/orcid/orcid_affiliations.parquet"


class OrcidProcessor:
    def __init__(self, con: duckdb.DuckDBPyConnection | None = None):
        self._owns_con = con is None
        self.con = con or duckdb.connect()
        if self._owns_con:
            self.con.execute("INSTALL sqlite; LOAD sqlite;")
        self.con.execute(f"ATTACH IF NOT EXISTS '{ORCID_DB}' AS odb (TYPE sqlite);")

    def close(self) -> None:
        if self._owns_con:
            self.con.close()

    def __enter__(self) -> "OrcidProcessor":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Pre-link discovery
    # ------------------------------------------------------------------

    def discover(self, first_name: str, family_name: str,
                 institution_names: list[str] | None = None) -> dict | None:
        """Return {"orcid", "confidence", "source"} for a corroborated match, or None."""
        institution_names = institution_names or []
        wanted = {i.lower() for i in institution_names}

        candidates = self._broad_name_match(first_name, family_name)
        source = "orcid_processor_name"
        if not candidates and institution_names:
            initial = first_name[:1] if first_name else ""
            candidates = self._broad_institution_match(family_name, initial, institution_names)
            source = "orcid_processor_bare_initial"
        if not candidates:
            return None

        inst_matched = [c for c in candidates if c["org_name"] and c["org_name"].lower() in wanted]

        if len(inst_matched) == 1:
            return {"orcid": inst_matched[0]["orcid"], "confidence": "au_match",
                    "source": f"{source}_institution"}
        if len(inst_matched) > 1:
            resolved = self._corroborate_by_employment(inst_matched, wanted)
            if resolved:
                return {"orcid": resolved, "confidence": "au_match",
                        "source": f"{source}_employment"}
            return None  # still ambiguous -- defer, don't guess

        if len(candidates) == 1:
            return {"orcid": candidates[0]["orcid"], "confidence": "high", "source": source}
        return None  # 2+ candidates, no institution corroboration -- defer

    def _broad_name_match(self, first_name: str, family_name: str) -> list[dict]:
        full = f"{first_name} {family_name}"
        rows = self.con.execute(
            """
            SELECT p.orcid, p.name, o.name AS org_name
            FROM odb.person p
            LEFT JOIN odb.organization o ON p.ror = o.ror
            WHERE lower(p.name) = lower(?)
            """,
            [full],
        ).fetchall()
        return [{"orcid": r[0], "name": r[1], "org_name": r[2]} for r in rows]

    def _broad_institution_match(self, family_name: str, first_initial: str,
                                  institution_names: list[str]) -> list[dict]:
        wanted = [i.lower() for i in institution_names]
        rows = self.con.execute(
            """
            SELECT DISTINCT p.orcid, p.name, o.name AS org_name
            FROM odb.person p
            JOIN odb.organization o ON p.ror = o.ror
            WHERE lower(o.name) = ANY(?)
            """,
            [wanted],
        ).fetchall()
        fam_tokens = family_name.lower().replace("-", " ").split()
        init = first_initial.lower()
        return [
            {"orcid": orcid, "name": name, "org_name": org_name}
            for orcid, name, org_name in rows
            if name and _name_matches(name, fam_tokens, init)
        ]

    def _corroborate_by_employment(self, candidates: list[dict], wanted: set[str]) -> str | None:
        """HQ source's dated employment history, keyed by ORCID -- the check that turned
        "ambiguous" into "clearly this one" for Simon Kelly and Brian Wilson (2026-09-01):
        orcid.db's own `ror` only ever gives one current/primary org, which can miss an
        institution a person held in the past (or never got recorded in orcid.db at all)."""
        hits = []
        for c in candidates:
            rows = self.con.execute(
                "SELECT org_name FROM read_parquet(?) WHERE orcid = ?",
                [HQ_AFFIL, c["orcid"]],
            ).fetchall()
            if any(r[0] and r[0].lower() in wanted for r in rows):
                hits.append(c["orcid"])
        return hits[0] if len(hits) == 1 else None

    # ------------------------------------------------------------------
    # Post-link cleanup
    # ------------------------------------------------------------------

    @staticmethod
    def collapse_candidates(acif_orcid: str | None, oax_candidates: list[str],
                             oax_orcid_by_id: dict[str, str | None]) -> list[str]:
        """Collapse oax_candidates to the single ORCID-confirmed winner when the ACIF's own
        recorded orcid matches exactly one candidate's own OpenAlex orcid field. Returns
        oax_candidates unchanged if there's no ACIF orcid, no match, or 2+ matches (a real
        conflict to flag for review, never something to silently resolve here)."""
        if not acif_orcid:
            return oax_candidates
        matches = [c for c in oax_candidates if oax_orcid_by_id.get(c) == acif_orcid]
        if len(matches) == 1:
            return matches
        return oax_candidates
