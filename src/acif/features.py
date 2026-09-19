"""
Feature computation for the cyclic ACIF build -- NOT YET IMPLEMENTED.

This module will hold the single-item (Part 1) and multi-item/relational (Part 2) feature
computation the merge tests in build.py depend on, per
/home/lc/.claude/plans/plan-that-in-tiny-immutable-heron.md. Scaffolding only -- no functions
defined yet, per direct instruction.

Planned contents (see the plan file for full detail):
    - ARC-population-computed FOR-name-signature rarity (AwardCIFItem.for_name_rarity) --
      currently a throwaway local dict inside cluster_items(); needs to become a real,
      reusable table built once over the ARC item population (not OpenAlex-population, which
      serves a different, already-existing rare-name gate elsewhere).
    - single_institution_grant flag + institution_rarity (gated on n_eligible_orgs == 1, i.e.
      single-institution grants -- NOT single-investigator grants; measured coverage 79.2% of
      items).
    - the scheme/year aggregate (full DE-grant_code set + min(funding_commence_year)) that
      feeds a promoted, ACIF-aware version of _scheme_incompat() -- an exact, lossless
      summary for that specific test, needing no separate constituent-level check.
    - FOR-name-signature propensity gate (already built and live in
      src/utils/awards_cif.py::cluster_items() as FOR_NAME_COINCIDENCE_THRESHOLD -- to be
      ported here, not redesigned, once the rest of this module exists).
"""

from __future__ import annotations
