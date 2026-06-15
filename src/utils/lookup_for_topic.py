"""
Bi-directional lookup between ANZSRC FOR codes and OpenAlex taxonomy.

Mapping levels (the only valid pairings):
  ANZSRC division (2-digit) ↔ OAX field
  ANZSRC group    (4-digit) ↔ OAX subfield

Navigation between levels goes via the shared field_id:
  subfield → division:  subfield row has oax_field_id → field_to_divs()
  group → field:        group row has oax_field_id (same as its division's)

Cross-level lookups are forbidden because they conflate granularity:
  division → subfield  (general → particular, no canonical answer)
  group → field        (particular → general, loses information)
Calling either raises CrossLevelError.
"""

import csv
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_PROJECT = _HERE.parents[1]
_DEFAULT_FIELD_CSV      = _PROJECT / "config" / "for_oax_field_map.csv"
_DEFAULT_SUBFIELD_CSV   = _PROJECT / "config" / "for_oax_subfield_map.csv"
_DEFAULT_CONV_CSV       = _PROJECT / "config" / "for_2008_to_2020.csv"


class CrossLevelError(ValueError):
    """Raised when a lookup crosses taxonomy levels (general↔particular)."""


class ForTopicLookup:
    """
    Load the concordance CSVs and expose typed lookups in both directions.

    Parameters
    ----------
    field_csv : path to for_oax_field_map.csv
    subfield_csv : path to for_oax_subfield_map.csv
    conv_csv : path to for_2008_to_2020.csv
    for_series : restrict division lookups to this series ("2008", "2020", or
                 None for all).  Group lookups are always 2020 only.
    """

    def __init__(self, field_csv=None, subfield_csv=None, conv_csv=None, for_series=None):
        field_csv    = Path(field_csv)    if field_csv    else _DEFAULT_FIELD_CSV
        subfield_csv = Path(subfield_csv) if subfield_csv else _DEFAULT_SUBFIELD_CSV
        conv_csv     = Path(conv_csv)     if conv_csv     else _DEFAULT_CONV_CSV

        with open(field_csv, newline="") as f:
            all_divs = list(csv.DictReader(f))

        self._divs = [
            r for r in all_divs
            if for_series is None or r["for_series"] == for_series
        ]

        with open(subfield_csv, newline="") as f:
            self._groups = list(csv.DictReader(f))

        with open(conv_csv, newline="") as f:
            conv_rows = list(csv.DictReader(f))
        # {2008_group_code: 2020_group_code}
        self._conv_2008_to_2020 = {r["for_2008_code"]: r["for_2020_code"] for r in conv_rows}

        # indices
        self._div_by_code     = {r["div_code"]: r for r in self._divs}
        self._groups_by_code  = {r["group_code"]: r for r in self._groups}

    # ------------------------------------------------------------------
    # Division ↔ OAX field
    # ------------------------------------------------------------------

    def div_to_field(self, div_code: str) -> dict | None:
        """
        Return the OAX field row for an ANZSRC division code, or None.

        Keys: for_series, div_code, div_name,
              oax_domain, oax_domain_id, oax_field, oax_field_id
        """
        return self._div_by_code.get(str(div_code).zfill(2))

    def field_to_divs(self, oax_field_id: str) -> list[dict]:
        """Return all ANZSRC division rows mapping to an OAX field_id."""
        return [r for r in self._divs if r["oax_field_id"] == str(oax_field_id)]

    # ------------------------------------------------------------------
    # Group ↔ OAX subfield
    # ------------------------------------------------------------------

    def group_to_subfield(self, group_code: str) -> dict | None:
        """
        Return the OAX subfield row for an ANZSRC 2020 group code, or None.

        Keys: group_code, group_name, div_code, div_name,
              oax_subfield_id, oax_subfield_name,
              oax_field_id, oax_field_name,
              oax_domain_id, oax_domain_name
        """
        return self._groups_by_code.get(str(group_code))

    def subfield_to_groups(self, oax_subfield_id: str) -> list[dict]:
        """Return all ANZSRC group rows mapping to an OAX subfield_id."""
        return [r for r in self._groups if r["oax_subfield_id"] == str(oax_subfield_id)]

    # ------------------------------------------------------------------
    # Convenience: all loaded rows
    # ------------------------------------------------------------------

    def all_divisions(self) -> list[dict]:
        return list(self._divs)

    def all_groups(self) -> list[dict]:
        return list(self._groups)

    # ------------------------------------------------------------------
    # 2008 → 2020 FOR code upgrade
    # ------------------------------------------------------------------

    def upgrade_for_code(self, code: str) -> str | None:
        """
        Convert a FOR code to its ANZSRC 2020 4-digit group equivalent.

        ANZSRC 2020 group codes have first two digits in range 30–52.
        ANZSRC 2008 group codes have first two digits in range 01–22.

        - 2020 code: returned as-is.
        - 2008 code: looked up in for_2008_to_2020.csv; None if no 4-digit
          2020 group mapping exists (e.g. some codes only map to a 2020 division).
        - Anything else (unknown range, non-numeric): returns None.
        """
        code = str(code).strip()
        if len(code) != 4 or not code.isdigit():
            return None
        prefix = code[:2]
        if "30" <= prefix <= "52":
            return code
        if "01" <= prefix <= "22":
            return self._conv_2008_to_2020.get(code)
        return None

    # ------------------------------------------------------------------
    # Forbidden cross-level lookups
    # ------------------------------------------------------------------

    def div_to_subfield(self, div_code: str):
        """Forbidden: division is more general than subfield."""
        raise CrossLevelError(
            f"div_to_subfield({div_code!r}) is not allowed: a division maps to "
            "an OAX *field*, not a subfield.  Use div_to_field() then "
            "subfield_to_groups() to navigate down."
        )

    def group_to_field(self, group_code: str):
        """Forbidden: group already implies a field; use group_to_subfield()."""
        raise CrossLevelError(
            f"group_to_field({group_code!r}) is not allowed: a group maps to "
            "an OAX *subfield*, not a field.  The field is available as "
            "oax_field_id on the row returned by group_to_subfield()."
        )
