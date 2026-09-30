"""
ERA 2023 journal title → FOR code lookup.

load_era_lookup()  — builds {title_lower: [(for_code, for_name), ...]}
orcid_for_codes()  — extracts FOR from an ORCID /record via journal title matching
"""

from pathlib import Path

import pandas as pd

_ERA_SHEET = "ERA2023 Submission Journal List"
_FOR_COLS  = [("FoR 1", "FoR 1 Name"), ("FoR 2", "FoR 2 Name"), ("FoR 3", "FoR 3 Name")]


def load_era_lookup(data_dir: Path) -> dict[str, list[tuple[str, str]]]:
    """Return {title_lower: [(for_code, for_name), ...]} from ERA 2023 journal list."""
    era = pd.read_excel(data_dir / "ERA 2023 Submission Journal List.xlsx",
                        sheet_name=_ERA_SHEET)
    lookup: dict[str, list[tuple[str, str]]] = {}
    for _, row in era.iterrows():
        title = str(row["Title"]).strip().lower() if pd.notna(row["Title"]) else ""
        if not title or title == "nan":
            continue
        for code_col, name_col in _FOR_COLS:
            code = row.get(code_col)
            name = row.get(name_col)
            if not pd.notna(code) or not pd.notna(name):
                continue
            try:
                code_str = str(int(float(code)))
            except (ValueError, TypeError):
                continue
            entry = (code_str, str(name).strip())
            lookup.setdefault(title, [])
            if entry not in lookup[title]:
                lookup[title].append(entry)
    return lookup


def orcid_for_codes(rec: dict, era_lookup: dict) -> list[dict]:
    """Extract FOR codes from ORCID /record works via ERA journal title matching.

    Returns list of {code, name, count} sorted descending by count.
    Only journal-article work-summaries with a matching journal title are counted.
    """
    counts: dict[str, dict] = {}
    try:
        for group in rec["activities-summary"]["works"]["group"]:
            ws = group.get("work-summary", [{}])[0]
            if ws.get("type") not in ("journal-article", None):
                continue
            jt = (ws.get("journal-title") or {}).get("value")
            if not jt:
                continue
            for code, name in era_lookup.get(jt.strip().lower(), []):
                if code not in counts:
                    counts[code] = {"code": code, "name": name, "count": 0}
                counts[code]["count"] += 1
    except (KeyError, TypeError):
        pass
    return sorted(counts.values(), key=lambda x: -x["count"])
