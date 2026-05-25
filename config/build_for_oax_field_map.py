"""
Preparation script — not part of the pipeline.

Reads FOR_2008_2020.xlsx and writes config/for_oax_field_map.csv:
one row per 2-digit FOR division, mapped to the corresponding OAX field.

FOR codes are strings with leading zeros: "01"–"22" (2008), "30"–"52" (2020).
"""

import csv
from pathlib import Path
import pandas as pd

XLSX = Path("/home/lc/m/working/WORKING_ARC_PROJECT/FOR_2008_2020.xlsx")
OUT  = Path(__file__).resolve().parent / "for_oax_field_map.csv"

# 2-digit div_code → (oax_domain, oax_field)
OAX_FIELD = {
    # 2008
    "01": ("Physical Sciences", "Mathematics"),
    "02": ("Physical Sciences", "Physics and Astronomy"),
    "03": ("Physical Sciences", "Chemistry"),
    "04": ("Physical Sciences", "Earth and Planetary Sciences"),
    "05": ("Physical Sciences", "Environmental Science"),
    "06": ("Life Sciences",     "Agricultural and Biological Sciences"),
    "07": ("Life Sciences",     "Agricultural and Biological Sciences"),
    "08": ("Physical Sciences", "Computer Science"),
    "09": ("Physical Sciences", "Engineering"),
    "10": ("Physical Sciences", "Engineering"),
    "11": ("Health Sciences",   "Medicine"),
    "12": ("Social Sciences",   "Arts and Humanities"),
    "13": ("Social Sciences",   "Social Sciences"),
    "14": ("Social Sciences",   "Economics, Econometrics and Finance"),
    "15": ("Social Sciences",   "Business, Management and Accounting"),
    "16": ("Social Sciences",   "Social Sciences"),
    "17": ("Social Sciences",   "Psychology"),
    "18": ("Social Sciences",   "Social Sciences"),
    "19": ("Social Sciences",   "Arts and Humanities"),
    "20": ("Social Sciences",   "Arts and Humanities"),
    "21": ("Social Sciences",   "Arts and Humanities"),
    "22": ("Social Sciences",   "Arts and Humanities"),
    # 2020
    "30": ("Life Sciences",     "Agricultural and Biological Sciences"),
    "31": ("Life Sciences",     "Agricultural and Biological Sciences"),
    "32": ("Health Sciences",   "Medicine"),
    "33": ("Social Sciences",   "Arts and Humanities"),
    "34": ("Physical Sciences", "Chemistry"),
    "35": ("Social Sciences",   "Business, Management and Accounting"),
    "36": ("Social Sciences",   "Arts and Humanities"),
    "37": ("Physical Sciences", "Earth and Planetary Sciences"),
    "38": ("Social Sciences",   "Economics, Econometrics and Finance"),
    "39": ("Social Sciences",   "Social Sciences"),
    "40": ("Physical Sciences", "Engineering"),
    "41": ("Physical Sciences", "Environmental Science"),
    "42": ("Health Sciences",   "Medicine"),
    "43": ("Social Sciences",   "Arts and Humanities"),
    "44": ("Social Sciences",   "Social Sciences"),
    "45": ("Social Sciences",   "Social Sciences"),
    "46": ("Physical Sciences", "Computer Science"),
    "47": ("Social Sciences",   "Arts and Humanities"),
    "48": ("Social Sciences",   "Social Sciences"),
    "49": ("Physical Sciences", "Mathematics"),
    "50": ("Social Sciences",   "Arts and Humanities"),
    "51": ("Physical Sciences", "Physics and Astronomy"),
    "52": ("Social Sciences",   "Psychology"),
}


def main():
    df08 = pd.read_excel(XLSX, sheet_name="2008", dtype=str)
    rows_2008 = [
        {
            "for_series": "2008",
            "div_code":   str(r["forCode"]).strip().zfill(2),
            "div_name":   str(r["forName"]).strip(),
        }
        for _, r in df08.iterrows()
        if len(str(r["forCode"]).strip()) <= 2
    ]

    # 2020: division rows have DIVISION not null
    df20 = pd.read_excel(XLSX, sheet_name="2020", dtype=str)
    rows_2020 = []
    for _, r in df20.iterrows():
        div_raw = str(r["DIVISION"]).strip()
        if div_raw in ("", "nan"):
            continue
        rows_2020.append({
            "for_series": "2020",
            "div_code":   str(int(float(div_raw))).zfill(2),
            "div_name":   str(r["MIXED"]).strip(),
        })

    rows = []
    for r in rows_2008 + rows_2020:
        domain, field = OAX_FIELD.get(r["div_code"], ("", ""))
        rows.append({**r, "oax_domain": domain, "oax_field": field})

    with open(OUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "for_series", "div_code", "div_name", "oax_domain", "oax_field",
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows → {OUT}")
    for r in rows:
        print(f"  {r['for_series']}  {r['div_code']}  {r['div_name']:<55}→  {r['oax_field']}")


if __name__ == "__main__":
    main()
