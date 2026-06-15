"""
Preparation script — not part of the pipeline.

Produces two output files in config/:

  for_oax_field_map.csv
      One row per 2-digit FOR division (both 2008 and 2020 series), mapped to
      the corresponding OAX domain + field.  Columns include oax_field_id and
      oax_domain_id to support bi-directional lookup.

  for_oax_subfield_map.csv
      One row per 4-digit ANZSRC 2020 group, mapped to an OAX subfield.
      Every subfield MUST belong to the OAX field that the parent division maps
      to — no cross-level jumps.  The script raises ValueError and stops if any
      mapping violates this constraint.

Taxonomy levels:
  ANZSRC division (2-digit) ↔ OAX field
  ANZSRC group    (4-digit) ↔ OAX subfield

Cross-level lookups (division→subfield, group→field) are meaningless and
are rejected by lookup_for_topic.ForTopicLookup.
"""

import csv
import sys
from pathlib import Path
import pandas as pd

XLSX = Path("/home/lc/m/working/WORKING_ARC_PROJECT/FOR_2008_2020.xlsx")

DATA_DIR           = Path(__file__).resolve().parent.parent / "data"
ANZSRC_2020_XLSX   = DATA_DIR / "anzsrc2020_246_digit.xlsx"
OAX_TOPICS_XLSX    = DATA_DIR / "OpenAlex_topic_mapping_table.xlsx"
FOR_CONV_XLSX      = DATA_DIR / "2008_FoR_to_2020_FoR_conversion_04Apr2022.xlsx"

OUT_FIELD       = Path(__file__).resolve().parent / "for_oax_field_map.csv"
OUT_SUBFIELD    = Path(__file__).resolve().parent / "for_oax_subfield_map.csv"
OUT_2008_TO_2020 = Path(__file__).resolve().parent / "for_2008_to_2020.csv"


# ---------------------------------------------------------------------------
# Division → OAX field  (div_code → (domain_name, field_name))
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Group → OAX subfield  (group_code → oax_subfield_id)
#
# CONSTRAINT: every subfield_id must start with the field_id of the parent
# division.  E.g. Div 30 → field 11 (AgBio), so all 30xx groups must map to
# subfields 11xx.  The script enforces this at runtime.
#
# Where no precise subfield exists within the constrained field the group is
# assigned to the General/broadest subfield in that field.
# ---------------------------------------------------------------------------

OAX_SUBFIELD = {
    # --- Div 30  AGRICULTURAL, VETERINARY AND FOOD SCIENCES  → field 11 ---
    "3001": "1100",  # Agricultural biotechnology  (no Biotechnology in AgBio subfields)
    "3002": "1102",  # Agriculture, land and farm management → Agronomy and Crop Science
    "3003": "1103",  # Animal production → Animal Science and Zoology
    "3004": "1102",  # Crop and pasture production → Agronomy and Crop Science
    "3005": "1104",  # Fisheries sciences → Aquatic Science
    "3006": "1106",  # Food sciences → Food Science
    "3007": "1107",  # Forestry sciences → Forestry
    "3008": "1108",  # Horticultural production → Horticulture
    "3009": "1103",  # Veterinary sciences → Animal Science and Zoology
    "3099": "1100",  # Other → General Agricultural and Biological Sciences

    # --- Div 31  BIOLOGICAL SCIENCES  → field 11 ---
    # Many groups (Biochemistry, Genetics, Microbiology) have closer homes in
    # OAX field 13 or 24, but are constrained to field 11 by the div mapping.
    "3101": "1100",  # Biochemistry and cell biology → General (no Biochemistry in AgBio)
    "3102": "1100",  # Bioinformatics and computational biology → General
    "3103": "1105",  # Ecology → Ecology, Evolution, Behavior and Systematics
    "3104": "1105",  # Evolutionary biology → Ecology, Evolution, Behavior and Systematics
    "3105": "1100",  # Genetics → General (no Genetics in AgBio subfields)
    "3106": "1100",  # Industrial biotechnology → General
    "3107": "1100",  # Microbiology → General (no Microbiology in AgBio subfields)
    "3108": "1110",  # Plant biology → Plant Science
    "3109": "1103",  # Zoology → Animal Science and Zoology
    "3199": "1100",  # Other → General

    # --- Div 32  BIOMEDICAL AND CLINICAL SCIENCES  → field 27 (Medicine) ---
    "3201": "2705",  # Cardiovascular medicine and haematology → Cardiology and Cardiovascular Medicine
    "3202": "2724",  # Clinical sciences → Internal Medicine
    "3203": "2724",  # Dentistry → Internal Medicine (Dentistry is OAX field 35, constrained to 27)
    "3204": "2723",  # Immunology → Immunology and Allergy
    "3205": "2704",  # Medical biochemistry and metabolomics → Biochemistry
    "3206": "2716",  # Medical biotechnology → Genetics
    "3207": "2726",  # Medical microbiology → Microbiology
    "3208": "2737",  # Medical physiology → Physiology
    "3209": "2728",  # Neurosciences → Neurology
    "3210": "2712",  # Nutrition and dietetics → Endocrinology, Diabetes and Metabolism
    "3211": "2730",  # Oncology and carcinogenesis → Oncology
    "3212": "2731",  # Ophthalmology and optometry → Ophthalmology
    "3213": "2735",  # Paediatrics → Pediatrics, Perinatology and Child Health
    "3214": "2736",  # Pharmacology and pharmaceutical sciences → Pharmacology
    "3215": "2743",  # Reproductive medicine → Reproductive Medicine
    "3299": "2724",  # Other → Internal Medicine

    # --- Div 33  BUILT ENVIRONMENT AND DESIGN  → field 12 (Arts and Humanities) ---
    # Architecture/Building have closer homes in OAX Engineering, but the
    # division maps to Arts and Humanities.
    "3301": "1200",  # Architecture → General Arts and Humanities
    "3302": "1200",  # Building → General Arts and Humanities
    "3303": "1213",  # Design → Visual Arts and Performing Arts
    "3304": "1200",  # Urban and regional planning → General Arts and Humanities
    "3399": "1200",  # Other → General Arts and Humanities

    # --- Div 34  CHEMICAL SCIENCES  → field 16 (Chemistry) ---
    "3401": "1602",  # Analytical chemistry → Analytical Chemistry
    "3402": "1604",  # Inorganic chemistry → Inorganic Chemistry
    "3403": "1605",  # Macromolecular and materials chemistry → Organic Chemistry
    "3404": "1605",  # Medicinal and biomolecular chemistry → Organic Chemistry
    "3405": "1605",  # Organic chemistry → Organic Chemistry
    "3406": "1606",  # Physical chemistry → Physical and Theoretical Chemistry
    "3407": "1606",  # Theoretical and computational chemistry → Physical and Theoretical Chemistry
    "3499": "1606",  # Other → Physical and Theoretical Chemistry

    # --- Div 35  COMMERCE, MANAGEMENT, TOURISM AND SERVICES  → field 14 ---
    "3501": "1402",  # Accounting, auditing and accountability → Accounting
    "3502": "1403",  # Banking, finance and investment → Business and International Management
    "3503": "1403",  # Business systems in context → Business and International Management
    "3504": "1403",  # Commercial services → Business and International Management
    "3505": "1407",  # Human resources and industrial relations → Org Behavior and HRM
    "3506": "1406",  # Marketing → Marketing
    "3507": "1408",  # Strategy, management and organisational behaviour → Strategy and Management
    "3508": "1409",  # Tourism → Tourism, Leisure and Hospitality Management
    "3509": "1403",  # Transportation, logistics and supply chains → Business and International Management
    "3599": "1403",  # Other → Business and International Management

    # --- Div 36  CREATIVE ARTS AND WRITING  → field 12 (Arts and Humanities) ---
    "3601": "1213",  # Art history, theory and criticism → Visual Arts and Performing Arts
    "3602": "1208",  # Creative and professional writing → Literature and Literary Theory
    "3603": "1210",  # Music → Music
    "3604": "1213",  # Performing arts → Visual Arts and Performing Arts
    "3605": "1213",  # Screen and digital media → Visual Arts and Performing Arts
    "3606": "1213",  # Visual arts → Visual Arts and Performing Arts
    "3699": "1200",  # Other → General Arts and Humanities

    # --- Div 37  EARTH SCIENCES  → field 19 (Earth and Planetary Sciences) ---
    "3701": "1902",  # Atmospheric sciences → Atmospheric Science
    "3702": "1902",  # Climate change science → Atmospheric Science
    "3703": "1906",  # Geochemistry → Geochemistry and Petrology
    "3704": "1907",  # Geoinformatics → Geology
    "3705": "1907",  # Geology → Geology
    "3706": "1908",  # Geophysics → Geophysics
    "3707": "1904",  # Hydrology → Earth-Surface Processes
    "3708": "1910",  # Oceanography → Oceanography
    "3709": "1904",  # Physical geography and environmental geoscience → Earth-Surface Processes
    "3799": "1907",  # Other → Geology

    # --- Div 38  ECONOMICS  → field 20 (Economics, Econometrics and Finance) ---
    "3801": "2002",  # Applied economics → Economics and Econometrics
    "3802": "2002",  # Econometrics → Economics and Econometrics
    "3803": "2002",  # Economic theory → Economics and Econometrics
    "3899": "2000",  # Other → General Economics, Econometrics and Finance

    # --- Div 39  EDUCATION  → field 33 (Social Sciences) ---
    "3901": "3304",  # Curriculum and pedagogy → Education
    "3902": "3304",  # Education policy, sociology and philosophy → Education
    "3903": "3304",  # Education systems → Education
    "3904": "3304",  # Specialist studies in education → Education
    "3999": "3304",  # Other → Education

    # --- Div 40  ENGINEERING  → field 22 (Engineering) ---
    "4001": "2202",  # Aerospace engineering → Aerospace Engineering
    "4002": "2203",  # Automotive engineering → Automotive Engineering
    "4003": "2204",  # Biomedical engineering → Biomedical Engineering
    "4004": "2200",  # Chemical engineering → General Engineering (OAX ChemEng is field 15)
    "4005": "2205",  # Civil engineering → Civil and Structural Engineering
    "4006": "2208",  # Communications engineering → Electrical and Electronic Engineering
    "4007": "2207",  # Control engineering, mechatronics and robotics → Control and Systems Engineering
    "4008": "2208",  # Electrical engineering → Electrical and Electronic Engineering
    "4009": "2208",  # Electronics, sensors and digital hardware → Electrical and Electronic Engineering
    "4010": "2200",  # Engineering practice and education → General Engineering
    "4011": "2200",  # Environmental engineering → General Engineering (Env Eng is field 23)
    "4012": "2210",  # Fluid mechanics and thermal engineering → Mechanical Engineering
    "4013": "2205",  # Geomatic engineering → Civil and Structural Engineering
    "4014": "2209",  # Manufacturing engineering → Industrial and Manufacturing Engineering
    "4015": "2212",  # Maritime engineering → Ocean Engineering
    "4016": "2200",  # Materials engineering → General Engineering (Materials Sci is field 25)
    "4017": "2210",  # Mechanical engineering → Mechanical Engineering
    "4018": "2200",  # Nanotechnology → General Engineering
    "4019": "2200",  # Resources engineering and extractive metallurgy → General Engineering
    "4099": "2200",  # Other → General Engineering

    # --- Div 41  ENVIRONMENTAL SCIENCES  → field 23 (Environmental Science) ---
    "4101": "2306",  # Climate change impacts and adaptation → Global and Planetary Change
    "4102": "2303",  # Ecological applications → Ecology
    "4103": "2304",  # Environmental biotechnology → Environmental Chemistry
    "4104": "2308",  # Environmental management → Management, Monitoring, Policy and Law
    "4105": "2310",  # Pollution and contamination → Pollution
    "4106": "2304",  # Soil sciences → Environmental Chemistry (Soil Sci is AgBio field 11)
    "4199": "2303",  # Other → Ecology

    # --- Div 42  HEALTH SCIENCES  → field 27 (Medicine) ---
    # Nursing (field 29) and Health Professions (field 36) are constrained to
    # Medicine subfields because the division maps to Medicine.
    "4201": "2742",  # Allied health and rehabilitation science → Rehabilitation
    "4202": "2713",  # Epidemiology → Epidemiology
    "4203": "2739",  # Health services and systems → Public Health
    "4204": "2729",  # Midwifery → Obstetrics and Gynecology
    "4205": "2724",  # Nursing → Internal Medicine
    "4206": "2739",  # Public health → Public Health
    "4207": "2732",  # Sports science and exercise → Orthopedics and Sports Medicine
    "4208": "2707",  # Traditional, complementary and integrative medicine → Complementary medicine
    "4299": "2724",  # Other → Internal Medicine

    # --- Div 43  HISTORY, HERITAGE AND ARCHAEOLOGY  → field 12 (Arts and Humanities) ---
    "4301": "1204",  # Archaeology → Archeology
    "4302": "1209",  # Heritage, archive and museum studies → Museology
    "4303": "1202",  # Historical studies → History
    "4399": "1200",  # Other → General Arts and Humanities

    # --- Div 44  HUMAN SOCIETY  → field 33 (Social Sciences) ---
    "4401": "3314",  # Anthropology → Anthropology
    "4402": "3312",  # Criminology → Sociology and Political Science
    "4403": "3317",  # Demography → Demography
    "4404": "3303",  # Development studies → Development
    "4405": "3318",  # Gender studies → Gender Studies
    "4406": "3305",  # Human geography → Geography, Planning and Development
    "4407": "3321",  # Policy and administration → Public Administration
    "4408": "3320",  # Political science → Political Science and International Relations
    "4409": "3300",  # Social work → General Social Sciences
    "4410": "3312",  # Sociology → Sociology and Political Science
    "4499": "3300",  # Other → General Social Sciences

    # --- Div 45  INDIGENOUS STUDIES  → field 33 (Social Sciences) ---
    "4501": "3316",  # Aboriginal and TSI culture, language and history → Cultural Studies
    "4502": "3304",  # Aboriginal and TSI education → Education
    "4503": "3305",  # Aboriginal and TSI environmental knowledges → Geography, Planning and Development
    "4504": "3306",  # Aboriginal and TSI health and wellbeing → Health
    "4505": "3312",  # Aboriginal and TSI peoples, society and community → Sociology and Political Science
    "4506": "3300",  # Aboriginal and TSI sciences → General Social Sciences
    "4507": "3316",  # Māori culture, language and history → Cultural Studies
    "4508": "3304",  # Mātauranga Māori (education) → Education
    "4509": "3305",  # Māori environmental knowledges → Geography, Planning and Development
    "4510": "3306",  # Māori health and wellbeing → Health
    "4511": "3312",  # Māori peoples, society and community → Sociology and Political Science
    "4512": "3300",  # Māori sciences → General Social Sciences
    "4513": "3316",  # Pacific Peoples culture, language and history → Cultural Studies
    "4514": "3304",  # Pacific Peoples education → Education
    "4515": "3305",  # Pacific Peoples environmental knowledges → Geography, Planning and Development
    "4516": "3306",  # Pacific Peoples health and wellbeing → Health
    "4517": "3300",  # Pacific Peoples sciences → General Social Sciences
    "4518": "3312",  # Pacific Peoples society and community → Sociology and Political Science
    "4519": "3300",  # Other Indigenous data, methodologies → General Social Sciences
    "4599": "3300",  # Other → General Social Sciences

    # --- Div 46  INFORMATION AND COMPUTING SCIENCES  → field 17 (Computer Science) ---
    "4601": "1706",  # Applied computing → Computer Science Applications
    "4602": "1702",  # Artificial intelligence → Artificial Intelligence
    "4603": "1707",  # Computer vision and multimedia computation → Computer Vision and Pattern Recognition
    "4604": "1705",  # Cybersecurity and privacy → Computer Networks and Communications
    "4605": "1710",  # Data management and data science → Information Systems
    "4606": "1712",  # Distributed computing and systems software → Software
    "4607": "1704",  # Graphics, augmented reality and games → Computer Graphics and Computer-Aided Design
    "4608": "1709",  # Human-centred computing → Human-Computer Interaction
    "4609": "1710",  # Information systems → Information Systems
    "4610": "1706",  # Library and information studies → Computer Science Applications
    "4611": "1702",  # Machine learning → Artificial Intelligence
    "4612": "1712",  # Software engineering → Software
    "4613": "1703",  # Theory of computation → Computational Theory and Mathematics
    "4699": "1706",  # Other → Computer Science Applications

    # --- Div 47  LANGUAGE, COMMUNICATION AND CULTURE  → field 12 (Arts and Humanities) ---
    # Communication (3315) and Cultural Studies (3316) are OAX Social Sciences
    # subfields; constrained to Arts and Humanities here.
    "4701": "1200",  # Communication and media studies → General Arts and Humanities
    "4702": "1200",  # Cultural studies → General Arts and Humanities
    "4703": "1203",  # Language studies → Language and Linguistics
    "4704": "1203",  # Linguistics → Language and Linguistics
    "4705": "1208",  # Literary studies → Literature and Literary Theory
    "4799": "1200",  # Other → General Arts and Humanities

    # --- Div 48  LAW AND LEGAL STUDIES  → field 33 (Social Sciences) ---
    "4801": "3308",  # Commercial law → Law
    "4802": "3308",  # Environmental and resources law → Law
    "4803": "3308",  # International and comparative law → Law
    "4804": "3308",  # Law in context → Law
    "4805": "3308",  # Legal systems → Law
    "4806": "3308",  # Private law and civil obligations → Law
    "4807": "3308",  # Public law → Law
    "4899": "3308",  # Other → Law

    # --- Div 49  MATHEMATICAL SCIENCES  → field 26 (Mathematics) ---
    "4901": "2604",  # Applied mathematics → Applied Mathematics
    "4902": "2610",  # Mathematical physics → Mathematical Physics
    "4903": "2605",  # Numerical and computational mathematics → Computational Mathematics
    "4904": "2602",  # Pure mathematics → Algebra and Number Theory
    "4905": "2613",  # Statistics → Statistics and Probability
    "4999": "2604",  # Other → Applied Mathematics

    # --- Div 50  PHILOSOPHY AND RELIGIOUS STUDIES  → field 12 (Arts and Humanities) ---
    "5001": "1211",  # Applied ethics → Philosophy
    "5002": "1207",  # History and philosophy of specific fields → History and Philosophy of Science
    "5003": "1211",  # Philosophy → Philosophy
    "5004": "1212",  # Religious studies → Religious studies
    "5005": "1212",  # Theology → Religious studies
    "5099": "1200",  # Other → General Arts and Humanities

    # --- Div 51  PHYSICAL SCIENCES  → field 31 (Physics and Astronomy) ---
    "5101": "3103",  # Astronomical sciences → Astronomy and Astrophysics
    "5102": "3107",  # Atomic, molecular and optical physics → Atomic and Molecular Physics, and Optics
    "5103": "3109",  # Classical physics → Statistical and Nonlinear Physics
    "5104": "3104",  # Condensed matter physics → Condensed Matter Physics
    "5105": "3108",  # Medical and biological physics → Radiation
    "5106": "3106",  # Nuclear and plasma physics → Nuclear and High Energy Physics
    "5107": "3106",  # Particle and high energy physics → Nuclear and High Energy Physics
    "5108": "3104",  # Quantum physics → Condensed Matter Physics
    "5109": "3103",  # Space sciences → Astronomy and Astrophysics
    "5110": "3105",  # Synchrotrons and accelerators → Instrumentation
    "5199": "3102",  # Other → Acoustics and Ultrasonics

    # --- Div 52  PSYCHOLOGY  → field 32 (Psychology) ---
    "5201": "3202",  # Applied and developmental psychology → Applied Psychology
    "5202": "3206",  # Biological psychology → Neuropsychology and Physiological Psychology
    "5203": "3203",  # Clinical and health psychology → Clinical Psychology
    "5204": "3205",  # Cognitive and computational psychology → Experimental and Cognitive Psychology
    "5205": "3207",  # Social and personality psychology → Social Psychology
    "5299": "3200",  # Other → General Psychology
}


# ---------------------------------------------------------------------------
# Division-level CSV
# ---------------------------------------------------------------------------

def _load_oax_field_lookup():
    """Return {field_name: (field_id, domain_id)} from OAX topics file."""
    oax = pd.read_excel(OAX_TOPICS_XLSX, dtype=str)
    return (
        oax.drop_duplicates("field_id")
        .set_index("field_name")[["field_id", "domain_id"]]
        .to_dict("index")
    )


def main():
    oax_field_lookup = _load_oax_field_lookup()

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
        fi = oax_field_lookup.get(field, {})
        rows.append({
            **r,
            "oax_domain":    domain,
            "oax_domain_id": fi.get("domain_id", ""),
            "oax_field":     field,
            "oax_field_id":  fi.get("field_id", ""),
        })

    with open(OUT_FIELD, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "for_series", "div_code", "div_name",
            "oax_domain", "oax_domain_id", "oax_field", "oax_field_id",
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows → {OUT_FIELD}")
    for r in rows:
        print(
            f"  {r['for_series']}  {r['div_code']}  {r['div_name']:<55}"
            f"→  field {r['oax_field_id']:>2}  {r['oax_field']}"
        )


# ---------------------------------------------------------------------------
# Group-level CSV
# ---------------------------------------------------------------------------

def _validate_hierarchy(groups, subfield_info, oax_field_lookup):
    """
    Raise ValueError if any group maps to a subfield outside its division's
    OAX field.  Each subfield_id's leading two digits equal its field_id.
    """
    errors = []
    for g in groups:
        sfid = OAX_SUBFIELD.get(g["group_code"])
        if sfid is None:
            continue
        div_code = g["group_code"][:2]
        _, field_name = OAX_FIELD.get(div_code, ("", ""))
        expected_field_id = oax_field_lookup.get(field_name, {}).get("field_id", "")
        actual_field_id   = sfid[:2]
        if actual_field_id != expected_field_id:
            sf_name = subfield_info.get(sfid, {}).get("subfield_name", "?")
            errors.append(
                f"  group {g['group_code']} ({g['group_name']}): "
                f"subfield {sfid} ({sf_name}) is in OAX field {actual_field_id}, "
                f"but div {div_code} maps to OAX field {expected_field_id} ({field_name})"
            )
    if errors:
        raise ValueError(
            "Hierarchy violation — group→subfield crosses OAX field boundary "
            "(general→particular mapping has no meaning):\n" + "\n".join(errors)
        )


def main_subfield():
    # Parse ANZSRC 2020 groups from Table 2
    df = pd.read_excel(ANZSRC_2020_XLSX, sheet_name="Table 2", dtype=str, header=None)
    groups = []
    cur_div_code, cur_div_name = None, None
    for _, r in df.iterrows():
        c0 = str(r[0]).strip() if pd.notna(r[0]) else ""
        c1 = str(r[1]).strip() if pd.notna(r[1]) else ""
        c2 = str(r[2]).strip() if pd.notna(r[2]) else ""
        if c0.isdigit() and len(c0) == 2:
            cur_div_code, cur_div_name = c0, c1
        elif c0 == "" and c1.isdigit() and len(c1) == 4 and cur_div_code:
            groups.append({
                "div_code":   cur_div_code,
                "div_name":   cur_div_name,
                "group_code": c1,
                "group_name": c2,
            })

    oax = pd.read_excel(OAX_TOPICS_XLSX, dtype=str)
    subfield_info = (
        oax.drop_duplicates("subfield_id")
        .set_index("subfield_id")[
            ["subfield_name", "field_id", "field_name", "domain_id", "domain_name"]
        ]
        .to_dict("index")
    )
    oax_field_lookup = _load_oax_field_lookup()

    # Fail fast on any cross-level mapping
    _validate_hierarchy(groups, subfield_info, oax_field_lookup)

    rows = []
    missing = []
    for g in groups:
        sfid = OAX_SUBFIELD.get(g["group_code"])
        if sfid is None:
            missing.append(g["group_code"])
            continue
        sf = subfield_info.get(sfid, {})
        rows.append({
            "group_code":        g["group_code"],
            "group_name":        g["group_name"],
            "div_code":          g["div_code"],
            "div_name":          g["div_name"],
            "oax_subfield_id":   sfid,
            "oax_subfield_name": sf.get("subfield_name", ""),
            "oax_field_id":      sf.get("field_id", ""),
            "oax_field_name":    sf.get("field_name", ""),
            "oax_domain_id":     sf.get("domain_id", ""),
            "oax_domain_name":   sf.get("domain_name", ""),
        })

    if missing:
        print(f"WARNING: no OAX_SUBFIELD entry for group codes: {missing}", file=sys.stderr)

    with open(OUT_SUBFIELD, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "group_code", "group_name", "div_code", "div_name",
            "oax_subfield_id", "oax_subfield_name",
            "oax_field_id", "oax_field_name",
            "oax_domain_id", "oax_domain_name",
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nWrote {len(rows)} rows → {OUT_SUBFIELD}")
    for r in rows:
        print(
            f"  {r['group_code']}  {r['group_name']:<55}"
            f"→  {r['oax_subfield_id']}  {r['oax_subfield_name']}"
            f"  ({r['oax_field_name']})"
        )


# ---------------------------------------------------------------------------
# 2008 → 2020 group code conversion CSV
# ---------------------------------------------------------------------------

def main_2008_to_2020():
    """
    Build config/for_2008_to_2020.csv — one row per 2008 4-digit group code
    that has a direct 2020 4-digit group equivalent.

    Codes with no 4-digit 2020 equivalent (e.g. 1701 Psychology → division 52
    only) are omitted; callers fall back to division-level lookup.
    """
    df = pd.read_excel(FOR_CONV_XLSX, sheet_name="Sheet1", dtype=str)
    df.columns = ["desc_2020", "code_2020", "code_2008", "desc_2008"]

    # Keep only rows where the 2020 code is a 4-digit group
    groups = df[df["code_2020"].str.match(r"^\d{4}$", na=False) & df["code_2008"].notna()].copy()

    # Zero-pad 3-digit 2008 codes (divisions 01–09) to 4 digits
    groups["code_2008_4"] = groups["code_2008"].str.zfill(4)

    rows = []
    for _, r in groups.iterrows():
        rows.append({
            "for_2008_code": r["code_2008_4"],
            "for_2020_code": r["code_2020"],
            "desc_2008":     r["desc_2008"],
            "desc_2020":     r["desc_2020"],
        })

    # Sort by 2008 code for readability
    rows.sort(key=lambda r: r["for_2008_code"])

    with open(OUT_2008_TO_2020, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["for_2008_code", "for_2020_code", "desc_2008", "desc_2020"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows → {OUT_2008_TO_2020}")
    unmapped = [r for r in rows if not r["for_2020_code"].startswith(("3", "4", "5"))]
    if unmapped:
        print(f"  WARNING: {len(unmapped)} rows have non-2020 target codes")


if __name__ == "__main__":
    main()
    main_subfield()
    main_2008_to_2020()
