"""
analysis/40_postnominal_audit.py -- which of nameparser's post-nominal words are really family names in OpenAlex
names, and which are really used as post-nominals (2026-10-24, user: "develop a definitive list of the
postnominals in openalex names that are actually family names and not postnominals. In general the postnominals
for research works bylines will not be as extensive as the set used in human_names").

nameparser's suffix sets (CONSTANTS.suffix_acronyms + suffix_not_acronyms, 623 words, plus the project's
Australian award letters added in src/utils/names.py) make HumanName read the last word of a 3+ word name as a
post-nominal when it is one of them: "Y. W. Mai" -> first Y., last W., suffix Mai.

Each word is counted, case-insensitively and without dots, in two name populations: every OpenAlex author
display name, and every printed byline (author_name) in the HEP-context authorships (authorships_hep.parquet).
Evidence that the word is a FAMILY NAME:
  family_comma     the whole part before the first comma ("Mai, Y. W.")
  two_word_last    the last of exactly two words, not in capitals ("Wei Ma")
  multi_word_last  the last of 3+ words, not in capitals ("Wei Jun Ma") -- for generational words (Jr, II, ...)
                   this counts as post-nominal use instead ("John Smith Jr.")
Evidence that the word is a POST-NOMINAL:
  caps_last        the last of 3+ words, in capitals, the first word not in capitals ("Anthony Thomas AO")
  after_comma      the part after the last comma, with 2+ words before it ("John Smith, Jr.", "Kinloch, FRS")
Post-nominal style = all capitals or a capital after a lower-case letter (PhD, FREng); title case (Mai) is name style.
Class: single letters and digits -> initial (never a post-nominal: "Kumar V" is an initial); postnominal when
post-nominal evidence is 20+ and at least the family evidence; both when family evidence is 20+ and post-nominal
evidence is 20+ and at least 2% of it (a name in title case, a post-nominal in capitals: Ao / AO, Mbe / MBE);
family_name when family evidence is 20+ otherwise (initials instead when the word has no vowel and 3 letters or
fewer: "Wang Jp", "Smith Mm" are run-together initials); unused when both are under 20. Note: two-letter title-case
words after a surname are often run-together initials too ("Lukevich Am", "Ivanova Ea"); either way they are
names, not post-nominals.
Writes processed/postnominal_audit.md and processed/postnominal_audit.csv (one row per word, every count).
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nameparser.config import CONSTANTS

DEFAULT = {w.lower().replace(".", "") for w in set(CONSTANTS.suffix_acronyms) | set(CONSTANTS.suffix_not_acronyms)}
ARC_AWARDS = {w.lower() for w in ("AC", "AO", "AM", "OAM", "AK", "OL", "FAA", "FAHMS", "FTSE", "FASSA", "FAHA",
                                    "FRS", "FREng", "CBE", "OBE", "MBE", "KBE", "DBE", "Pharmacist")}
GENERATIONAL = {"jr", "sr", "jnr", "snr", "junior", "senior", "ii", "iii", "iv", "esq", "esquire"}

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402

import src.utils.names  # noqa: E402,F401  (registers the project's own post-nominals)
from config.settings import OAX_AUTHORS, PROCESSED_DATA  # noqa: E402

OUT_MD = PROCESSED_DATA / "postnominal_audit.md"
OUT_CSV = PROCESSED_DATA / "postnominal_audit.csv"
MIN = 20
# post-nominal style: all capitals, or a capital after a lower-case letter (PhD, FREng, DPhil)
PN_STYLE = r"^([A-Z.\-]+|[A-Z][A-Za-z.]*[a-z][A-Z][A-Za-z.]*)$"
VOWELS = set("aeiouy")
SHARE = 0.02   # post-nominal use under 2% of family use counts as noise (all-capital names, stray letters)


def features_sql(source: str) -> str:
    """Per name: the normalised word in each evidence position (NULL where the position doesn't apply)."""
    return f"""
        WITH n AS (SELECT regexp_replace(trim(nm), '\\s+', ' ', 'g') AS nm FROM ({source}) WHERE nm IS NOT NULL),
        f AS (
          SELECT nm, strpos(nm, ',') > 0 AS has_comma,
                 string_split(nm, ' ') AS toks,
                 trim(split_part(nm, ',', 1)) AS before_first,
                 trim(regexp_extract(nm, ',([^,]*)$', 1)) AS after_last,
                 trim(regexp_extract(nm, '^(.*),[^,]*$', 1)) AS before_last
          FROM n)
        SELECT
          CASE WHEN has_comma AND strpos(before_first, ' ') = 0 THEN lower(replace(before_first, '.', '')) END AS w_family_comma,
          CASE WHEN NOT has_comma AND len(toks) = 2 AND NOT regexp_matches(toks[2], '{PN_STYLE}') THEN lower(replace(toks[2], '.', '')) END AS w_two_word_last,
          CASE WHEN NOT has_comma AND len(toks) >= 3 AND NOT regexp_matches(toks[-1], '{PN_STYLE}') THEN lower(replace(toks[-1], '.', '')) END AS w_multi_word_last,
          CASE WHEN NOT has_comma AND len(toks) >= 3 AND regexp_matches(toks[-1], '{PN_STYLE}') AND toks[1] <> upper(toks[1])
               THEN lower(replace(toks[-1], '.', '')) END AS w_caps_last,
          CASE WHEN has_comma AND len(string_split(trim(replace(before_last, ',', ' ')), ' ')) >= 2
               THEN lower(replace(after_last, '.', '')) END AS w_after_comma
        FROM f"""


def count(con, source: str, words: pd.DataFrame, tag: str) -> pd.DataFrame:
    con.execute(f"CREATE OR REPLACE TEMP TABLE feat AS {features_sql(source)}")
    out = words[["word"]].copy()
    for col in ("family_comma", "two_word_last", "multi_word_last", "caps_last", "after_comma"):
        c = con.execute(f"SELECT w_{col} AS word, count(*) AS n FROM feat WHERE w_{col} IN (SELECT word FROM words) GROUP BY 1").df()
        out = out.merge(c.rename(columns={"n": f"{tag}_{col}"}), on="word", how="left")
    return out.fillna(0)


def classify(r) -> str:
    if len(r.word) == 1 or r.word.isdigit():
        return "initial"
    fam = r.family_comma + r.two_word_last + (0 if r.word in GENERATIONAL else r.multi_word_last)
    suf = r.caps_last + r.after_comma + (r.multi_word_last if r.word in GENERATIONAL else 0)
    if suf >= MIN and suf >= fam:
        return "postnominal"
    if fam >= MIN:
        if suf >= MIN and suf >= SHARE * fam:
            return "both"                     # name in title case, post-nominal in capitals (Ao / AO, Mbe / MBE)
        # no vowel and 3 letters or fewer: run-together initials after a surname ("Wang Jp"), not a family name
        return "initials" if len(r.word) <= 3 and not set(r.word) & VOWELS else "family_name"
    return "unused"


def main():
    allw = {w.lower().replace(".", "") for w in set(CONSTANTS.suffix_acronyms) | set(CONSTANTS.suffix_not_acronyms)}
    words = pd.DataFrame({"word": sorted(allw)})
    words["origin"] = ["nameparser" if w in DEFAULT else "project" for w in words.word]
    con = duckdb.connect()
    con.execute("SET temp_directory='/home/lc/s/.tmp'")
    con.register("words", words[["word"]])
    a = count(con, f"SELECT display_name AS nm FROM read_parquet('{OAX_AUTHORS}/*.parquet')", words, "dn")
    b = count(con, f"SELECT author_name AS nm FROM read_parquet('{PROCESSED_DATA}/authorships_hep.parquet')", words, "byl")
    d = words.merge(a, on="word").merge(b, on="word")
    for col in ("family_comma", "two_word_last", "multi_word_last", "caps_last", "after_comma"):
        d[col] = d[f"dn_{col}"] + d[f"byl_{col}"]
    d["family_evidence"] = d.family_comma + d.two_word_last + [0 if w in GENERATIONAL else m for w, m in zip(d.word, d.multi_word_last)]
    d["postnominal_evidence"] = d.caps_last + d.after_comma + [m if w in GENERATIONAL else 0 for w, m in zip(d.word, d.multi_word_last)]
    d["class"] = [classify(r) for r in d.itertuples()]
    arc = con.execute(f"SELECT lower(replace(family_name_raw, '.', '')) AS word, count(*) AS arc_records "
                      f"FROM read_parquet('{PROCESSED_DATA}/arc_names.parquet') GROUP BY 1").df()
    d = d.merge(arc, on="word", how="left").fillna({"arc_records": 0})
    d = d.astype({c: "int64" for c in d.columns if c not in ("word", "origin", "class")})
    d.sort_values(["class", "family_evidence"], ascending=[True, False]).to_csv(OUT_CSV, index=False)

    cols = ["word", "origin", "family_evidence", "postnominal_evidence", "family_comma", "two_word_last",
            "multi_word_last", "caps_last", "after_comma", "arc_records"]
    L = ["# nameparser post-nominal words in OpenAlex names (2026-10-24)", "",
         f"Words in the parser's post-nominal list: {len(d)} (nameparser {int((d.origin == 'nameparser').sum())}, "
         f"project {int((d.origin == 'project').sum())}). Counted in every OpenAlex author display name and every "
         "HEP-context byline; method in the script's docstring.", "",
         "| class | words |", "|---|---|"] + [f"| {k} | {v} |" for k, v in d["class"].value_counts().items()]
    for cls, title in [("family_name", "Family names, not post-nominals (remove from the parser's list)"),
                       ("both", "Used both ways: a name in title case, a post-nominal in capitals"),
                       ("postnominal", "Post-nominals actually used in OpenAlex names (keep)"),
                       ("initials", "Run-together initials after a surname (not post-nominals)"),
                       ("initial", "Single letters and digits (initials, not post-nominals)")]:
        s = d[d["class"] == cls].sort_values("family_evidence" if cls != "postnominal" else "postnominal_evidence",
                                             ascending=False)
        L += ["", f"## {title}: {len(s)}", "", s[cols].to_markdown(index=False) if len(s) else "(none)"]
    u = d[d["class"] == "unused"]
    L += ["", f"## Unused in OpenAlex names (fewer than {MIN} of either kind): {len(u)}", "",
          ", ".join(u.word)]
    keep = sorted(set(d.word[d["class"].isin(["postnominal", "both"])]) | ARC_AWARDS)
    gen = sorted(set(keep) & GENERATIONAL)
    L += ["", "## Proposed post-nominal list for this project", "",
          f"{len(keep)} words: the post-nominals seen in OpenAlex names, the words used both ways, and the Australian "
          "award letters ARC names carry (src/utils/names.py). Proposed rule: one of these words is a post-nominal "
          "only when written in post-nominal style (capitals, or a capital after a lower-case letter: AO, MBE, PhD, "
          f"FREng); generational words ({', '.join(gen)}) in any case. Every other word in nameparser's list -- the "
          "family names, initials and unused words above -- would no longer be read as a post-nominal.", "",
          ", ".join(keep)]
    OUT_MD.write_text("\n".join(L) + "\n", encoding="utf-8")
    print(OUT_MD)


if __name__ == "__main__":
    main()
