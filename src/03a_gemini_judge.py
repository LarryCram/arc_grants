"""
src/03a_gemini_judge.py -- send step 5's Gemini requests (processed/oeuvre/gemini_requests.parquet,
written by src/03_build_oeuvres.py) within a budget; answers are appended to
processed/oeuvre/gemini_verdicts.jsonl and never re-sent. Then rerun
`src/03_build_oeuvres.py --from-step 5` to apply them.

Usage: .venv/bin/python src/03a_gemini_judge.py --max-calls 50 [--max-input-tokens 500000]
           [--kinds component works] [--workers 4]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from config.settings import OEUVRE_DIR
from src.oeuvre import gemini


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-calls", type=int, required=True)
    ap.add_argument("--max-input-tokens", type=int, default=500_000)
    ap.add_argument("--kinds", nargs="+", choices=["component", "works"], default=None)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()
    req = pd.read_parquet(OEUVRE_DIR / "gemini_requests.parquet")
    tot = gemini.run(req, OEUVRE_DIR / "gemini_verdicts.jsonl", args.max_calls, args.max_input_tokens,
                     workers=args.workers, kinds=args.kinds)
    print(tot)


if __name__ == "__main__":
    main()
