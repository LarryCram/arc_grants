"""
src/03a_gemini_judge.py -- send step 5's Gemini requests (processed/oeuvre/gemini_requests.parquet,
written by src/03_build_oeuvres.py) within a budget; answers are appended to
processed/oeuvre/gemini_verdicts.jsonl and never re-sent. Then rerun
`src/03_build_oeuvres.py --from-step 5` to apply them.

Usage: .venv/bin/python src/03a_gemini_judge.py --max-calls 50 [--max-input-tokens 500000]
           [--kinds component works] [--workers 4]
Model comparison (2026-10-08): --compare-sample N re-asks N requests already answered in
gemini_verdicts.jsonl (half component, half works, fixed seed) with --model / --key-env, saving to
--out (a separate file, so the main verdicts are never mixed):
       .venv/bin/python src/03a_gemini_judge.py --max-calls 200 --compare-sample 200 \
           --model gemini-3.5-flash-lite --key-env GEMINI_FREE_API_KEY --out gemini_verdicts_flashlite.jsonl --workers 2
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
    ap.add_argument("--model", default=gemini.MODEL)
    ap.add_argument("--key-env", default="GEMINI_API_KEY")
    ap.add_argument("--out", default="gemini_verdicts.jsonl")
    ap.add_argument("--compare-sample", type=int, default=None)
    args = ap.parse_args()
    req = pd.read_parquet(OEUVRE_DIR / "gemini_requests.parquet")
    only = None
    if args.compare_sample:
        done = {k for k, v in gemini.load_verdicts(OEUVRE_DIR / "gemini_verdicts.jsonl").items() if v["answer"] is not None}
        a = req[req.request_key.isin(done)]
        n = args.compare_sample // 2
        only = pd.concat([a[a.kind == k].sample(min(n, (a.kind == k).sum()), random_state=42)
                          for k in ("component", "works")]).request_key
    tot = gemini.run(req, OEUVRE_DIR / args.out, args.max_calls, args.max_input_tokens, model=args.model,
                     workers=args.workers, kinds=args.kinds, key_env=args.key_env, only_keys=only)
    print(tot)


if __name__ == "__main__":
    main()
