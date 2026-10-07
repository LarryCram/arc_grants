"""
Gemini calls for step 5 of the oeuvre extractor (2026-10-07): each request in
processed/oeuvre/gemini_requests.parquet (written by src/oeuvre/classify.py) is sent once; the
answer is appended to gemini_verdicts.jsonl with its token counts and model version, and is never
sent again. A run stops cleanly at its budget (calls and input tokens). The key is GEMINI_API_KEY
in .env (a Google AI Studio key: billed to its Cloud project, or free tier; separate from any Gemini
app subscription).
"""

from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

MODEL = "gemini-flash-latest"


def load_verdicts(path) -> dict:
    """request_key -> saved record (answer, id_order, tokens, ...); the last record per key wins."""
    out = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    out[r["request_key"]] = r
    return out


def run(requests, verdicts_path, max_calls: int, max_input_tokens: int, model: str = MODEL,
        workers: int = 4, order_seed: str = "", kinds=None) -> dict:
    """Send unanswered requests (in a fixed pseudo-random order) until the budget is reached."""
    from dotenv import load_dotenv
    from google import genai
    from google.genai import types

    load_dotenv()
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    done = load_verdicts(verdicts_path)
    todo = requests[~requests.request_key.isin(set(done))].copy()
    if kinds:
        todo = todo[todo.kind.isin(kinds)]
    todo["_o"] = [hash_key(k + order_seed) for k in todo.request_key]
    todo = todo.sort_values("_o")
    lock = threading.Lock()
    tot = {"calls": 0, "input_tokens": 0, "output_tokens": 0, "thinking_tokens": 0, "errors": 0, "stopped_by": None}

    def one(r):
        with lock:
            if tot["calls"] >= max_calls:
                tot["stopped_by"] = "max_calls"
                return
            if tot["input_tokens"] >= max_input_tokens:
                tot["stopped_by"] = "max_input_tokens"
                return
            tot["calls"] += 1
        for attempt in range(5):
            try:
                t = time.time()
                resp = client.models.generate_content(
                    model=model, contents=r.prompt,
                    config=types.GenerateContentConfig(temperature=0, response_mime_type="application/json"))
                try:
                    answer = json.loads(resp.text)
                except (json.JSONDecodeError, TypeError):
                    answer = None
                u = resp.usage_metadata
                rec = {"request_key": r.request_key, "kind": r.kind, "cluster_id": r.cluster_id,
                       "id_order": [int(x) for x in r.id_order], "answer": answer, "raw": resp.text,
                       "model": model, "model_version": getattr(resp, "model_version", None),
                       "input_tokens": u.prompt_token_count, "output_tokens": u.candidates_token_count,
                       "thinking_tokens": getattr(u, "thoughts_token_count", None),
                       "total_tokens": getattr(u, "total_token_count", None),
                       "seconds": round(time.time() - t, 1), "at": time.strftime("%Y-%m-%d %H:%M:%S")}
                with lock:
                    with open(verdicts_path, "a", encoding="utf-8") as f:
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    tot["input_tokens"] += u.prompt_token_count or 0
                    tot["output_tokens"] += u.candidates_token_count or 0
                    tot["thinking_tokens"] += getattr(u, "thoughts_token_count", None) or 0
                return
            except Exception as e:  # rate limits and transient errors: back off and retry
                if attempt == 4:
                    with lock:
                        tot["errors"] += 1
                    print(f"failed {r.request_key}: {e}")
                    return
                time.sleep(10 * (attempt + 1))

    with ThreadPoolExecutor(workers) as ex:
        list(ex.map(one, list(todo.itertuples())))
    tot["remaining"] = len(todo) - tot["calls"]
    return tot


def hash_key(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode()).hexdigest()
