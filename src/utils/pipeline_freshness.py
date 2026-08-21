"""
src/utils/pipeline_freshness.py

Enforces that a pipeline stage's declared inputs are not newer than its own declared output --
i.e. makes it structurally impossible to run a later stage on a stale earlier one without an
explicit, loud failure. Directly motivated by two real incidents found in one session
(2026-08-21): `manual_name_corrections.csv` was silently never applied to production
`01_prepare_arc.py` for an unknown period, and `01a_diagnose.py` ran a real diagnostic pass
against a stale `awards_cif.parquet` immediately after `_merge_awards_cifs()` was fixed, giving a
misleading flood of "failures" that were actually just staleness, not new bugs.

No separate manifest is needed -- real filesystem mtimes ARE the ground truth (the same
principle `make` itself uses: a target is stale if any prerequisite's mtime is newer than the
target's own). This is deliberately simpler than a content-hash/JSON-manifest scheme: fewer
moving parts, and mtimes are already reliable on this project's local/mounted disks.

Usage, at the top of every pipeline stage's main():

    from src.utils.pipeline_freshness import assert_fresh
    from config.settings import PROCESSED_DATA, DATA_PERSISTED

    assert_fresh(
        "03_link_arc_oax",
        outputs=[PROCESSED_DATA / "arc_oax_links.parquet"],
        inputs=[PROCESSED_DATA / "awards_cif.parquet",
                PROCESSED_DATA / "openalex_authors_prep.parquet"],
    )

Raises PipelineStalenessError (never silently warns) if any declared input is newer than the
oldest declared output, or if any declared output is simply missing. Callers decide whether that
error is fatal (default: yes -- every stage script should let it propagate and stop, not catch
and continue) or, in a read-only diagnostic context, worth catching and printing as a warning
instead (see `warn_only=True`).
"""

from datetime import datetime
from pathlib import Path


class PipelineStalenessError(Exception):
    """Raised when a pipeline stage's output predates one of its own declared inputs -- the
    stage that produced or last touched that input needs to be re-run before this one proceeds."""


def assert_fresh(
    stage_name: str,
    outputs: list[Path],
    inputs: list[Path],
    warn_only: bool = False,
) -> None:
    missing_outputs = [o for o in outputs if not o.exists()]
    if missing_outputs:
        msg = (
            f"{stage_name}: missing output file(s), this stage has never been run (or its "
            f"output was deleted):\n" + "\n".join(f"  {o}" for o in missing_outputs)
        )
        _fail(msg, warn_only)
        return

    missing_inputs = [i for i in inputs if not i.exists()]
    if missing_inputs:
        msg = (
            f"{stage_name}: missing input file(s) -- an earlier pipeline stage has never been "
            f"run:\n" + "\n".join(f"  {i}" for i in missing_inputs)
        )
        _fail(msg, warn_only)
        return

    oldest_output_mtime = min(o.stat().st_mtime for o in outputs)
    stale = [(i, i.stat().st_mtime) for i in inputs if i.stat().st_mtime > oldest_output_mtime]
    if stale:
        lines = "\n".join(
            f"  {p}  (modified {datetime.fromtimestamp(m).isoformat(timespec='seconds')})"
            for p, m in sorted(stale, key=lambda x: -x[1])
        )
        oldest_out = min(outputs, key=lambda o: o.stat().st_mtime)
        msg = (
            f"{stage_name}: STALE. Its own output is older than the following input(s) it "
            f"depends on -- re-run the stage that produces/consumes them before continuing:\n"
            f"{lines}\n"
            f"  (output {oldest_out} was last built "
            f"{datetime.fromtimestamp(oldest_output_mtime).isoformat(timespec='seconds')})"
        )
        _fail(msg, warn_only)


def _fail(msg: str, warn_only: bool) -> None:
    if warn_only:
        print(f"  WARNING (pipeline freshness, continuing anyway): {msg}")
    else:
        raise PipelineStalenessError(msg)
