"""
Tests for src/utils/pipeline_freshness.py -- real files on a tmp_path, real mtimes (via os.utime),
not mocked, since the whole point of this module is to trust the filesystem directly.
"""
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.pipeline_freshness import assert_fresh, PipelineStalenessError


def _touch(path: Path, mtime: float) -> None:
    path.write_text("x")
    os.utime(path, (mtime, mtime))


class TestAssertFresh:
    def test_output_newer_than_inputs_passes(self, tmp_path):
        inp = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        _touch(inp, 100)
        _touch(out, 200)
        assert_fresh("stage", outputs=[out], inputs=[inp])  # no raise

    def test_input_newer_than_output_raises(self, tmp_path):
        inp = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        _touch(out, 100)
        _touch(inp, 200)
        with pytest.raises(PipelineStalenessError, match="STALE"):
            assert_fresh("stage", outputs=[out], inputs=[inp])

    def test_missing_output_raises(self, tmp_path):
        inp = tmp_path / "in.parquet"
        _touch(inp, 100)
        with pytest.raises(PipelineStalenessError, match="missing output"):
            assert_fresh("stage", outputs=[tmp_path / "missing.parquet"], inputs=[inp])

    def test_missing_input_raises(self, tmp_path):
        out = tmp_path / "out.parquet"
        _touch(out, 100)
        with pytest.raises(PipelineStalenessError, match="missing input"):
            assert_fresh("stage", outputs=[out], inputs=[tmp_path / "missing.parquet"])

    def test_multiple_outputs_uses_oldest(self, tmp_path):
        # A stale input only needs to be newer than the OLDEST output to count as stale --
        # otherwise a stage with two outputs could silently have one of them go stale.
        inp = tmp_path / "in.parquet"
        out1 = tmp_path / "out1.parquet"
        out2 = tmp_path / "out2.parquet"
        _touch(out1, 100)   # older output
        _touch(out2, 300)   # newer output
        _touch(inp, 200)    # newer than out1, older than out2
        with pytest.raises(PipelineStalenessError, match="STALE"):
            assert_fresh("stage", outputs=[out1, out2], inputs=[inp])

    def test_warn_only_does_not_raise(self, tmp_path, capsys):
        inp = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        _touch(out, 100)
        _touch(inp, 200)
        assert_fresh("stage", outputs=[out], inputs=[inp], warn_only=True)  # no raise
        assert "WARNING" in capsys.readouterr().out

    def test_equal_mtime_not_stale(self, tmp_path):
        inp = tmp_path / "in.parquet"
        out = tmp_path / "out.parquet"
        _touch(inp, 100)
        _touch(out, 100)
        assert_fresh("stage", outputs=[out], inputs=[inp])  # no raise -- not strictly newer
