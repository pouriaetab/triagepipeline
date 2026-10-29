"""Tests for scripts/upload.sh (step 3a).

A fake databricks command, put first on the PATH, writes down what it was asked to do,
so the tests run without a Databricks account and never upload anything.

Run: python3 -m pytest tests/test_upload.py -v
"""
import os
import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "upload.sh"


def run(tmp_path: Path, check_word: str) -> subprocess.CompletedProcess:
    """Make a data folder whose check.txt says check_word, then run upload.sh with the fake databricks."""
    data = tmp_path / "data"
    data.mkdir()
    (data / "check.txt").write_text(check_word + "\n")
    fake = tmp_path / "bin" / "databricks"
    fake.parent.mkdir()
    fake.write_text(f'#!/usr/bin/env bash\necho "$@" >> "{tmp_path}/calls.txt"\n')  # records each call
    fake.chmod(0o755)
    env = {**os.environ, "PATH": f"{fake.parent}:{os.environ['PATH']}"}  # the fake is found first
    return subprocess.run(["bash", str(SCRIPT), str(data)], capture_output=True, text=True, env=env, check=False)


def test_ok_uploads_both_files(tmp_path):
    result = run(tmp_path, "ok")
    assert result.returncode == 0
    calls = (tmp_path / "calls.txt").read_text().splitlines()
    assert [c.split()[-1] for c in calls] == ["dbfs:/Volumes/workspace/triage/nhtsa/ads_old.csv",
                                              "dbfs:/Volumes/workspace/triage/nhtsa/ads_new.csv"]


def test_failed_check_uploads_nothing(tmp_path):
    result = run(tmp_path, "failed")
    assert result.returncode == 1
    assert "nothing uploaded" in result.stderr
    assert not (tmp_path / "calls.txt").exists()