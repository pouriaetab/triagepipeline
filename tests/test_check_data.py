"""Tests for scripts/check_data.py (step 2).

Each test builds a small fake data folder with pandas, runs the script on it as a program,
and checks the exit code and what it printed. Nothing is downloaded and data/ is never touched.

Run: python3 -m pytest tests/test_check_data.py -v
"""
import subprocess
import sys
from pathlib import Path

import pandas as pd

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_data.py"
COLUMNS = ["Report ID", "Report Version", "Reporting Entity", "Incident Date", "City", "Narrative",
           "Incident Time (24:00)", "Highest Injury Severity Alleged", "Crash With", "Same Incident ID", "VIN"]
GOOD = [["30270-1", "1", "Waymo LLC", "JUN-2025", "Phoenix", "A car hit the AV from behind.", 
         "14:05", "Minor W/O Hospitalization", "Passenger Car", "a1", "SADHW2S15R1"],
        ["30270-2", "1", "Zoox, Inc.", "JUL-2025", "Las Vegas", "The AV was stopped at a light.",
         "09:30", "No Injured Reported", "SUV", "b2", "7SAYGDEE0PA"]]


# --- helpers ------------------------------------------------------------------
# builds the test input: two small CSV files and a status.txt in a temporary folder
def make_folder(folder: Path, rows: list, status: str = "changed", columns: list = COLUMNS) -> None:
    """Write both CSV files with these rows, and status.txt the way fetch.sh does."""
    for name in ["ads_old.csv", "ads_new.csv"]:
        pd.DataFrame(rows, columns=columns).to_csv(folder / name, index=False)
    (folder / "status.txt").write_text(status + "\n")

# runs check_data.py on that folder, the same way the pipeline does, and returns what it printed and its exit code
def run(folder: Path) -> subprocess.CompletedProcess:
    """Run check_data.py on the folder; the result has .returncode and .stdout."""
    return subprocess.run([sys.executable, str(SCRIPT), str(folder)], capture_output=True, text=True, check=False)


# --- test cases: one per check, plus the skip and the review list --------------
def test_good_files_pass(tmp_path):
    make_folder(tmp_path, GOOD)
    result = run(tmp_path)
    assert result.returncode == 0
    assert (tmp_path / "check.txt").read_text() == "ok\n"


def test_missing_column_fails(tmp_path):
    make_folder(tmp_path, [r[:5] for r in GOOD], columns=COLUMNS[:5])  # no Narrative
    result = run(tmp_path)
    assert result.returncode == 1
    assert "missing columns: Narrative" in result.stdout


def test_repeated_report_fails(tmp_path):
    make_folder(tmp_path, GOOD + [GOOD[0]])  # the first report twice
    result = run(tmp_path)
    assert result.returncode == 1
    assert "1 rows repeat" in result.stdout


def test_fewer_rows_than_last_time_fails(tmp_path):
    make_folder(tmp_path, GOOD)
    run(tmp_path)  # a good run saves 2 as the row count
    make_folder(tmp_path, GOOD[:1])  # now the files have 1 row
    result = run(tmp_path)
    assert result.returncode == 1
    assert "fewer than the 2 last time" in result.stdout


def test_unchanged_skips(tmp_path):
    make_folder(tmp_path, GOOD, status="unchanged")
    result = run(tmp_path)
    assert result.returncode == 0
    assert "nothing to check" in result.stdout


def test_a_new_value_is_listed_and_never_stops_the_run(tmp_path):
    hover = [GOOD[0][:8] + ["Hoverboard"] + GOOD[0][9:], GOOD[1]] # a Crash With value NHTSA never used
    make_folder(tmp_path, hover)
    result = run(tmp_path)
    assert result.returncode == 0 # the data still flows
    assert (tmp_path / "check.txt").read_text() == "ok\n"
    assert "NEW   ads_old.csv  Crash With: 'Hoverboard' (1 rows), for example 30270-1" in result.stdout
    listed = pd.read_csv(tmp_path / "new_values.csv") # one row per file that has it, for the person
    assert listed[["column", "value", "rows", "report_ids"]].iloc[0].tolist() == ["Crash With", "Hoverboard", 1, "30270-1"]


def test_no_new_values_leaves_an_empty_review_list(tmp_path):
    make_folder(tmp_path, GOOD)
    result = run(tmp_path)
    assert pd.read_csv(tmp_path / "new_values.csv").empty