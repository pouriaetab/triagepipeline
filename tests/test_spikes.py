"""Tests for scripts/spikes.py (step 6). Run: python3 -m pytest tests/test_spikes.py -v"""
import subprocess
import sys
from pathlib import Path

import pandas as pd
from spikes import find_spikes, wilson  # found through pythonpath in pyproject.toml

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "spikes.py"
MONTHS = pd.period_range("2024-01", periods=14, freq="M").astype(str)  # 2024-01 ... 2025-02

def crashes(per_month, value="Pole"):
    """A made-up table: per_month is a list of (crashes with `value`, all crashes), one pair per month."""
    rows = [(m, value if i < hit else "Car") for m, (hit, total) in zip(MONTHS, per_month) for i in range(total)]
    return pd.DataFrame(rows, columns=["month", "hit"])


def test_wilson_range_holds_the_share_and_stays_between_0_and_1():
    low, high = wilson(50, 100)
    assert (round(low, 3), round(high, 3)) == (0.404, 0.596)
    low, high = wilson(0, 10)
    assert round(low, 3) == 0 and 0 < high < 1  # 0 of 10 still has a range above 0


def test_a_steady_share_is_never_flagged():
    assert find_spikes(crashes([(10, 100)] * 14), "month", "hit").empty


def test_a_jump_in_the_share_is_a_spike():
    found = find_spikes(crashes([(10, 100)] * 13 + [(30, 100)]), "month", "hit")
    assert list(zip(found.month, found.value, found.status)) == [("2025-02", "Pole", "spike")]


def test_a_growing_fleet_alone_is_never_a_spike():
    assert find_spikes(crashes([(10, 100)] * 13 + [(40, 400)]), "month", "hit").empty  # 4 times the crashes, same 10%


def test_a_value_never_seen_before_is_a_new_value_not_a_spike():
    found = find_spikes(crashes([(0, 100)] * 13 + [(5, 100)], value="Scooter"), "month", "hit")
    assert list(zip(found.value, found.status)) == [("Scooter", "new value")]


def test_two_rows_are_never_flagged():
    assert find_spikes(crashes([(0, 100)] * 13 + [(2, 100)]), "month", "hit").empty  # new, but only 2


def test_command_line_writes_a_csv(tmp_path):
    crashes([(10, 100)] * 13 + [(30, 100)]).to_csv(tmp_path / "in.csv", index=False)
    subprocess.run([sys.executable, str(SCRIPT), str(tmp_path / "in.csv"), "--month", "month", "--column", "hit",
                    "--out", str(tmp_path / "out.csv")], check=True, capture_output=True)
    assert pd.read_csv(tmp_path / "out.csv").shape == (1, 10)