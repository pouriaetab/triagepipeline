"""Tests for scripts/find_duplicates.py (step 5). Run: python3 -m pytest tests/test_find_duplicates.py -v"""
import subprocess
import sys
from pathlib import Path
 
import pandas as pd
from find_duplicates import find_pairs  # found through pythonpath in pyproject.toml
 
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "find_duplicates.py"
 
STORY = ("A car hit the AV from behind while it waited at a red light on Main Street. "
         "The AV was stopped and had no passengers. Both cars were driven away after the crash.")
HIDDEN = "[REDACTED, MAY CONTAIN CONFIDENTIAL BUSINESS INFORMATION]"
LETTER = ("The company filed an incident report under the standing order. The company has not "
          "investigated the incident and has no knowledge of the facts in the report.")
ROWS = pd.DataFrame({
    "id":    ["r1", "r2", "r3", "r4", "r5", "f1", "f2", "f3", "f4", "f5"],
    "city":  ["Phoenix", "Phoenix", "Austin", "Phoenix", "Phoenix"] + ["Phoenix"] * 5,
    "story": [STORY, STORY.replace("Both cars", "Both vehicles"), STORY, HIDDEN, HIDDEN] + [LETTER] * 5,
})
 
 
def ids(pairs):
    return list(pairs[["a", "b"]].itertuples(index=False, name=None))
 
 
def test_similar_stories_in_the_same_city_are_a_pair():
    assert ids(find_pairs(ROWS, "story", "id", ["city"])) == [("r1", "r2")]
 
 
def test_the_same_story_in_another_city_is_not_a_pair():
    assert "r3" not in str(ids(find_pairs(ROWS, "story", "id", ["city"])))
 
 
def test_short_texts_never_pair():
    assert "r4" not in str(ids(find_pairs(ROWS, "story", "id", ["city"])))
 
 
def test_a_text_that_looks_like_many_others_is_a_form_letter():
    assert "f1" not in str(ids(find_pairs(ROWS, "story", "id", ["city"])))  # f1 to f5 each look like 4 others
    assert len(find_pairs(ROWS, "story", "id", ["city"], max_matches=4)) == 11  # r1-r2 and the 10 letter pairs
 
 
def test_a_high_threshold_keeps_only_identical_texts():
    assert ids(find_pairs(ROWS, "story", "id", [], threshold=0.999)) == [("r1", "r3")]  # 0.999, not 1: decimals round
 
 
def test_command_line_writes_a_csv(tmp_path):
    ROWS.to_csv(tmp_path / "in.csv", index=False)
    subprocess.run([sys.executable, str(SCRIPT), str(tmp_path / "in.csv"), "--text", "story", "--key", "id",
                    "--same", "city", "--out", str(tmp_path / "out.csv")], check=True, capture_output=True)
    assert pd.read_csv(tmp_path / "out.csv").shape == (1, 3)