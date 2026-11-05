#!/usr/bin/env python3
"""check_data.py: step 2 of the triage pipeline.

Checks that NHTSA's two downloaded files are safe to use before any later step reads them.
Runs after scripts/fetch.sh and starts from its answer in data/status.txt:
  unchanged  nothing new, nothing to check (exit 0)
  changed    five checks on each file:
               1. the columns the later steps read are there
               2. every row has a Report ID
               3. no Report ID and Report Version appears twice
               4. not fewer rows than the last good run (NHTSA only adds reports)
               5. the columns the severity rule reads have only known values (config/known_values.json):
                  a new value, like 'Moderate W/ Hospitalization' from NHTSA's June 2025 form, never stops
                  the run: it is listed once in data/new_values.csv for a person, and step 4 gives those
                  reports at least S1 and severity_pending until the value is added to the list
Writes data/check.txt (ok or failed) for the next steps. Saves each file's row count in
data/<file>.rows only when every check passed, so a bad file never becomes the new normal.
Exit code: 0 = safe to use (or nothing new), 1 = stop.

Usage: python3 scripts/check_data.py [data_folder]
"""
import json
import sys
from pathlib import Path

import pandas as pd

FILES = ["ads_old.csv", "ads_new.csv"]  # the names fetch.sh saves
NEEDED = ["Report ID", "Report Version", "Reporting Entity", "Incident Date", "City", "Narrative",
          "Incident Time (24:00)", "Highest Injury Severity Alleged", "Crash With", "Same Incident ID", "VIN"]
KNOWN = json.loads((Path(__file__).resolve().parents[1] / "config" / "known_values.json").read_text()) #  column -> values seen


def read_table(path: Path) -> pd.DataFrame:
    """Read one CSV file with every value kept exactly as written."""
    # dtype=str: no guessing, "1" stays "1"; keep_default_na=False: NHTSA's word "None" stays "None"
    return pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")


def check_file(folder: Path, name: str) -> tuple[list[str], int, list[dict]]:
    """Run the checks on one file. Returns (problems, row count, new values); no problems means the file is fine."""
    path = folder / name
    if not path.exists():
        return ["the file is missing"], 0, []
    try:
        df = read_table(path)
    except (UnicodeDecodeError, pd.errors.ParserError, pd.errors.EmptyDataError) as error:
        # for example a download cut off in the middle of a story: a quote opens and never closes
        return [f"cannot be read as a table: {error}"], 0, []

    # check 1: stop here if a column is missing, because the next checks look columns up by name
    missing = [c for c in NEEDED if c not in df.columns]
    if missing:
        return ["missing columns: " + ", ".join(missing)], len(df), []

    problems = []
    # check 2: every row has a Report ID (the whole column at once, no loop)
    blank = (df["Report ID"].str.strip() == "").sum()
    if blank:
        problems.append(f"{blank} rows have no Report ID")

    # check 3: the same ID with a new version is a normal update; the same ID AND version twice is not
    repeats = df.duplicated(subset=["Report ID", "Report Version"]).sum()
    if repeats:
        problems.append(f"{repeats} rows repeat a Report ID and Report Version")

    # check 4: compare with the row count saved by the last good run
    last_file = folder / (name + ".rows")
    if last_file.exists():
        last = int(last_file.read_text())
        if len(df) < last:
            problems.append(f"{len(df)} rows, fewer than the {last} last time")

    # check 5: IMPORTANT: a value the severity rule has never seen could get the wrong severity.
    #          It never stops the run: each new value is listed once, with 3 example reports, for a person.
    new = []
    for column, seen in KNOWN.items():
        if column in df.columns: #  the airbag column has a different name on the old and the new form
            values = df[column].str.strip()
            unknown = (values != "") & ~values.isin(seen)
            for value, n in values[unknown].value_counts().items():
                examples = df.loc[values == value, "Report ID"].drop_duplicates().head(3)  # 3 reports, not 3 versions of one
                new.append({"column": column, "value": value, "rows": n, "report_ids": " ".join(examples)})
    return problems, len(df), new


def main() -> int:
    """Check both files, print the result, write check.txt and return the exit code."""
    folder = Path(sys.argv[1] if len(sys.argv) > 1 else "data")  # the test passes a temporary folder

    # step 1's answer: is there anything new to check?
    status_file = folder / "status.txt"
    if not status_file.exists():
        print("no status.txt: run scripts/fetch.sh first")
        return 1
    if status_file.read_text().strip() == "unchanged":
        print("no new data: nothing to check")
        return 0  # check.txt keeps the answer from when these files arrived

    # the checks, file by file
    all_ok = True
    counts = {}
    new_values = []
    for name in FILES:
        problems, n, new = check_file(folder, name)
        counts[name] = n
        if problems:
            all_ok = False
            for p in problems:
                print(f"FAIL  {name}  {p}")
        else:
            print(f"ok    {name}  {n} rows")
        for v in new:
            print(f"NEW   {name}  {v['column']}: '{v['value']}' ({v['rows']} rows), for example {v['report_ids']}")
            new_values.append({"file": name, **v})

    # the review list: one row per new value, rewritten on every checked run (only a header when there are none)
    pd.DataFrame(new_values, columns=["file", "column", "value", "rows", "report_ids"]).to_csv(folder / "new_values.csv", index=False)

    # one word for the next steps; the row counts are saved only after a clean pass
    if not all_ok:
        (folder / "check.txt").write_text("failed\n")
        print("result: NOT safe to use, stopping")
        return 1
    for name, n in counts.items():
        (folder / (name + ".rows")).write_text(f"{n}\n")
    (folder / "check.txt").write_text("ok\n")
    print("result: safe to use")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # 0 or 1 becomes the exit code that && and a scheduler read