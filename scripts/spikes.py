#!/usr/bin/env python3
"""spikes.py: step 6 of the triage pipeline, a spike detector with 95% ranges.

For every month and every value of one column (for crashes: what the AV hit), it compares the
value's share of that month with its share in the 12 months before (a p-chart, from statistical
process control). A share above the upper limit is a spike: more than chance explains.
Each share gets a 95% range (Wilson), so a reader sees how sure the number is.
NHTSA gives no miles driven, so shares are compared, not counts: a growing fleet alone never flags.
A value never seen in the 12 months before is not a spike but a new value: often a form change
(NHTSA's June 2025 form added 'Non-Motorist: Scooter - Skateboard'), so a person checks it first.

Usage:
    python3 scripts/spikes.py data/crashes.csv --month month --column crash_with --out data/spikes.csv
"""
import argparse
import sys

import numpy as np
import pandas as pd


def wilson(k, n, z: float = 1.96):
    """95% range for a share k out of n (Wilson score interval): stays between 0 and 1, even for small n."""
    p = k / n
    mid = (p + z**2 / (2 * n)) / (1 + z**2 / n)
    half = z * np.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / (1 + z**2 / n)
    return mid - half, mid + half


def to_month(s: pd.Series) -> pd.Series:
    """2025-06 or JUN-2025 -> the month 2025-06; anything else (like 'unknown') -> empty."""
    d = pd.to_datetime(s, format="%Y-%m", errors="coerce").fillna(pd.to_datetime(s, format="%b-%Y", errors="coerce"))
    return d.dt.to_period("M")


def find_spikes(df: pd.DataFrame, month: str, column: str, window: int = 12, sigmas: float = 3,
                min_reports: int = 3) -> pd.DataFrame:
    """Months where a value's share is above its upper limit (spike), or where a value appears for the first time (new value)."""
    # NOTE: why sigmas=3: even when nothing really changed, a share moves up and down by chance. It lands above
    #       normal + 3 standard errors only 1 time in 741 (scipy.stats.norm.sf(3) = 0.00135; 2 sigmas: 1 in 44,
    #       2.5: 1 in 161). NHTSA's crashes give about 58 checks a year (a check = one month and one value),
    #       so 3 sigmas means about 0.08 false alarms a year. Any number works, 2.5 too: --sigmas 2.5.
    # NOTE: why min_reports=3: a value expected 0.5 times a month reaches 2 by chance 9% of the time, but 3 only 1.4%
    #       (scipy.stats.poisson.sf(2, 0.5) = 0.014). A single report is never ignored: step 2 lists every new value
    #       and step 4 gives every ticket its own severity.
    # 1. Count the rows per month and value
    k = pd.crosstab(to_month(df[month]), df[column])  # one row per month, one column per value, counts inside
    n = k.sum(axis=1)  # all rows in each month
    # 2. The normal share of each value: its share in the `window` months before (not counting this month)
    normal = k.rolling(window).sum().shift(1).div(n.rolling(window).sum().shift(1), axis=0)
    # 3. IMPORTANT: the upper limit is wider when the month has few rows; 3 sigmas keeps false alarms rare
    # p-chart upper control limit (Shewhart, normal approximation). Assumes independent crashes and a stable
    # 12-month normal; rough when n * normal < 5 (hence min_reports), misses slow drifts, and flags only rises.
    upper = normal + sigmas * np.sqrt(normal * (1 - normal)).div(np.sqrt(n), axis=0)
    share = k.div(n, axis=0)  # shares, not counts: fleet growth alone never flags
    # 4. One row per month and value
    total = pd.DataFrame({value: n for value in k.columns})  # the month's total under every value, same shape as k
    t = pd.concat({"rows": k, "month_total": total, "share": share, "normal": normal, "upper": upper}, axis=1).stack()
    t = t[t.normal.notna() & (t.rows >= min_reports)].copy()  # NOTE: the first `window` months have no normal yet
    t["status"] = np.where(t.normal == 0, "new value", np.where(t.share > t.upper, "spike", ""))
    t = t[t.status != ""].copy()
    t["low"], t["high"] = wilson(t.rows, t.month_total)
    t = t.reset_index(names=["month", "value"])
    t["month"] = t["month"].astype(str)
    return t.round(3).sort_values(["month", "value"], ascending=[False, True], ignore_index=True)


def main() -> None:
    p = argparse.ArgumentParser(description="Flag months where a value's share is unusually high, or new.")
    p.add_argument("csv", nargs="+", help="one or more CSV files, read as one table")
    p.add_argument("--month", required=True, help="the column with the month, like 2025-06 or JUN-2025")
    p.add_argument("--column", required=True, help="the column whose values are counted")
    p.add_argument("--window", type=int, default=12, help="months that make the normal share")
    p.add_argument("--sigmas", type=float, default=3, help="how far above normal counts as a spike")
    p.add_argument("--min-reports", type=int, default=3, help="fewer rows than this are never flagged")
    p.add_argument("--out", default="-", help="output CSV file; - prints to the screen")
    args = p.parse_args()
 
    df = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False) for f in args.csv], ignore_index=True)
    found = find_spikes(df, args.month, args.column, args.window, args.sigmas, args.min_reports)
    found.to_csv(sys.stdout if args.out == "-" else args.out, index=False)
    print(f"{(found.status == 'spike').sum()} spikes and {(found.status == 'new value').sum()} new values "
          f"in '{args.column}' among {len(df):,} rows", file=sys.stderr)


if __name__ == "__main__":
    main()