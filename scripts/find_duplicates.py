#!/usr/bin/env python3
"""find_duplicates.py: step 5 of the triage pipeline, a reusable near-duplicate finder.

Give it one or more CSV files, the column with the text, the column with the id, and the columns
that must be equal (for crashes: the month and the city). It prints every pair of rows whose texts
are at least --threshold similar (TF-IDF cosine, 0 to 1), most similar first. Texts shorter than
--min-length are skipped, and a text that looks like more than --max-matches others is a form letter, not a copy.
Works on any table, not only NHTSA's: that is what makes it a tool and not a one-off script.

Usage:
  python3 scripts/find_duplicates.py data/ads_old.csv data/ads_new.csv \
      --text Narrative --key "Report ID" --newest "Report Version" --same "Incident Date" City --out data/pairs.csv
"""
import argparse
import sys

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


def find_pairs(df: pd.DataFrame, text: str, key: str, same: list[str], threshold: float = 0.9,
               max_matches: int = 3, min_length: int = 100) -> pd.DataFrame:
    """Pairs of rows that share the `same` columns and whose `text` is at least `threshold` similar."""
    # 1. The rows to compare: one row per key, and only texts long enough to mean something
    df = df.drop_duplicates(key, keep="last")  # one row per key: the last one stays
    df = df[df[text].str.len() >= min_length].reset_index(drop=True)  # short or redacted texts never count
    # 2. Turn every text into numbers, once for the whole table
    vectors = TfidfVectorizer().fit_transform(df[text])  # each text -> a row of word weights
    # 3. IMPORTANT: drop form letters. A crash is filed a few times at most; a text that looks like
    #    many others anywhere in the table is a template a company pastes into every report.
    alike = np.asarray((vectors @ vectors.T >= threshold).sum(axis=1)).ravel() - 1  # how many OTHER texts each one looks like
    df, vectors = df[alike <= max_matches].reset_index(drop=True), vectors[alike <= max_matches]
    # 4. Compare only rows that share the `same` columns; keep pairs at or above the threshold
    groups = df.groupby(same).indices.values() if same else [np.arange(len(df))]  # rows with the same month and city
    pairs = []
    for rows in groups:
        ids = df[key].to_numpy()[rows]  # the keys of the rows in this group
        sim = np.triu(cosine_similarity(vectors[rows]), k=1)  # every pair once: diagonal and lower half set to 0
        for a, b in zip(*np.nonzero(sim >= threshold)):  # positions of the similar pairs
            pairs.append((ids[a], ids[b], round(float(sim[a, b]), 3)))
    pairs = pd.DataFrame(pairs, columns=["a", "b", "similarity"])
    return pairs.sort_values("similarity", ascending=False, ignore_index=True)  # most similar first


def main() -> None:
    p = argparse.ArgumentParser(description="List near-duplicate rows in CSV files.")
    p.add_argument("csv", nargs="+", help="one or more CSV files, read as one table")
    p.add_argument("--text", required=True, help="the column with the text to compare")
    p.add_argument("--key", required=True, help="the column that names each row")
    p.add_argument("--same", nargs="*", default=[], help="columns that must be equal in a pair")
    p.add_argument("--threshold", type=float, default=0.9, help="lowest similarity that counts, 0 to 1")
    p.add_argument("--max-matches", type=int, default=3, help="drop texts that look like more than this many others (form letters)")
    p.add_argument("--min-length", type=int, default=100, help="skip texts shorter than this many characters")
    p.add_argument("--newest", help="if a key appears twice, keep the row with the highest value here")
    p.add_argument("--out", default="-", help="output CSV file; - prints to the screen")
    args = p.parse_args()

    df = pd.concat([pd.read_csv(f, dtype=str, keep_default_na=False) for f in args.csv], ignore_index=True)
    if args.newest:  # keep the newest row of each key, e.g. the highest Report Version
        df = df.sort_values(args.newest, key=lambda c: pd.to_numeric(c, errors="coerce"))
    pairs = find_pairs(df, args.text, args.key, args.same, args.threshold, args.max_matches, args.min_length)
    pairs.to_csv(sys.stdout if args.out == "-" else args.out, index=False)
    print(f"{len(pairs)} pairs at similarity {args.threshold} or more, among {df[args.key].nunique():,} rows", file=sys.stderr)


if __name__ == "__main__":
    main()