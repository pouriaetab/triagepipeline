# triagepipeline

A small, tested pipeline that turns public crash reports of automated vehicles into triage tickets, built one step at a time.

## Steps

1. scripts/fetch.sh: checks NHTSA for new data files (Bash, SHA-256).
2. scripts/check_data.py: checks the files are safe to use before anything runs (pandas).
3. scripts/upload.sh and notebooks/load_reports.py: keep one row per report in a Delta table on Databricks; only new or changed reports are written.
4. notebooks/build_tickets.py: turns new or changed reports into tickets with a permanent key, a severity from S0 to S3, and links for exact copies.
5. scripts/find_duplicates.py: lists pairs of reports that tell the same crash in almost the same words, in any CSV (TF-IDF, cosine similarity); form letters are left out and nothing is removed.

Each step has its own pull request and tests.

## Run the tests

```bash
python3 -m pip install pandas numpy scikit-learn pytest
python3 -m pytest -v
bash tests/test_fetch.sh
```

## Data

[NHTSA Standing General Order crash reports](https://www.nhtsa.gov/laws-regulations/standing-general-order-crash-reporting), public data from the US government.