#!/usr/bin/env bash
# fetch.sh: step 1 of the triage pipeline.
# Downloads NHTSA's two crash report files and says whether either one changed
# since the last run.
#
# Writes into the data folder:
#   ads_old.csv, ads_new.csv   the two files (replaced only when a download is complete)
#   <file>.sha256              each file's fingerprint, compared on the next run
#   status.txt                 one word for step 2: changed or unchanged
# Exit code: 0 = fine, 22 = a download failed and nothing was written.
#
# Usage: bash scripts/fetch.sh [data_folder]   (default folder: data)
set -euo pipefail   # stop at the first error, an unset variable or a failed pipe

# --- settings ----------------------------------------------------------------
DATA_DIR="${1:-data}"   # the test passes a temporary folder here
BASE="https://static.nhtsa.gov/odi/ffdd/sgo-2021-01"
OLD_URL="$BASE/Archive-2021-2025/SGO-2021-01_Incident_Reports_ADS.csv"   # reports July 2021 to April 2025
NEW_URL="$BASE/SGO-2021-01_Incident_Reports_ADS.csv"                     # reports from April 2025 on

mkdir -p "$DATA_DIR"
changed=0   # becomes 1 if either file changed

# --- fetch_one NAME URL: download one file safely, compare its fingerprint ---
fetch_one() {
    local name="$1"
    local url="$2"
    local dest="$DATA_DIR/$name"
    local tmp="$DATA_DIR/$name.part"   # download here first: a broken download never replaces a good file

    # --fail: an HTTP error such as 404 is a failure, not a saved error page
    if ! curl --fail --silent --show-error --location --retry 3 --output "$tmp" "$url"; then
        echo "FAILED      $name   could not download $url" >&2
        rm -f "$tmp"
        exit 22
    fi

    local new_hash   # on its own line, so set -e still sees a failure in the next line
    new_hash=$(sha256sum "$tmp" | cut -d' ' -f1)
    local old_hash=""
    if [ -f "$dest.sha256" ]; then
        old_hash=$(cut -d' ' -f1 "$dest.sha256")
    fi

    if [ "$new_hash" = "$old_hash" ]; then
        echo "unchanged   $name   $new_hash"
        rm -f "$tmp"   # the same file as last time: drop the copy
    else
        mv "$tmp" "$dest"   # a rename is instant, so the real file is never half written
        echo "$new_hash   $name" > "$dest.sha256"
        echo "Updated     $name   $new_hash   $(wc -c < "$dest") bytes"
        changed=1
    fi
}

# --- run: both files, then one word for the next step -------------------------
fetch_one ads_old.csv "$OLD_URL"
fetch_one ads_new.csv "$NEW_URL"

if [ "$changed" -eq 1 ]; then
    echo "changed" > "$DATA_DIR/status.txt"
    echo "result: new data"
else
    echo "unchanged" > "$DATA_DIR/status.txt"
    echo "result: no new data"
fi