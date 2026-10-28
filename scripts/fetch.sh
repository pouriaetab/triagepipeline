#!/usr/bin/env bash
# fetch.sh: download NHTSA's two crash-report files and say whether either one changed.
# Usage: bash scripts/fetch.sh [data_folder]
set -euo pipefail

DATA_DIR="${1:-data}"
BASE="https://static.nhtsa.gov/odi/ffdd/sgo-2021-01"
OLD_URL="$BASE/Archive-2021-2025/SGO-2021-01_Incident_Reports_ADS.csv"
NEW_URL="$BASE/SGO-2021-01_Incident_Reports_ADS.csv"

mkdir -p "$DATA_DIR"
changed=0
echo "data folder: $DATA_DIR"

fetch_one() {
    local name="$1"
    local url="$2"
    local dest="$DATA_DIR/$name"
    local tmp="$DATA_DIR/$name.part"

    if ! curl --fail --silent --show-error --location --retry 3 --output "$tmp" "$url"; then
      echo "FAILED      $name   could not download $url" >&2
      rm -f "$tmp"
      exit 22
    fi

    local new_hash
    new_hash=$(sha256sum "$tmp" | cut -d' ' -f1)
    local old_hash=""
    if [ -f "$dest.sha256" ]; then
      old_hash=$(cut -d' ' -f1 "$dest.sha256")
    fi
    if [ "$new_hash" = "$old_hash" ]; then
      echo "unchanged   $name   $new_hash"
      rm -f "$tmp"
    else
      mv "$tmp" "$dest"
      echo "$new_hash   $name" > "$dest.sha256"
      echo "Updated     $name   $new_hash   $(wc -c < "$dest") bytes"
      changed=1
    fi
}

fetch_one ads_old.csv "$OLD_URL"
fetch_one ads_new.csv "$NEW_URL"
if [ "$changed" -eq 1 ]; then
  echo "changed" > "$DATA_DIR/status.txt"
  echo "result: new data"
else
  echo "unchanged" > "$DATA_DIR/status.txt"
  echo "result: no new data"
fi
