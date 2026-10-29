#!/usr/bin/env bash
# upload.sh: stepp 3a of the triage pipeline
# Copies the two NHTSA files from the Codespace to the Databricks volume, but only 
# when step 2 (check_data.py) wrote ok into data/check.txt.
# Needs the Databricks CLI, signed in once with: databricks configure
# Exit code: 0 = both files uploaded, 1 = check.txt is not ok (nothing uploaded).
#
# Useage: bash scripts/upload.sh [data_folder]
set -euo pipefail

DATA_DIR="${1:-data}"
VOLUME="dbfs:/Volumes/workspace/triage/nhtsa" # catalog workspace, schema triage, volume nhtsa

grep -qx ok "$DATA_DIR/check.txt" || { echo "check.txt is not ok: nothing uploaded" >&2; exit 1; }

for f in ads_old.csv ads_new.csv; do
  databricks fs cp --overwrite "$DATA_DIR/$f" "$VOLUME/$f"
done
echo "uploaded both files to $VOLUME"