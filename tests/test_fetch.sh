#!/usr/bin/env bash
# test_fetch.sh: check that scripts/fetch.sh keeps its promises.
# Usage: bash tests/test_fetch.sh
set -uo pipefail

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
failures=0

check() {
    local name="$1"
    shift
    if "$@"; then
      echo "PASS    $name"
    else
      echo "FAIL    $name"
      failures=$((failures + 1))
    fi
}

# 1 and 2: the first run fins new data, the second run finds nothing new
bash scripts/fetch.sh "$TMP/run" > /dev/null
check "first run says changed" grep -qx changed "$TMP/run/status.txt"
bash scripts/fetch.sh "$TMP/run" > /dev/null
check "second run says unchanged" grep -qx unchanged "$TMP/run/status.txt"

# 3 and 4: a wrong address stops with exit code 22 and writes nothing
sed 's/_ADS\.csv"/_ERROR.csv"/' scripts/fetch.sh > "$TMP/broken.sh"
bash "$TMP/broken.sh" "$TMP/bad" > /dev/null 2>&1
code=$?
check "a wrong address stops with exit code 22" test "$code" -eq 22
check "a wrong address writes nothing" test -z "$(ls -A "$TMP/bad")"

echo "$failures failed"
[ "$failures" -eq 0 ]