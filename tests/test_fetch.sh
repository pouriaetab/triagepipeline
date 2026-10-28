#!/usr/bin/env bash
# test_fetch.sh: checks that scripts/fetch.sh keeps its four promises:
#   1. a first run into an empty folder says changed
#   2. a second run right after says unchanged
#   3. a wrong address stops the script with exit code 22
#   4. a wrong address leaves nothing behind
# Works in a temporary folder, so the real data folder is never touched.
# Downloads both files twice (about 20 seconds). Prints PASS or FAIL for each check.
# Exit code: 0 if all four pass, 1 if any fails.
#
# Usage: bash tests/test_fetch.sh   (from the repo folder)
set -uo pipefail   # no -e on purpose: check 3 expects a command to fail

TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT   # delete the temporary folder however the test ends
failures=0

# check NAME COMMAND...: runs the command, prints PASS or FAIL, counts failures
check() {
    local name="$1"
    shift   # what is left in "$@" is the command to run
    if "$@"; then
        echo "PASS    $name"
    else
        echo "FAIL    $name"
        failures=$((failures + 1))
    fi
}

# 1 and 2: the first run finds new data, the second run finds nothing new
bash scripts/fetch.sh "$TMP/run" > /dev/null
check "first run says changed" grep -qx changed "$TMP/run/status.txt"
bash scripts/fetch.sh "$TMP/run" > /dev/null
check "second run says unchanged" grep -qx unchanged "$TMP/run/status.txt"

# 3 and 4: a broken copy where both addresses end in _ERROR.csv, a file NHTSA does not have
sed 's/_ADS\.csv"/_ERROR.csv"/' scripts/fetch.sh > "$TMP/broken.sh"
bash "$TMP/broken.sh" "$TMP/bad" > /dev/null 2>&1
code=$?   # save it at once: the next command replaces $?
check "a wrong address stops with exit code 22" test "$code" -eq 22
check "a wrong address writes nothing" test -z "$(ls -A "$TMP/bad")"

echo "$failures failed"
[ "$failures" -eq 0 ]   # the last command sets the exit code: 0 only if nothing failed