#!/bin/bash
# Launch all the functional tests for the seqdd command.
set -e

TEST_SCRIPTS_PATH=$(dirname "$0")

echo "Running functional tests for seqdd"

for test in "$TEST_SCRIPTS_PATH"/test_*.sh; do
    echo "[TEST] $test"
    bash "$test"
done
