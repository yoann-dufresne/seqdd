#!/bin/bash
# Functional test for the `seqdd init` command.
set -e

echo "Running functional tests for the seqdd init command..."

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
register="$workdir/.register"

# init creates the register directory
seqdd init --register-location "$register"
if [[ ! -d "$register" ]]; then
    echo "❌ Register directory not created: $register" >&2
    exit 1
fi

# a second init without --force must fail (register already present)
if seqdd init --register-location "$register" 2>/dev/null; then
    echo "❌ Re-init without --force should have failed" >&2
    exit 1
fi

# a second init with --force must succeed
seqdd init --force --register-location "$register"

echo "✅ seqdd init OK"
