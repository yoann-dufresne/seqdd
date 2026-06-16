#!/bin/bash
# Functional test: `sequences` type — real add + download from ENA.
set -e

echo "Running functional test: sequences (real add + download)"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
reg="$work/.register"
data="$work/data"
acc="MN908947"

seqdd init --register-location "$reg"
seqdd add -t sequences -a "$acc" --register-location "$reg"

if [[ ! -s "$reg/sequences.txt" ]]; then
    echo "❌ add did not register $acc" >&2
    exit 1
fi

seqdd download --register-location "$reg" -d "$data" --log-directory "$work/logs" --tmp-directory "$work/tmp"

out="$data/$acc/$acc.fa.gz"
if [[ ! -s "$out" ]]; then
    echo "❌ Expected download not found or empty: $out" >&2
    exit 1
fi
gzip -t "$out"
if [[ "$(gzip -dc "$out" | head -c1)" != ">" ]]; then
    echo "❌ Downloaded file is not FASTA: $out" >&2
    exit 1
fi

echo "✅ sequences OK"
