#!/bin/bash
# Functional test: `logan` type — real add + download of Logan contigs from S3.
set -e

echo "Running functional test: logan (real add + download)"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
reg="$work/.register"
data="$work/data"
acc="SRR000001"

seqdd init --register-location "$reg"
seqdd add -t logan -a "$acc" --register-location "$reg"

if [[ ! -s "$reg/logan.txt" ]]; then
    echo "❌ add did not register $acc" >&2
    exit 1
fi

seqdd download --register-location "$reg" -d "$data" --log-directory "$work/logs" --tmp-directory "$work/tmp"

if [[ -z "$(find "$data" -type f -name '*.zst' -size +0c 2>/dev/null)" ]]; then
    echo "❌ No non-empty Logan .zst download found under $data" >&2
    exit 1
fi

echo "✅ logan OK"
