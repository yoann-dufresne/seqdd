#!/bin/bash
# Functional test: `url` type — real add + download of an arbitrary URL.
set -e

echo "Running functional test: url (real add + download)"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
reg="$work/.register"
data="$work/data"
url="https://www.ebi.ac.uk/ena/browser/api/fasta/MN908947"

seqdd init --register-location "$reg"
seqdd add -t url -a "$url" --register-location "$reg"

if [[ ! -s "$reg/url.txt" ]]; then
    echo "❌ add did not register the URL" >&2
    exit 1
fi

seqdd download --register-location "$reg" -d "$data" --log-directory "$work/logs" --tmp-directory "$work/tmp"

if [[ -z "$(find "$data" -type f -name 'url0_*' -size +0c 2>/dev/null)" ]]; then
    echo "❌ No non-empty url download found under $data" >&2
    exit 1
fi

echo "✅ url OK"
