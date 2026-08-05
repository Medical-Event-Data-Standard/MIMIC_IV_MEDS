#!/usr/bin/env bash
# Build a working environment for a *released* version of this ETL.
#
#   compat/build_version.sh <version> [target_dir]
#
# e.g. compat/build_version.sh 0.1.2 /tmp/mimic-0.1.2
#      /tmp/mimic-0.1.2/.venv/bin/MEDS_extract-MIMIC_IV root_output_dir=$OUT do_demo=True
#
# Why this exists: seven of the ten released versions no longer install into a working ETL,
# because their upstream specifier is unbounded and today's resolver picks a release years
# newer than the one the version was written against. The ETL code in those releases is fine.
# `compat/constraints/<version>.txt` records the upstream that actually works, so a published
# version can still be rebuilt and audited.
#
# This script deliberately installs the released artifact from PyPI, NOT the working tree --
# the point is to reproduce what was published.
set -euo pipefail

VERSION="${1:-}"
TARGET="${2:-}"

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -z $VERSION ]]; then
    echo "usage: $0 <version> [target_dir]" >&2
    echo >&2
    echo "available:" >&2
    for f in "$HERE"/constraints/*.txt; do
        b="$(basename "$f" .txt)"
        echo "  $b  -- $(grep -m1 -oE '^[a-z-]+==[0-9.]+' "$f")" >&2
    done
    exit 2
fi

CONSTRAINTS="$HERE/constraints/$VERSION.txt"
if [[ ! -f $CONSTRAINTS ]]; then
    echo "ERROR: no constraints recorded for version '$VERSION'" >&2
    echo "       known: $(cd "$HERE/constraints" && ls *.txt | sed 's/\.txt//' | tr '\n' ' ')" >&2
    exit 2
fi

TARGET="${TARGET:-$(mktemp -d -t "mimic-iv-meds-$VERSION-XXXX")}"
mkdir -p "$TARGET"

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: uv is required (https://docs.astral.sh/uv/)" >&2
    exit 1
fi

echo "Building MIMIC-IV-MEDS $VERSION in $TARGET"
# 3.12 first, 3.11 as a fallback: the oldest releases predate 3.12 wheels for parts of their
# numeric stack.
for py in 3.12 3.11; do
    rm -rf "$TARGET/.venv"
    uv venv --python "$py" "$TARGET/.venv" >/dev/null 2>&1 || continue
    if VIRTUAL_ENV="$TARGET/.venv" uv pip install \
        "MIMIC-IV-MEDS==$VERSION" -c "$CONSTRAINTS" >"$TARGET/build.log" 2>&1; then
        echo "  installed on Python $py"
        VIRTUAL_ENV="$TARGET/.venv" uv pip freeze >"$TARGET/requirements.frozen.txt"
        echo "  frozen closure -> $TARGET/requirements.frozen.txt"
        echo
        echo "Run the demo with:"
        echo "  PATH=\"$TARGET/.venv/bin:\$PATH\" \\"
        echo "    $TARGET/.venv/bin/MEDS_extract-MIMIC_IV root_output_dir=\$OUT do_demo=True"
        echo
        echo "(PATH matters: the ETL shells out to MEDS_transform-pipeline by bare name.)"
        exit 0
    fi
done

echo "ERROR: install failed on both 3.12 and 3.11; see $TARGET/build.log" >&2
exit 1
