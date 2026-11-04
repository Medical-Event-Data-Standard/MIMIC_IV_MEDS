#!/usr/bin/env bash
# Regenerate or check the summary-stats baselines in tests/regression/.
#
# Each baseline (`<kind>_summary_stats.json`) is a meds_summary_stats fingerprint of the MEDS dataset
# this checkout builds: counts, code-vocabulary digests, value and time distributions, splits and
# schema. No code string, no cell of fewer than 20 subjects and no minimum or maximum is written, which
# is what makes it safe to commit (https://github.com/Medical-Event-Data-Standard/meds_summary_stats).
# Regenerate it in every pull request that changes the ETL's output, so the diff shows what changed.
#
# Usage:
#   scripts/summary_stats.sh KIND --input-dir RAW_DIR [--output-dir DIR]  build with this checkout, then update
#   scripts/summary_stats.sh KIND --meds-dir MEDS_DIR                     update from an existing build
#   scripts/summary_stats.sh KIND ... --compare                           compare with the baseline instead
#
# KIND is one of: full demo. Updating prints how the new statistics differ from the old baseline.
# --compare writes nothing and exits 1 if tests/regression/policy.yaml flags an error.
set -euo pipefail

# Pinned together with the committed baselines: comparison refuses to diff across stats_schema_version.
STATS_SPEC="meds_summary_stats==0.0.3"
KINDS="full demo"
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REGRESSION_DIR="$REPO_ROOT/tests/regression"

die() {
  echo "error: $*" >&2
  exit 2
}

# How this repository builds each kind of dataset: `build KIND INPUT_DIR OUTPUT_DIR`, run from the
# repository root. Prints the root of the MEDS dataset it wrote (the directory holding data/ and
# metadata/). The build uses this checkout and its locked dependencies.
build() {
  local kind=$1 input=$2 output=$3
  case $kind in
    full)
      uv run --frozen meds-extract-run spec=MIMIC-IV do_download=false input_dir="$input" output_dir="$output" >&2 \
        && echo "$output"
      ;;
    demo)
      uv run --frozen meds-extract-run spec=MIMIC-IV dataset_key=demo output_dir="$output" ${input:+do_download=false} ${input:+"input_dir=$input"} >&2 \
        && echo "$output"
      ;;
    *) die "unknown kind '$kind' (expected one of: $KINDS)" ;;
  esac
}

stats() {
  uvx -q --python 3.12 --from "$STATS_SPEC" "$@"
}

usage() {
  awk 'NR > 1 && !/^#/ { exit } NR > 1 { sub(/^# ?/, ""); print }' "${BASH_SOURCE[0]}"
  exit 2
}

[[ $# -ge 1 ]] || usage
kind=$1
shift
input_dir="" output_dir="" meds_dir="" compare=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --input-dir) input_dir=${2:?--input-dir needs a value}; shift 2 ;;
    --output-dir) output_dir=${2:?--output-dir needs a value}; shift 2 ;;
    --meds-dir) meds_dir=${2:?--meds-dir needs a value}; shift 2 ;;
    --compare) compare=true; shift ;;
    -h | --help) usage ;;
    *) die "unknown argument '$1'" ;;
  esac
done
case " $KINDS " in
  *" $kind "*) ;;
  *) die "unknown kind '$kind' (expected one of: $KINDS)" ;;
esac
baseline="$REGRESSION_DIR/${kind}_summary_stats.json"
policy="$REGRESSION_DIR/policy.yaml"

cd "$REPO_ROOT"
if [[ -z $meds_dir ]]; then
  [[ -n $input_dir || $kind == demo ]] || die "pass --input-dir (raw release) or --meds-dir (existing build)"
  if [[ -z $output_dir ]]; then
    output_dir="${TMPDIR:-/tmp}/$(basename "$REPO_ROOT")-$kind-$(git rev-parse --short HEAD)"
  fi
  [[ -e $output_dir ]] && die "$output_dir already exists; pass it as --meds-dir to reuse a finished build"
  echo "Building the $kind dataset into $output_dir" >&2
  meds_dir=$(build "$kind" "$input_dir" "$output_dir")
fi
[[ -f $meds_dir/metadata/dataset.json ]] || die "$meds_dir is not a MEDS dataset (no metadata/dataset.json)"
echo "Summarizing $meds_dir ($(tr -d '\n' <"$meds_dir/metadata/dataset.json"))" >&2

current=$(mktemp "${TMPDIR:-/tmp}/summary_stats.XXXXXX")
trap 'rm -f "$current"' EXIT
stats extract_MEDS_summary_stats "$meds_dir" --output "$current"

if $compare; then
  [[ -f $baseline ]] || die "no baseline at $baseline"
  stats compare_MEDS_summary_stats "$baseline" "$current" --config "$policy"
  exit
fi

if [[ -f $baseline ]]; then
  echo "Changes against the previous baseline:" >&2
  stats compare_MEDS_summary_stats "$baseline" "$current" --config "$policy" --fail-on never
fi
mkdir -p "$REGRESSION_DIR"
cp "$current" "$baseline"
echo "Wrote ${baseline#"$REPO_ROOT"/}" >&2
