# MIMIC-IV MEDS Extraction ETL

[![PyPI - Version](https://img.shields.io/pypi/v/MIMIC-IV-MEDS)](https://pypi.org/project/MIMIC-IV-MEDS/)
[![tests](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/tests.yaml)
[![code-quality](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.11-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/Medical-Event-Data-Standard/MIMIC_IV_MEDS.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/901560093.svg)](https://doi.org/10.5281/zenodo.17535579)

This pipeline extracts the MIMIC-IV dataset (from PhysioNet) into the MEDS format.

## Usage:

```bash
pip install MIMIC_IV_MEDS
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR
```

When you run this, the program will:

1. Download the needed raw MIMIC files for the currently supported version (v3.1) into
    `$MEDS_OUTPUT_DIR/.meds_extract_run/raw_input`. Files that already exist and verify
    against their checksums are skipped, so an interrupted download resumes. Pass
    `download_dest_dir=$RAW_INPUT_DIR` to keep the raw data somewhere durable and reuse it
    across runs.
2. Construct the final MEDS cohort directly from those raw files — all transformations,
    joins, and metadata extraction are declared in
    `src/MIMIC_IV_MEDS/event_configs.yaml` — and write it to `$MEDS_OUTPUT_DIR`
    (`data/` and `metadata/`, alongside the run's intermediate stage outputs).

To run over the publicly available, fully open MIMIC-IV demo dataset (v2.2, no credentials
required):

```bash
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR dataset_key=demo
```

If you already have the raw MIMIC-IV files on disk, skip the download entirely:

```bash
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR do_download=false input_dir=$RAW_INPUT_DIR
```

Run `meds-extract-run --help` for the full set of arguments and options.

Budget for the download rather than the extraction: the raw release is 41 files and 9.9 GiB
from PhysioNet, which rate-limits each connection to roughly 50 KB/s, while the extraction
itself is ~32 minutes and ~20 GB of RAM. Raise `download_concurrency` and keep the raw data
with `download_dest_dir`; see [Expected runtime and compute needs](#expected-runtime-and-compute-needs).

## How this ETL is defined

There is no Python in this package. The entire ETL is one file —
[`src/MIMIC_IV_MEDS/event_configs.yaml`](src/MIMIC_IV_MEDS/event_configs.yaml)
— written in MESSY (MEDS-Extract Specification Syntax YAML):

- a `sources:` block declaring the release versions and where to fetch the raw data
    (PhysioNet for MIMIC-IV itself, checksum-pinned GitHub URLs for the
    [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps);
- an `etl:` block with the run's few knobs;
- one block per raw table describing the events extracted from it, including joins,
    derived columns, and the code-metadata programs that attach descriptions and parent
    vocabulary codes.

`pyproject.toml` registers that file with MEDS-Extract under the name `MIMIC-IV`, which is
what makes `spec=MIMIC-IV` work from anywhere and what supplies the version stamped into
the output's `metadata/dataset.json` (as `<MIMIC-IV release>:<this package's version>`).
Everything the pipeline does is therefore inspectable — and modifiable — in that one YAML;
see [MEDS-Extract's documentation](https://github.com/mmcdermott/MEDS_extract) for the
syntax.

## What goes in the code, and what doesn't

MIMIC carries a lot of columns that are neither the measurement nor metadata about it. Where
each one lands is a modelling decision, so the reasoning is recorded here rather than in the
config. All figures are from the full 3.1 release.

### Demographics

`insurance`, `language`, `marital_status` and `race` are properties of the subject, not
annotations on the admission, so they are emitted as their own events (`INSURANCE//…`,
`LANGUAGE//…`, `MARITAL_STATUS//…`, `RACE//…`) rather than carried as extension columns. MIMIC
records no separate timestamp for them, so they are co-timed with the admission.

Their nulls are **not** coalesced to `UNK`. A null code component drops the row under 0.7, so a
missing demographic produces no event — which is the honest encoding, and avoids minting a
`RACE//UNK` code that would read as an observed category. Null rates: insurance 1.71%, language
0.14%, marital_status 2.49%, race 0%.

### DRG

**`drg_type: HCFA` means MS-DRG**, not the legacy CMS-DRG the name suggests. 302 distinct HCFA
codes fall above 579, inside the MS-DRG-only numbering space (MS-DRG replaced CMS-DRG in FY2008;
MIMIC-IV spans 2008–2022). `APR` is Solventum's proprietary APR-DRG. Both are genuine external
classifications, so each code carries the bare identifier as a parent — `MS-DRG/003`,
`APR-DRG/047` — via a `_self` metadata block.

**`description` is not part of the code.** It is not a function of the code: 87.8% of HCFA
`(drg_type, drg_code)` pairs carry more than one description, up to five, and the variants are
spelling differences for the same DRG — `W MCC` vs `WITH MCC`, `&` vs `AND`, some truncated
near 72 characters. Rendering it into the code split single DRGs across several MEDS codes and
inflated the HCFA vocabulary **1.98×** (1,557 codes for 787 real DRGs). It rides in `text_value`
instead, which keeps the string without letting MIMIC's spelling changes fragment the
vocabulary.

**`drg_severity` and `drg_mortality` stay extension columns.** They are the APR-DRG severity-of-
illness and risk-of-mortality subclasses, computed by the grouper from the coded diagnoses —
external model output, not something observed on the patient — so they are not MEDS
measurements. They remain available for cohort selection. They are not redundant either: 278 of
300 APR codes span all four severity levels, and knowing severity still leaves 66% of
mortality's own entropy. Both are APR-only, hence null on every HCFA row.

Note that the DRG code is itself grouper-assigned. What distinguishes it is that the DRG is an
administrative fact with consequences — it is what was billed — whereas the subclasses are
gradations attached to that assignment.

### Order modifiers

`priority` (`ROUTINE` / `STAT`) joins the `LAB//SPECIMEN_COLLECTED` code only, not
`LAB//RESULT`. Priority modifies how the specimen was collected; the result is simply what was
observed, and is the same measurement however urgently it was drawn. Nothing is lost by leaving
it off the result: `charttime` is never null across all 158M rows, so the collection event is
always emitted. It is coalesced, being 4.8% null.

`route` and `frequency` on `hosp/pharmacy` were considered for the same treatment and
**deliberately left as extension columns**. Adding them takes `MEDICATION//START` from 22,539 to
125,381 codes — **5.56×**, with 56,248 singleton codes — and `frequency` alone accounts for
4.53× of that across 177 values. That fragments the medication vocabulary far more than it
sharpens it.

`icu/inputevents` is untouched for related reasons: `ordercategorydescription` (5 values) and
`statusdescription` (6 values) are closed enums rather than text, they cannot both occupy the
single `text_value` slot on `input_end` (100% of rows carry both), and `rateuom` is 44.7% null
so promoting it would stamp `UNK` into half of all `INFUSION_START` codes. See
[#15](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/15).

## Parallel and multi-node runs

`meds-extract-run` runs the pipeline's stages serially. To parallelize, run the pipeline
step yourself with a `parallelize:` block, using the config the runner writes out:

```bash
# The launchers live in MEDS-transforms; install the extra you need directly.
pip install 'meds-transforms[local-parallelism]' # or [slurm-parallelism]

# The runner writes the pipeline config it is about to use, then runs it. Interrupt it
# after the "Wrote synthesized pipeline config" line if you don't want the serial run.
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR

# Add parallelism and re-run the pipeline directly. This is resumable: stages whose
# outputs already exist are skipped.
cat >>$MEDS_OUTPUT_DIR/.meds_extract_run/pipeline.yaml <<'YAML'
parallelize:
  n_workers: 8
  launcher: joblib
YAML
MEDS_transform-pipeline $MEDS_OUTPUT_DIR/.meds_extract_run/pipeline.yaml
```

That file is self-contained (every path and version inlined), so it is also the place to
change the stage sequence, point a run at different directories, or add
`launcher_params:` for a Slurm launcher.

The raw download can be parallelized in the same spirit — PhysioNet rate-limits each TCP
connection to roughly 50 KB/s but does not throttle aggregate per-IP throughput, so
several connections give a near-linear speedup:

```bash
meds-extract-download spec=MIMIC-IV output_dir=$RAW_INPUT_DIR key=dataset concurrency=8
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR do_download=false input_dir=$RAW_INPUT_DIR
```

## Expected runtime and compute needs

> [!IMPORTANT]
> **The download dominates, not the ETL.** The figures below are for the extraction alone,
> with the raw data already on disk. Fetching MIMIC-IV 3.1 from PhysioNet is **41 files and
> 9.9 GiB**, and PhysioNet rate-limits each TCP connection to roughly 50 KB/s. At the default
> `download_concurrency=4` that is on the order of several hours to a day; a single connection
> would be far worse. Aggregate per-IP throughput is *not* throttled, so raising
> `download_concurrency` speeds this up close to linearly and is the single most effective
> thing you can change.
>
> Download once and keep it. Pass `download_dest_dir=$RAW_INPUT_DIR` so the raw files land
> somewhere durable, then use `do_download=false input_dir=$RAW_INPUT_DIR` for every
> subsequent run. Downloads resume: files that already exist and verify against their
> checksums are skipped, so an interrupted transfer costs only what it had not yet fetched.

Measured on the full MIMIC-IV 3.1 release — all 364,627 subjects — with MEDS-Extract 0.7.0, no
parallelism configured, raw data already downloaded:

|                   |                                          |
| ----------------- | ---------------------------------------- |
| wall time         | **32 min**                               |
| peak RSS          | **20.3 GB**                              |
| MEDS data output  | 4.5 GB (366 shards)                      |
| metadata output   | 8.6 MB                                   |
| whole output tree | 15 GB (includes per-stage intermediates) |

Hardware: 20-core aarch64, 121 GB RAM, local NVMe. The run used ~350–400% CPU, i.e. about four
cores' worth, so it is I/O- and single-thread-bound rather than CPU-bound — adding cores alone
will not help much, and 20 GB of headroom is the real requirement.

Per stage, for anyone budgeting a smaller machine:

| stage                      | duration | peak RSS |
| -------------------------- | -------- | -------- |
| convert_to_parquet         | 4.1 min  | 14.4 GB  |
| split_and_shard_subjects   | 5 s      | 20.3 GB  |
| convert_to_subject_sharded | 8.3 min  | 4.5 GB   |
| convert_to_MEDS_events     | 5.0 min  | 2.6 GB   |
| extract_code_metadata      | 3.6 min  | 8.1 GB   |
| merge_to_MEDS_cohort       | 6.5 min  | 3.9 GB   |
| finalize_MEDS_metadata     | 1 s      | 0.6 GB   |
| finalize_MEDS_data         | 4.4 min  | 2.6 GB   |

Peak memory lives in the early, whole-file stages — `convert_to_parquet` and the shard split —
not in the per-subject work. (Stage peaks are attributed by sampling the process tree and
binning against stage boundaries, so the 5-second `split_and_shard_subjects` figure includes
memory `convert_to_parquet` had not yet returned to the OS; treat those two as one 20 GB
envelope rather than two independent peaks.)

Memory is only weakly sensitive to cohort size under 0.7.0. Measured over a nested subject
ladder, peak RSS ran 3.1 GB at 3,200 subjects and 3.8 GB at 25,600 — a tail growth exponent of
0.12, essentially flat — with the remainder of the full-scale 20.3 GB coming from whole-file
stages that see every row regardless. Wall time, by contrast, is close to linear in subjects.

> [!NOTE]
> These figures are much lower than earlier releases of this ETL reported (~165 GB, ~7 hours).
> Before 0.7.0, `extract_code_metadata` performed a full-dataset scan/unique/collect and
> dominated peak memory — a full run was OOM-killed on this machine holding 115 GB. 0.7.0
> reworked that stage into a shard-scoped map-reduce, cutting its marginal cost by ~45× at no
> measurable runtime cost.

Intermediate stage outputs are retained under the output directory, which is why the tree is
15 GB against 4.5 GB of actual MEDS data. See
[this github issue](https://github.com/Medical-Event-Data-Standard/MEDS_transforms/issues/235) for tracking on ensuring these
directories are automatically cleaned up in the future.

## 📚 Citing this work

If you use this software in your research, please cite it! You can use the **"Cite this repository"** button on GitHub.

The citation information is maintained in the `CITATION.cff` file in this repository.

## 🔧 Common Issues / FAQ

### ❓ Issue: the download fails with a 403 on the full (non-demo) dataset

#### Problem:

MIMIC-IV itself is a credentialed PhysioNet release. A 403 means PhysioNet declined the
request before any data was served.

#### Solution:

Check, in order: that `DATASET_DOWNLOAD_USERNAME` / `DATASET_DOWNLOAD_PASSWORD` are
exported in the shell that runs the command (the demo bucket needs neither, so a working
`dataset_key=demo` run proves nothing about credentials); and that the PhysioNet account
those credentials belong to has a signed data use agreement for MIMIC-IV — access is
per-release, so credentials that work for another dataset will still 403 here. The error
message names which of these applies.
