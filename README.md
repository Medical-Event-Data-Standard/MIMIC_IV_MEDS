# MIMIC-IV MEDS Extraction ETL

[![PyPI - Version](https://img.shields.io/pypi/v/MIMIC-IV-MEDS)](https://pypi.org/project/MIMIC-IV-MEDS/)
[![tests](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/tests.yaml)
[![code-quality](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.11-blue?logo=python&logoColor=white)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/Medical-Event-Data-Standard/MIMIC_IV_MEDS.svg)](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/901560093.svg)](https://doi.org/10.5281/zenodo.17535579)

This pipeline extracts the MIMIC-IV dataset (from PhysioNet) into the MEDS format, optionally
together with companion datasets such as MIMIC-Note-Extract.

**Documentation: <https://mimic-iv-meds.readthedocs.io>**

## Usage:

```bash
pip install MIMIC_IV_MEDS
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR
```

This one command runs the ETL and then, as its last step, adds external-ontology links to
`metadata/codes.parquet` with [suMEDS](https://github.com/rvandewater/SumMEDS) (installed with this
package; see [Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)). For the
plain ETL, add `skip_stages=[sumeds_enrich]`.

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
3. Enrich `metadata/codes.parquet` in place with the suMEDS rules this package ships, and record
    the enrichment in `metadata/dataset.json`.

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

## Documentation

The full documentation is at **<https://mimic-iv-meds.readthedocs.io>**. It is the canonical
place for the ETL's design and for the extensions:

- [Getting started](https://mimic-iv-meds.readthedocs.io/en/latest/getting-started/): PhysioNet
    access per spec, commands, suMEDS enrichment, output layout, runtime.
- [Base ETL](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/): every table, code and
    timing rule, the reasoning behind the modelling decisions summarized below, the code metadata
    and what suMEDS adds to it, and the extension-column roles in `dataset.json`.
- [Extensions](https://mimic-iv-meds.readthedocs.io/en/latest/extensions/): the opt-in specs
    (`MIMIC-IV-MNE`; MIMIC-IV-ED and MIMIC-CXR-DB in progress), how they are built, and how to add
    one.
- [Sources and citations](https://mimic-iv-meds.readthedocs.io/en/latest/sources/): every dataset
    and work used, with licences and BibTeX.
- [Contributing](https://mimic-iv-meds.readthedocs.io/en/latest/contributing/): tests, the
    summary-statistics baselines, the synthetic-data CI and the generated specs.

The site's source is in [`docs/`](docs/); preview it with `uv run --group docs zensical serve`.

## How this ETL is defined

There is no Python in this package. The entire ETL is one file —
[`src/MIMIC_IV_MEDS/event_configs.yaml`](src/MIMIC_IV_MEDS/event_configs.yaml)
— written in MESSY (MEDS-Extract Specification Syntax YAML):

- a `sources:` block declaring the release versions and where to fetch the raw data
    (PhysioNet for MIMIC-IV itself, checksum-pinned GitHub URLs for the
    [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps);
- an `etl:` block with the run's few knobs, the roles of the extension columns for
    `metadata/dataset.json`, and the suMEDS enrichment that runs after the pipeline;
- one block per raw table describing the events extracted from it, including joins,
    derived columns, and the code-metadata programs that attach descriptions and parent
    vocabulary codes.

`pyproject.toml` registers that file with MEDS-Extract under the name `MIMIC-IV`, which is
what makes `spec=MIMIC-IV` work from anywhere and what supplies the version stamped into
the output's `metadata/dataset.json` (as `<MIMIC-IV release>:<this package's version>`).
Everything the pipeline does is therefore inspectable — and modifiable — in that one YAML;
see [MEDS-Extract's documentation](https://github.com/mmcdermott/MEDS_extract) for the
syntax.

Add-on datasets that need their own PhysioNet credentials are kept out of that file, because
MEDS-Extract fails on any configured table whose raw file is missing. Each lives in a fragment
under [`src/MIMIC_IV_MEDS/extensions/`](src/MIMIC_IV_MEDS/extensions/), and
`scripts/build_extension_specs.py` composes the base with it into a generated, separately
registered spec (`event_configs_<name>.yaml`). Never edit the generated file: change the base or
the fragment and re-run the script; `tests/extension_specs_test.py` fails if you forget. The
extensions so far are [MIMIC-Note-Extract](#optional-extension-mimic-note-extract) and
[MIMIC-IV-ED](#optional-extension-mimic-iv-ed). See
[Extensions](https://mimic-iv-meds.readthedocs.io/en/latest/extensions/) for the mechanism and
how to add one. 

## What goes in the code, and what doesn't

MIMIC carries many columns that are neither the measurement nor metadata about it, and where each
one lands is a modelling decision. The decisions are summarized here; the reasoning and the
supporting figures are in
[Base ETL → Modelling decisions](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#modelling-decisions).

### Demographics

`insurance`, `language`, `marital_status` and `race` are their own events (`INSURANCE//…`, etc.),
co-timed with the admission. Nulls are **not** coalesced to `UNK`: a missing demographic produces
no event. [Details.](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#demographics)

### DRG

`drg_type: HCFA` means MS-DRG; each DRG code gets an `MS-DRG/…` or `APR-DRG/…` parent. The DRG
title rides in `text_value`, not the code, because its spelling varies within one DRG.
`drg_severity` and `drg_mortality` stay extension columns.
[Details.](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#drg)

### OMR

`omr` numbers go in `numeric_value`; a blood pressure becomes `<result_name>//SYSTOLIC` and
`//DIASTOLIC` events, carrying `seq_num`; anything else stays in `text_value`. Three blocks split the
rows via null codes, so three `dropped N/M rows with null code` warnings per shard are expected.
This changed the `omr` vocabulary relative to earlier versions.
[Details.](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#omr)

### Order modifiers

Lab `priority` is in `LAB//SPECIMEN_COLLECTED` only. Pharmacy `route` and `frequency`, and the
`icu/inputevents` order and status descriptions, stay extension columns (see
[#15](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/15)).
[Details.](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#order-modifiers)

## Optional extension: MIMIC-Note-Extract

[MIMIC-IV-Ext MIMIC-Note-Extract](https://physionet.org/content/mimic-iv-ext-note-extract/1.0/)
(MNE, v1.0; [paper](https://arxiv.org/abs/2611.04685)) structures 13 sections of the MIMIC-IV-Note
v2.2 discharge summaries into tables keyed by `hadm_id`, using rules plus an LLM. The spec
`MIMIC-IV-MNE` is the full `MIMIC-IV` ETL plus those tables:

```bash
# Needs a PhysioNet account approved for BOTH MIMIC-IV and MIMIC-Note-Extract.
meds-extract-run spec=MIMIC-IV-MNE output_dir=$MEDS_OUTPUT_DIR
```

With `do_download=false`, put MNE's `*.csv.gz` files at the root of `input_dir`, next to `hosp/`
and `icu/` (that is where the download puts them). There is no demo subset of MNE, so
`dataset_key=demo` is not available for this spec.

In short: every MNE event is timed at `hosp/admissions.dischtime` (MNE has no timestamps, and it
was all written at discharge), every code starts with `NOTE//`, and MNE's free text rides in
`text_value`. It adds 17.6 M events to 137,362 subjects, covering 55.1% of admissions — mostly
before 2020. The
[MIMIC-IV-MNE page](https://mimic-iv-meds.readthedocs.io/en/latest/extensions/mne/) has the full
code table, the decisions behind it, the caveats, what is not extracted and why, and the
validation results.

## Optional extension: MIMIC-IV-ED

[MIMIC-IV-ED v2.2](https://physionet.org/content/mimic-iv-ed/2.2/) holds about 425k emergency
department stays from 2011 to 2019: arrival and disposition, triage, vital signs charted during
the stay, ED diagnoses, the medication reconciliation, and Pyxis dispensations. The spec
`MIMIC-IV-ED` is the full `MIMIC-IV` ETL plus those tables. Unlike MNE it has a public demo, so
`dataset_key=demo` works:

```bash
# Needs a PhysioNet account approved for BOTH MIMIC-IV and MIMIC-IV-ED.
meds-extract-run spec=MIMIC-IV-ED output_dir=$MEDS_OUTPUT_DIR
# The open MIMIC-IV demo plus the open MIMIC-IV-ED demo; no credentials needed.
meds-extract-run spec=MIMIC-IV-ED output_dir=$MEDS_OUTPUT_DIR dataset_key=demo
```

Every code starts with `ED//`. Triage events are timed at ED arrival; diagnoses and disposition at
ED departure, so they cannot leak into the stay. Against the full release the extension adds
**18.8 M events** to 205,504 subjects. 79,370 of the 141,175 subjects without any hospital
admission gain ED events. The ED tables take ~5 min and under 1 GB to extract on their own.

ED detail exists only for 2011–2019, so its presence is a proxy for calendar period. Count ED
visits with the base's `TRANSFER_TO//ED//…`, not with `ED//ARRIVAL`. The modelling decisions,
caveats and validation are in [`docs/extensions/ed.md`](docs/extensions/ed.md).

## Code metadata

`metadata/codes.parquet` gives every code a `description` and, where a standard vocabulary
applies, `parent_codes` in OMOP form (`LOINC/2160-0`, `ICD10CM/I10`, `NDC/…`). All of it is
declared in `event_configs.yaml`: dictionary-table joins at run time (MIMIC-IV's `d_*` tables and
the mimic-code concept maps), and `_self` templates paraphrasing the
[MIMIC-IV documentation](https://mimic.mit.edu/docs/iv/) for every other code. Literal codes
(`ED_REGISTRATION`, `MEDS_BIRTH`, …) are rendered from a constant column because MEDS-Extract
attaches metadata only through a code's components. Every parent the ETL writes is read from
MIMIC-IV's own tables or the mimic-code maps; links that need an external ontology (the OMOP Gender
tags, the ICD pack hierarchy) come from suMEDS instead (next section). On the demo (2.2), 10,250 of
10,254 codes are described and 7,899 have a parent code. The routes and what is left uncovered are
documented under
[Base ETL → Code metadata](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#code-metadata).

## Enriching code metadata with suMEDS

External-ontology links are not written by the ETL. The package ships
[`src/MIMIC_IV_MEDS/sumeds_rules.yaml`](src/MIMIC_IV_MEDS/sumeds_rules.yaml), a
[suMEDS](https://github.com/rvandewater/SumMEDS) rules file registered in suMEDS's `sumeds.rules`
entry-point group under the same name as the MEDS-Extract spec, `MIMIC-IV`. The spec declares
suMEDS's `sumeds_enrich` stage after the canonical pipeline (`etl.post_stages`), so
`meds-extract-run spec=MIMIC-IV` enriches the finished `metadata/codes.parquet` in place, and it
stays valid MEDS code metadata. `--rules MIMIC-IV` works for any suMEDS command. The ontology data
comes from suMEDS's versioned ontology packs (sumeds-ontologies, pinned release), fetched from GitHub
on first use and cached; no Athena download is needed.

Whether a build was enriched is recorded in `metadata/dataset.json`, under
`sumeds_enrichment.enriched`, with the rules, the suMEDS version and the coverage. Enrichment is on
by default, and a failed enrichment (for example, on a server without internet access) does not fail
the ETL: `metadata/codes.parquet` is left as the ETL wrote it, `dataset.json` records
`"enriched": false` with the error, and the exit code is still 0. The warning, with the command that
finishes the enrichment, is in `$MEDS_OUTPUT_DIR/.logs/pipeline.log`. Running `meds-extract-run`
again does not retry it (the stage is marked done); enrich the build afterwards, with the same rules
and options, with:

```bash
suMEDS-enrich $MEDS_OUTPUT_DIR
# Without internet access, from a copy of the sumeds-ontologies packs directory:
suMEDS-enrich $MEDS_OUTPUT_DIR --ontology-source /path/to/sumeds-ontologies/ontologies --offline
```

The stage's options are set at run time through `overrides=`, with `++` because the overrides reach
every stage of the pipeline:

```bash
# Without enrichment:
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR skip_stages=[sumeds_enrich]
# Make a failed enrichment fail the run (exit code 1):
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR "overrides=['++stage_cfg.strict=true']"
# Also look codes up in your own OHDSI Athena download (this validates the Gender tags too):
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR "overrides=['++stage_cfg.athena_csv=/path/to/athena']"
```

On a cluster whose compute nodes have no internet access, fill a cache on shared storage once from a
login node, then run offline on the compute nodes with the same cache:

```bash
export XDG_CACHE_HOME=/shared/project/cache
# On a login node:
suMEDS-enrich --prefetch --rules MIMIC-IV
# On a compute node:
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR "overrides=['++stage_cfg.offline=true']"
```

For MIMIC-IV, the rules add the OMOP Gender tags (`Gender//F`, `Gender//M`), the dotted ICD
vocabulary code and its pack ancestors after the ETL's own reference, same-dataset ICD parents, and
official names in `parent_names`. Enrichment never removes or rewrites the ETL's own parents or
descriptions. The full list, with the demo coverage, is under
[Base ETL → Enrichment with suMEDS](https://mimic-iv-meds.readthedocs.io/en/latest/base-etl/#enrichment-with-sumeds).

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

The pipeline config lists the `sumeds_enrich` stage too, so this also enriches the code metadata.

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
> 9.9 GiB**, and PhysioNet rate-limits each TCP connection to roughly 50 KB/s.
>
> Measured on a fresh download of the whole release at `download_concurrency=8`: **7 h 58 min**,
> averaging 0.35 MiB/s. That is roughly **15× the 32 minutes the extraction itself takes**. At
> the default `download_concurrency=4` expect something closer to a day; a single connection
> would be far worse. Aggregate per-IP throughput is *not* throttled, so raising
> `download_concurrency` speeds this up close to linearly and is the single most effective
> thing you can change.
>
> Do not run several PhysioNet downloads at once to go faster. Three concurrent releases at
> `concurrency=8` — 24 connections — got this machine throttled into `ConnectTimeout` failures
> and had to be restarted one at a time.
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
[this github issue](https://github.com/mmcdermott/MEDS_extract/issues/15) for tracking on ensuring these
directories are automatically cleaned up in the future.

## Summary statistics

[`tests/regression/demo_summary_stats.json`](tests/regression/demo_summary_stats.json) and
[`tests/regression/full_summary_stats.json`](tests/regression/full_summary_stats.json) are
[meds_summary_stats](https://github.com/Medical-Event-Data-Standard/meds_summary_stats) fingerprints
of what this ETL builds from the open MIMIC-IV demo and from the full MIMIC-IV 3.1 release. They name
no code and report no cell of fewer than 20 subjects and no minimum or maximum, which is what makes
them safe to publish. The Tests workflow fails when the demo no longer matches its baseline, as
decided by [`tests/regression/policy.yaml`](tests/regression/policy.yaml). CI cannot rebuild the
credentialed release, so regenerate both in every pull request that changes the output:

```bash
# The demo: downloaded and built with this checkout
scripts/summary_stats.sh demo
# The full release, built from a copy on disk, or summarized from an existing build
scripts/summary_stats.sh full --input-dir $RAW_INPUT_DIR
scripts/summary_stats.sh full --meds-dir $MEDS_OUTPUT_DIR
```

Both baselines are built with `skip_stages=[sumeds_enrich]`; summarize a build made the same way.
`--compare` checks a build without writing anything. See
[Contributing → Summary-statistics baselines](https://mimic-iv-meds.readthedocs.io/en/latest/contributing/#summary-statistics-baselines).

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
