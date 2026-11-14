# Getting started

## Install

```bash
pip install MIMIC_IV_MEDS
```

The package has two dependencies:

- **`meds-extract[download]`, at least 0.8.1 and below 0.9.** It brings in MEDS-Transforms, polars
    and hydra, and provides `meds-extract-run` and `meds-extract-download`. 0.8.1 is the floor
    because the spec names its joins `_table.joins` (0.8 renamed `_table.join` and rejects the old
    name), and because 0.8.1 is the first release that runs the `etl.post_stages` the spec declares
    and writes the `dataset.json` fields of its `etl:` block. MEDS-Extract 0.x minor releases break
    the spec syntax and the runner CLI, so the ceiling is raised deliberately, in a migration.
- **`sumeds`, at least 0.6.3 and below 0.7.** [suMEDS](https://github.com/rvandewater/SumMEDS)
    provides the `sumeds_enrich` stage that runs at the end of every run (see
    [Code-metadata enrichment](#code-metadata-enrichment)) and the `suMEDS-enrich` command.

Python 3.11.4 up to 3.13 is supported, matching MEDS-Extract.

## PhysioNet access

Every source dataset except the demo is a *credentialed* PhysioNet project. You need a PhysioNet
account that is credentialed, has completed the required training (CITI "Data or Specimens Only
Research"), and has **signed the data use agreement of each project the spec reads**. Access is
granted per project, so an account approved for MIMIC-IV will still get a 403 on an extension it has
not been approved for.

| Spec / mode                      | Projects whose DUA you need                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `spec=MIMIC-IV dataset_key=demo` | none: the [MIMIC-IV demo](https://physionet.org/content/mimic-iv-demo/2.2/) is open access (ODbL)                        |
| `spec=MIMIC-IV`                  | [MIMIC-IV 3.1](https://physionet.org/content/mimiciv/3.1/)                                                               |
| `spec=MIMIC-IV-MNE`              | MIMIC-IV 3.1 **and** [MIMIC-IV-Ext MIMIC-Note-Extract 1.0](https://physionet.org/content/mimic-iv-ext-note-extract/1.0/) |
| MIMIC-IV-ED (in progress)        | MIMIC-IV **and** [MIMIC-IV-ED 2.2](https://physionet.org/content/mimic-iv-ed/2.2/)                                       |
| MIMIC-CXR-DB (in progress)       | MIMIC-IV **and** [MIMIC-CXR-Ext-MIMIC-CXR-DB 1.0](https://physionet.org/content/mimic-cxr-ext-mimic-cxr-db/1.0/)         |

The downloader reads the PhysioNet credentials from two environment variables:

```bash
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
```

They are needed only for the credentialed bucket. A demo run never reads them. A full run without
them fails at the start with a clear missing-variable error. They are never written to the logs or
to the copy of the config stored in the output.

## Run the ETL

### The full release

```bash
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR
```

This does three things:

1. It **downloads** the raw files for the pinned release (MIMIC-IV 3.1) into
    `$MEDS_OUTPUT_DIR/.meds_extract_run/raw_input`, together with ten
    [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (tag `v2.4.0`, checksum-pinned)
    that supply lab descriptions and LOINC/SNOMED/RxNorm parent codes. Files that already exist and
    match their checksums are skipped, so an interrupted download resumes. Pass
    `download_dest_dir=$RAW_INPUT_DIR` to keep the raw data somewhere durable and reuse it across runs.
2. It **builds the MEDS cohort** from those files. Every transformation, join and metadata step is
    declared in `src/MIMIC_IV_MEDS/event_configs.yaml`. The cohort is written to `$MEDS_OUTPUT_DIR`.
3. It **enriches the code metadata** with suMEDS: `metadata/codes.parquet` gains external-ontology
    links, and `metadata/dataset.json` records whether that worked. See
    [Code-metadata enrichment](#code-metadata-enrichment), including how to leave this step out.

Run `meds-extract-run --help` for every argument and option.

### The open demo

The [MIMIC-IV demo](https://physionet.org/content/mimic-iv-demo/2.2/) (v2.2, 100 subjects) is fully
open and needs no credentials:

```bash
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR dataset_key=demo
```

This is what CI runs. It takes a few minutes at most, most of it the download. Extension specs have no demo
bucket: MIMIC-Note-Extract has no demo subset, so `dataset_key=demo` is not available for
`spec=MIMIC-IV-MNE`.

### Raw data you already have

To skip the download, point the runner at a directory that already holds the raw files in
PhysioNet's layout (`hosp/`, `icu/` and the concept-map CSVs at the root):

```bash
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR do_download=false input_dir=$RAW_INPUT_DIR
```

You can also download separately, which makes it easier to raise the number of connections:

```bash
meds-extract-download spec=MIMIC-IV output_dir=$RAW_INPUT_DIR key=dataset concurrency=8
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR do_download=false input_dir=$RAW_INPUT_DIR
```

### An extension spec

```bash
# Needs a PhysioNet account approved for BOTH MIMIC-IV and MIMIC-Note-Extract.
meds-extract-run spec=MIMIC-IV-MNE output_dir=$MEDS_OUTPUT_DIR
```

The spec's download fetches MIMIC-IV and the extension's tables in one go, and the run ends with
the same enrichment as `spec=MIMIC-IV`, using the `MIMIC-IV` rules. With
`do_download=false`, put the extension's files where the download would have put them; for MNE
that is the root of `input_dir`, next to `hosp/` and `icu/`. Each [extension page](extensions/index.md)
gives the details.

## Code-metadata enrichment

The ETL itself writes only parents it can read from MIMIC-IV's own tables or the mimic-code maps.
Links that need an external ontology (the OMOP Gender tags, the ICD hierarchy, official vocabulary
names) are added afterwards by [suMEDS](https://github.com/rvandewater/SumMEDS), from the rules file
this package ships,
[`src/MIMIC_IV_MEDS/sumeds_rules.yaml`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/src/MIMIC_IV_MEDS/sumeds_rules.yaml).
[Base ETL → Enrichment with suMEDS](base-etl.md#enrichment-with-sumeds) lists what the rules add.

**How it runs.** The spec declares suMEDS's `sumeds_enrich` stage as a *post-stage*, after the
canonical MEDS-Extract pipeline:

```yaml
etl:
  post_stages:
    - sumeds_enrich: {rules: MIMIC-IV}
```

`pyproject.toml` registers the rules file in suMEDS's `sumeds.rules` entry-point group under the
spec's name, `MIMIC-IV`, so the stage (and `--rules MIMIC-IV` on any suMEDS command) finds it. The
stage rewrites `metadata/codes.parquet` in place, and the result is still valid MEDS code metadata.
The ontology data comes from suMEDS's versioned ontology packs (sumeds-ontologies, a pinned
release), fetched from GitHub on first use and cached. No OHDSI Athena download is needed.

**What gets recorded.** `metadata/dataset.json` gets a `sumeds_enrichment` record: whether the build
was `enriched`, the rules, the suMEDS version and the coverage.

**When it fails, the run does not.** Enrichment is on by default and soft-fails. If it cannot run
(for example, on a machine without internet access), `codes.parquet` is left exactly as the ETL wrote
it, `dataset.json` records `"enriched": false` with the error and a retry command, and the exit code
is still 0. The warning is in `$MEDS_OUTPUT_DIR/.logs/pipeline.log`. Running `meds-extract-run`
again does **not** retry it, because the stage is marked done. Enrich the build later instead:

```bash
suMEDS-enrich $MEDS_OUTPUT_DIR
# Without internet access, from a copy of the sumeds-ontologies packs directory:
suMEDS-enrich $MEDS_OUTPUT_DIR --ontology-source /path/to/sumeds-ontologies/ontologies --offline
```

**Options.** The stage's options are set at run time through `overrides=`. They need `++`, because
overrides reach every stage of the pipeline:

```bash
# The plain ETL, without enrichment:
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR skip_stages=[sumeds_enrich]
# Make a failed enrichment fail the run (exit code 1):
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR "overrides=['++stage_cfg.strict=true']"
# Also look codes up in your own OHDSI Athena download (this validates the Gender tags too):
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR "overrides=['++stage_cfg.athena_csv=/path/to/athena']"
```

`skip_stages=[sumeds_enrich]` gives the output of the ETL alone, which does not depend on network
access or on the pack release. The demo cohort the tests check and the
[summary-statistics baselines](contributing.md#summary-statistics-baselines) are built this way.

**Clusters without internet access.** Fill a cache on shared storage once, from a login node, then
run offline on the compute nodes with the same cache:

```bash
export XDG_CACHE_HOME=/shared/project/cache
# On a login node:
suMEDS-enrich --prefetch --rules MIMIC-IV
# On a compute node:
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR "overrides=['++stage_cfg.offline=true']"
```

## Output layout

After a run, `$MEDS_OUTPUT_DIR` contains:

```text
$MEDS_OUTPUT_DIR/
├── data/                         # the MEDS data: sharded Parquet, one folder per split
│   ├── train/0.parquet ...
│   ├── tuning/...
│   └── held_out/...
├── metadata/
│   ├── dataset.json              # name, versions, extension-column roles, enrichment record
│   ├── codes.parquet             # one row per code: description, parent_codes, suMEDS columns
│   └── subject_splits.parquet    # subject_id -> train / tuning / held_out
├── .meds_extract_run/
│   ├── raw_input/                # the download (unless download_dest_dir is set)
│   └── pipeline.yaml             # the fully resolved pipeline the runner executed
├── .logs/                        # per-stage completion markers and pipeline.log
└── convert_to_parquet/, convert_to_MEDS_events/, merge_to_MEDS_cohort/, ...
                                  # intermediate output of each stage
```

- **Columns.** Every shard has the MEDS columns `subject_id`, `time`, `code`, `numeric_value` and
    `text_value`. It also has the extension columns the spec declares, such as `hadm_id`,
    `icustay_id`, `seq_num`, `drg_severity` and `route`, and MEDS-Extract's `source_block`, which
    names the config block that produced each row. A column is null on rows whose block does not set
    it.
- **Version stamp.** `metadata/dataset.json` records `dataset_name` (the spec name, e.g.
    `MIMIC-IV`) and `dataset_version` as `<source release>:<package version>`, e.g. `3.1:<version>`,
    or `2.2:<version>` for the demo.
- **Extension-column roles.** `dataset.json` also lists the extension columns by role, as declared in
    the spec's `etl:` block; see
    [Base ETL → Extension columns in dataset.json](base-etl.md#extension-columns-in-datasetjson).
- **Code metadata.** `codes.parquet` holds the ETL's `description` and `parent_codes`. An enriched
    build adds suMEDS's columns, such as `vocabulary_id`, `concept_code` and `parent_names`, and
    appends parents after the ETL's own.
- **Intermediates.** The stage folders are kept, which is why a full run's output tree is about
    15 GB against 4.5 GB of MEDS data. They can be deleted once `data/` and `metadata/` are written.
    Automatic clean-up of intermediates was tracked upstream in
    [MEDS_extract#15](https://github.com/mmcdermott/MEDS_extract/issues/15).
- **Resuming.** Stages whose output already exists are skipped, so re-running the same command
    after an interruption picks up where it stopped.

## Runtime and compute

!!! warning "The download dominates, not the ETL"

    The raw MIMIC-IV 3.1 release is **41 files and 9.9 GiB**, and PhysioNet rate-limits each TCP
    connection to roughly 50 KB/s. A fresh download of the whole release at
    `download_concurrency=8` took **7 h 58 min** (0.35 MiB/s on average), about 15× the 32 minutes
    the extraction itself takes. At the default `download_concurrency=4`, expect closer to a day.

    Total throughput per IP is *not* throttled, so raising `download_concurrency` speeds the
    download up almost linearly. Do not run several PhysioNet downloads at once, though: three
    releases at `concurrency=8` (24 connections) were throttled into `ConnectTimeout` failures.

    Download once and keep the files: pass `download_dest_dir=$RAW_INPUT_DIR`, then use
    `do_download=false input_dir=$RAW_INPUT_DIR` for every later run.

Extraction alone, on the full MIMIC-IV 3.1 release (all 364,627 subjects), with MEDS-Extract 0.7.0,
no parallelism and the raw data already on disk. These figures predate the move to MEDS-Extract 0.8
and the `sumeds_enrich` post-stage, which is not included:

| Measure           | Value                                    |
| ----------------- | ---------------------------------------- |
| wall time         | **32 min**                               |
| peak RSS          | **20.3 GB**                              |
| MEDS data output  | 4.5 GB (366 shards)                      |
| metadata output   | 8.6 MB                                   |
| whole output tree | 15 GB (includes per-stage intermediates) |

The hardware was a 20-core aarch64 machine with 121 GB RAM and a local NVMe disk. The run used
about four cores (350–400% CPU), so it is limited by I/O and single-threaded work, not CPU. More
cores alone will not help much; 20 GB of memory headroom is the real requirement.

| Stage                      | Duration | Peak RSS |
| -------------------------- | -------- | -------- |
| convert_to_parquet         | 4.1 min  | 14.4 GB  |
| split_and_shard_subjects   | 5 s      | 20.3 GB  |
| convert_to_subject_sharded | 8.3 min  | 4.5 GB   |
| convert_to_MEDS_events     | 5.0 min  | 2.6 GB   |
| extract_code_metadata      | 3.6 min  | 8.1 GB   |
| merge_to_MEDS_cohort       | 6.5 min  | 3.9 GB   |
| finalize_MEDS_metadata     | 1 s      | 0.6 GB   |
| finalize_MEDS_data         | 4.4 min  | 2.6 GB   |

Peak memory is reached in the early stages that read whole files (`convert_to_parquet` and the
shard split), not in the per-subject work. Stage peaks were attributed by sampling the process tree,
so the 5-second `split_and_shard_subjects` figure includes memory that `convert_to_parquet` had not
yet released. Treat the two as one 20 GB envelope. Memory barely grows with cohort size (3.1 GB at
3,200 subjects, 3.8 GB at 25,600). Wall time grows roughly linearly with the number of subjects.

!!! note

    Earlier releases of this ETL reported about 165 GB and 7 hours. Before MEDS-Extract 0.7.0,
    `extract_code_metadata` scanned the full dataset in memory; a full run was OOM-killed at 115 GB
    on the machine above. 0.7.0 rewrote that stage as a shard-scoped map-reduce.

For `spec=MIMIC-IV-MNE`, the MNE tables on their own take about 90 s and 1.4 GB of peak memory to
extract.

## Parallel and multi-node runs

`meds-extract-run` runs the stages one after another. To parallelize, run the pipeline yourself
with a `parallelize:` block, starting from the config the runner writes:

```bash
# The launchers live in MEDS-transforms; install the extra you need.
pip install 'meds-transforms[local-parallelism]' # or [slurm-parallelism]

# The runner writes the pipeline config it is about to use, then runs it. Interrupt it
# after the "Wrote synthesized pipeline config" line if you don't want the serial run.
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR

# Add parallelism and re-run the pipeline directly. Stages whose outputs exist are skipped.
cat >>$MEDS_OUTPUT_DIR/.meds_extract_run/pipeline.yaml <<'YAML'
parallelize:
  n_workers: 8
  launcher: joblib
YAML
MEDS_transform-pipeline $MEDS_OUTPUT_DIR/.meds_extract_run/pipeline.yaml
```

The pipeline config lists the `sumeds_enrich` stage too, so this also enriches the code metadata.
`pipeline.yaml` is self-contained, with every path and version inlined. You can also edit it to
change the stage sequence, point the run at other directories, or add `launcher_params:` for a
Slurm launcher.

## Troubleshooting

### The download fails with a 403 on the full dataset

MIMIC-IV is a credentialed PhysioNet release, and a 403 means PhysioNet refused the request before
serving any data. Check these, in order:

1. `DATASET_DOWNLOAD_USERNAME` and `DATASET_DOWNLOAD_PASSWORD` are exported in the shell that runs
    the command. The demo needs neither, so a working `dataset_key=demo` run says nothing about your
    credentials.
2. The PhysioNet account has signed the data use agreement for **this** project. Access is granted
    per project, so credentials that work for MIMIC-IV will still get a 403 on an extension such as
    MIMIC-Note-Extract until that project's DUA is signed too.

The error message says which of the two applies.
