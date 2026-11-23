# MIMIC-IV MEDS

**MIMIC-IV MEDS** is an ETL that turns the [MIMIC-IV](https://physionet.org/content/mimiciv/3.1/)
electronic health record database into the Medical Event Data Standard (MEDS). It ships no Python
of its own. The whole ETL is one declarative YAML file,
[`src/MIMIC_IV_MEDS/event_configs.yaml`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/src/MIMIC_IV_MEDS/event_configs.yaml),
which [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) runs: it downloads the raw files
from PhysioNet, extracts the events, attaches code metadata and writes a MEDS cohort. As its last
step, [suMEDS](https://github.com/rvandewater/SumMEDS) adds external-ontology links to the code
metadata, from rules this package ships.

## MEDS in one paragraph

[MEDS](https://medical-event-data-standard.github.io/) is a minimal data standard for machine
learning on EHR data. A subject's record is a time-ordered list of *measurements*, one per row,
each with a `subject_id`, a `time` (null for static facts), a string `code` naming what was
observed, and optionally a `numeric_value` and a `text_value`. Datasets add their own extension
columns, such as `hadm_id` here. Data is stored as sharded Parquet under `data/`. Next to it,
`metadata/` holds `dataset.json`, `codes.parquet` (a description and parent vocabulary codes for
each code) and `subject_splits.parquet`. Because every MEDS dataset has this shape, MEDS tools such
as task extraction, tokenization and model training work on any of them without changes.

## Available specs

MEDS-Extract runs a *spec*, a registered MESSY YAML file, selected with `spec=<name>`. This package
registers the following:

| Spec           | What it is                                                           | Status                                                                                      | PhysioNet access needed                   |
| -------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------- | ----------------------------------------- |
| `MIMIC-IV`     | The base ETL: MIMIC-IV `hosp/` and `icu/` (3.1; demo 2.2)            | Available                                                                                   | MIMIC-IV (or none for `dataset_key=demo`) |
| `MIMIC-IV-MNE` | Base + the structured discharge-summary tables of MIMIC-Note-Extract | Available ([docs](extensions/mne.md))                                                       | MIMIC-IV **and** MIMIC-Note-Extract       |
| MIMIC-IV-ED    | Base + the MIMIC-IV-ED emergency department module                   | In progress ([#74](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/74)) | MIMIC-IV **and** MIMIC-IV-ED              |
| MIMIC-CXR-DB   | Base + the MIMIC-CXR-DB chest-radiograph linkage                     | In progress                                                                                 | MIMIC-IV **and** MIMIC-CXR-DB             |

The extensions are separate specs, not flags, because MEDS-Extract has no optional tables and each
companion dataset needs its own PhysioNet data use agreement. [Extensions](extensions/index.md)
explains how they work.

## Quick start

```bash
pip install MIMIC_IV_MEDS

# The open MIMIC-IV demo (v2.2): no PhysioNet account needed.
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR dataset_key=demo

# The full release (v3.1): needs a credentialed PhysioNet account with the MIMIC-IV DUA signed.
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
meds-extract-run spec=MIMIC-IV output_dir=$MEDS_OUTPUT_DIR
```

[Getting started](getting-started.md) covers access, reusing a local copy of the raw data, the
output layout, and how long the download and extraction take.

## Where to go next

- [Getting started](getting-started.md): installation, PhysioNet access, commands, code-metadata
    enrichment and output.
- [Base ETL](base-etl.md): which MIMIC-IV tables are used, how codes and times are built, and why.
- [Extensions](extensions/index.md): the opt-in specs, their sources and their caveats.
- [Sources and citations](sources.md): every dataset and work used, with licences and BibTeX.
- [Contributing](contributing.md): tests, the summary-statistics baselines, the synthetic-data CI and
    the generated specs.

## Citing

If you use this ETL, please cite it via the repository's
[`CITATION.cff`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/CITATION.cff)
(the "Cite this repository" button on GitHub, or the Zenodo DOI
[10.5281/zenodo.17535579](https://doi.org/10.5281/zenodo.17535579)). Please also cite the datasets
you extracted; see [Sources and citations](sources.md).
