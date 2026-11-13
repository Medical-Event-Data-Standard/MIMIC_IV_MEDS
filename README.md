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
[MIMIC-IV-ED](#optional-extension-mimic-iv-ed).

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

### OMR

**`omr` results are numbers, so they go in `numeric_value`.** `hosp/omr.result_value` is a varchar
that holds two kinds of result: a single number (weight, height, BMI, ...) and a blood pressure
written as `systolic/diastolic`. Earlier versions copied the string into `text_value` and left
`numeric_value` null for every `omr` row, which hid these measurements from everything downstream
that reads numbers: value binning, feature aggregation, threshold predicates. On the full v3.1
release the raw table has 7.75 M rows: 4.91 M plain numbers, 2.84 M blood pressures, and 1,133
other strings (a censored `eGFR` such as `>60`, a handful of odd weight and height entries);
same-day repeats of one value collapse at the merge stage, which leaves 7.63 M distinct events.

**Three blocks partition the rows.** The row classes are computed once, in `_table.cols`
(`is_number`, `bp_systolic`, `bp_diastolic`), and the blocks only read them, so the patterns
cannot drift apart. A plain decimal is cast into `numeric_value` under the bare code, with
`text_value` left null for it (`labevents` and `chartevents` keep the raw string next to the
number; here the string carries nothing the number does not). A blood pressure becomes two
events, `<result_name>//SYSTOLIC` and `<result_name>//DIASTOLIC`, each with its number, the way
`icu/chartevents` already keeps systolic and diastolic as two items. Anything else stays in
`text_value` under the bare code as before, so a code such as `eGFR` can carry both numeric and
text rows. A block routes a row away by giving it a null code, which MEDS-Extract drops and
reports per shard as `dropped N/M rows with null code`: three such warnings per shard for this
table are expected and are not a data loss.

**`seq_num` rides on the two blood-pressure blocks.** `omr` is dated to the day, so several
readings on one day share one timestamp. `seq_num` is unique per (subject, day, result name) in
the raw table; carried as an extension column on the `SYSTOLIC` and `DIASTOLIC` events (null on
every other row), it keeps each systolic/diastolic pair recoverable and stops the merge stage's
`unique_by: "*"` de-duplication from collapsing same-day components that happen to share a value.
On the full release 4.6% of blood-pressure subject-days carry more than one reading (123,740 of
2,710,497 days; 257,684 readings), and 12,648 readings that repeat a same-day value would otherwise
collapse. The bare `omr` block does not carry it, so its existing de-duplication of same-day repeat
weights is unchanged.

**This changes the `omr` vocabulary.** `Blood Pressure` becomes `//SYSTOLIC` and `//DIASTOLIC`
codes, and weight, BMI and height move from `text_value` to `numeric_value`, so cohorts built
before and after are not comparable on them.

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
`dataset_key=demo` is not available for this spec, and CI checks it with synthetic MNE rows over
the MIMIC-IV demo (`tests/e2e_mne_synthetic_test.py`). Against the full release the extension adds
**17.6 M events** to 137,362 subjects; the MNE tables take ~90 s and 1.4 GB to extract on their own.

**What it covers.** MNE reaches 300,663 of MIMIC-IV 3.1's 546,028 admissions (55.1%) and 64.6% of
ICU stays, but unevenly: 61–68% of admissions from ~2008–2019 and only 7% from ~2020–2022, which
MIMIC-IV 3.x added and which have no notes. It adds what MIMIC has no structured table for — past
medical history, home and discharge medications, allergies, the physical examination (including
vital signs for the 78% of non-ICU admissions whose summary records them, where `hosp` has no
inpatient vitals), chief complaint, clinician-written discharge diagnoses and condition at
discharge.

**Every event is timed at `hosp/admissions.dischtime`.** MNE has no timestamps (the note's own are
redacted), and all of it was written down at discharge; placing any of it earlier in the stay would
leak. This is the rule `DIAGNOSIS` and `DRG` already follow. It means a prediction made during a
stay sees MNE only through *earlier* admissions — 45.9% of admissions and 40.0% of ICU stays have
one. The exam's own `timepoint` (ADMISSION, DISCHARGE, INTERIM, UNKNOWN) is kept in the code.

**Codes come from MNE's closed enums; free text rides in `text_value`.** MNE does not map its text to
any vocabulary: there are 234k distinct history concepts, 257k diagnosis titles and 50k medication
names. Rendering them into codes would split the vocabulary far more than the `DRG` descriptions
did. Every code starts with `NOTE//`, so events extracted by an LLM (item-level accuracy 92.2–99.8%
per section in the MNE paper) can be dropped wholesale.

| Code                                                                           | Value                                                                                | Source table                                        |
| ------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | --------------------------------------------------- |
| `NOTE//PAST_MEDICAL_HISTORY//{PATIENT,FAMILY}//{category}//{CURRENT,PAST,UNK}` | concept in `text_value`; `family_relation` column                                    | `past_medical_history`                              |
| `NOTE//MEDICATION//{ON_ADMISSION,AT_DISCHARGE}`                                | drug name in `text_value`; `dose_value`, `dose_unit`, `route`, `prn`, `hold` columns | `medications_on_admission`, `discharge_medications` |
| `NOTE//ALLERGY//{standardized allergen}`, `//UNK`, `//NONE_KNOWN`              | allergy as written in `text_value`                                                   | `allergies`                                         |
| `NOTE//PHYSICAL_EXAM//{timepoint}//{vital}`                                    | `numeric_value` (°C, bpm, mmHg, /min, %, kg); LOINC parents                          | `physical_exam_vitals`                              |
| `NOTE//PHYSICAL_EXAM//{timepoint}//O2_SUPPORT//{method}`                       | flow in L/min, if stated                                                             | `physical_exam_vitals`                              |
| `NOTE//PHYSICAL_EXAM//{timepoint}//{system}//{NORMAL,ABNORMAL}`                | abnormal findings in `text_value` (a Postgres array literal)                         | `physical_exam_systems`                             |
| `NOTE//DISCHARGE_CONDITION//{aspect}//{value}`                                 | —                                                                                    | `discharge_condition`                               |
| `NOTE//DISCHARGE_DIAGNOSIS//{PRIMARY,SECONDARY}`                               | diagnosis in `text_value`                                                            | `discharge_diagnosis`                               |
| `NOTE//MAJOR_PROCEDURE//{THIS_ADMISSION,PREVIOUS}`                             | procedure in `text_value`                                                            | `major_procedures`                                  |
| `NOTE//CHIEF_COMPLAINT//{FINDING,CONTEXT}`                                     | complaint in `text_value`                                                            | `chief_complaint`                                   |

The decisions behind the less obvious rows:

- **`seq_num` rides on every multi-row table**, as on the `omr` blood-pressure blocks. 66k history
    rows (1.8%) and 17k home-medication rows (0.7%) repeat within one admission except for
    `seq_num`, and the merge stage's `unique_by: "*"` would otherwise collapse them.
- **"No known allergies" gets its own code.** MNE stores it as an allergy row — 126k of them, 20.8%
    of the table, one per admission — always with a null `title_std`. Read naively it is an
    allergen. Only 2,012 real allergens lack a standardized label; they go to `NOTE//ALLERGY//UNK`.
- **NOT_ASSESSED exam systems are not emitted** (36% of rows). They record that the summary said
    nothing about the system, and MNE's authors warn that a missing entry is not evidence of a
    normal finding.
- **Temperature is taken in °C only.** `temperature_f` is present exactly when `temperature_c` is,
    and always converts to it. Weight arrives in kg or lbs and is normalized to kg; a weight with no
    unit is dropped.
- **Medication doses are extension columns, not `numeric_value`.** One code covers every drug, so
    a `numeric_value` would mix milligrams of one drug with puffs of another under the same code.
    Mapping the names to RxNorm would let the drug enter the code; MNE does not ship that mapping.
- **Not extracted:** `gender`, `services` and `discharge_disposition` duplicate MIMIC's own,
    timestamped columns (MNE's gender disagrees with `hosp/patients` for 0.04% of admissions; its
    `EXPIRED` agrees with `hospital_expire_flag` for ~97–98%). `patient_died` and `patient_left_ama`
    are covered by `MEDS_DEATH` and `HOSPITAL_DISCHARGE//AGAINST ADVICE`. `pertinent_results`
    repeats structured labs and microbiology for a quarter of its rows and needs array parsing for
    the imaging, pathology and electrophysiology rest. Medication `strength` and `frequency` are
    JSON in at least five shapes. `discharge_sections` is verbatim note text (845 MB of the
    ~1.2 GB release). The download's `include` list skips all of these.

**Known limits of the source.** 57 MNE admissions no longer exist in MIMIC-IV 3.1; their rows
(3,522) have no discharge time and are dropped. MNE filtered 9.4% of the source summaries (331,793
to 300,720) by criteria the paper does not spell out. Its `SHA256SUMS.txt` separates hash and path
by one space, which `shasum -c` rejects but MEDS-Extract's downloader accepts.

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

## Multimodal build: MIMIC-IV plus any combination of ED, notes, ECG, echo, chest X-rays and waveforms

`mimic-iv-meds-multimodal` builds one MEDS dataset from local copies of MIMIC-IV and any subset of
its linked PhysioNet modules. Unlike the `spec=` runs above it downloads nothing: point it at a
folder holding the releases side by side, in PhysioNet's own layout (`<slug>/<version>/` as
`wget -r` leaves it, or `<slug>-<version>/` as the ZIP download does).

```bash
# --modules: any subset of ed,note,ecg,echo,cxr,waveform, or `all`; empty for MIMIC-IV alone.
# Optional: --link-mode copy|hardlink|symlink (how objects get into the MEDS root; default copy),
# --path cxr=/elsewhere/mimic-cxr-jpg/2.1.0 (a release not under --root), --no-enrich (offline).
mimic-iv-meds-multimodal --root /data/MIMIC-IV_Multimodal_Demo --output-dir $MEDS_OUTPUT_DIR --modules all
```

| Module     | Release (demo / full)                | Events                                                                                                                        | Time                                           |
| ---------- | ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| core       | `mimic-iv-demo` / `mimiciv`          | the `MIMIC-IV` spec, unchanged                                                                                                | as in that spec                                |
| `ed`       | `mimic-iv-ed-demo` / `mimic-iv-ed`   | the `MIMIC-IV-ED` tables (`ED//…`)                                                                                            | as in that spec                                |
| `note`     | `mimic-iv-note`                      | `NOTE//DISCHARGE_SUMMARY`, `NOTE//RADIOLOGY_REPORT[//ADDENDUM]`, text in `text_value`                                         | `max(charttime, storetime)`                    |
| `ecg`      | `mimic-iv-ecg-demo` / `mimic-iv-ecg` | `ECG//WAVEFORM`, one per WFDB record, `ecg_link`                                                                              | `ecg_time` (acquisition)                       |
| `echo`     | `mimic-iv-echo`                      | `ECHO//DICOM`, one per DICOM file, `echo_link`; `ECHO//MEASUREMENT//<measurement>` from `structured-measurement` (if present) | `acquisition_datetime`; `measurement_datetime` |
| `cxr`      | `mimic-cxr-jpg`                      | `CXR//IMAGE//<ViewPosition>`, one per JPG, `cxr_link`                                                                         | `StudyDate` + `StudyTime`                      |
| `waveform` | `mimic4wdb`                          | `WAVEFORM//SEGMENT` per continuous segment and `WAVEFORM//RECORD` per multi-segment record, `waveform_link`                   | the **end** of the linked data                 |

Objects (waveforms, DICOMs, JPGs) are stored inside the MEDS root under their SHA-256, in their
original formats, and each object event names its file in a link column relative to that root:

```text
$MEDS_OUTPUT_DIR/
  data/…, metadata/dataset.json, codes.parquet, subject_splits.parquet
  metadata/objects.parquet   # link -> modality, format, sha256, size, source release + path, source IDs
  metadata/sources.json      # each release used (slug, version, demo/full, license) and what it contributed
  ecg/<sha[:2]>/<sha>/<record>.hea, .dat   # wfdb.rdrecord(root / ecg_link)
  echo/<sha[:2]>/<sha>.dcm
  cxr/<sha[:2]>/<sha>.jpg
  waveform/<sha[:2]>/<sha>/<record>.hea, <record>_NNNN.hea/.dat, <record>n.csv.gz   # a whole record
```

`metadata/dataset.json` says which combination this is: `dataset_name` is e.g.
`MIMIC-IV+ED+Note+ECG+ECHO+CXR`, `dataset_version` lists every release as `<slug>/<version>`,
`license` is the PhysioNet Credentialed Health Data License as soon as one credentialed release is
in, and the link columns are declared as MEDS `additional_value_modality_columns`. MEDS-Extract
writes all of that from the composed spec's `etl:` block. `objects.parquet` and `sources.json`
have no MEDS or MEDS-Extract counterpart yet; they are written by
`src/MIMIC_IV_MEDS/multimodal/finalize.py`, kept self-contained so they can move upstream as a
post-stage.

Every module is restricted to the subjects of the core's `hosp/patients` (for the demo: its 100
patients), whether its files hold only those subjects or the whole release, so a module can never
add subjects or reshuffle the core's splits. Tables too big to load whole, like MIMIC-IV-ECHO's
`structured-measurement.csv.gz` (28 M rows for all patients), stay compressed and are streamed,
keeping only the cohort's rows. An object listed in a record list but not (fully) on
disk gets no event: no link ever dangles, `sources.json` counts what was skipped, and re-running
after a download completes picks it up. On the MIMIC-IV demo subjects (100) the build adds 2,042
notes, 659 ECGs, 411 echo DICOMs, 6,820 echo measurements (58 subjects), 566 chest X-rays and
5 bedside-monitor waveform records (4 subjects, 60 segments, 190 h).

**Loader rule: a link's data becomes available at its event's `time`, and a loader reads only the
record the link names.** Every object event is timed when its data is complete, so a model that,
at prediction time *t*, loads only the links of events with `time <= t` never sees later data. This
matters most for waveforms: a record spans an ICU stay (days), and its folder also holds its later
segments and its numerics (`<record>n.csv.gz`, ~1 Hz HR, SpO2, NBP, …). `WAVEFORM//SEGMENT` is
timed at the end of its segment and its link is a WFDB record of that segment alone;
`WAVEFORM//RECORD`, the only event linking to the whole record (and so the only way to its
numerics), is timed at the end of the record. `waveform_duration_s` gives each window,
`[time - duration, time]`. Do not list or glob a link's folder. Every link, segments included, is
a row of `metadata/objects.parquet`. The price of end-timing: a segment is usable only once it
ends, and on the demo segments last up to ~60 h (records: median ~30 h, up to ~76 h). An encoder
that wants earlier access must cut the signal at the prediction time itself; the ETL could also
cut segments into fixed windows, which it does not do yet.

Caveats: the ECG time is the ECG machine's clock, which the release says was mostly not
synchronized with the hospital's. The CheXpert labels are not extracted: they are mined from the
report, which MIMIC-CXR-JPG does not time. Echo measurements with an empty `result` (two thirds of
the rows: blank fields of the report form) get no event; most measurements belong to echos whose
DICOMs are not released, so only some carry an `echo_study_id`. Notes are timed at
when they were saved, not when the care happened: the median gap is about 20 h for discharge
summaries and 2.4 h for radiology reports on the demo subjects.

## Code metadata

`metadata/codes.parquet` gives every code a `description` and, where a standard vocabulary
applies, `parent_codes` in OMOP form (`LOINC/2160-0`, `ICD10CM/I10`, `NDC/…`), so tooling
that maps codes across datasets or resolves them through OHDSI Athena has something to work
with. All of it is declared in `event_configs.yaml`, through three routes:

| Codes                                                                                                                                                                                | Route                                                                     | Source                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DIAGNOSIS`, `PROCEDURE//ICD`, `HCPCS`, ICU itemid codes (`LAB//<itemid>`, `PROCEDURE//START`/`END`, `INFUSION_*`, `SUBJECT_FLUID_OUTPUT`), `LAB//RESULT`, `LAB//SPECIMEN_COLLECTED` | `_metadata` joins against dictionary tables at run time                   | MIMIC-IV's own `d_icd_diagnoses`, `d_icd_procedures`, `d_hcpcs` and `d_items`; the [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (MIT, tag v2.4.0, checksum-pinned under `sources:`) for lab descriptions and the LOINC, SNOMED and RxNorm parents |
| every other component-coded event (admissions, demographics, transfers, ICU stays, DRGs, medications, OMR)                                                                           | `_self` templates over the code's own components                          | the [MIMIC-IV documentation](https://mimic.mit.edu/docs/iv/) of the column each component comes from (MIT-LCP/mimic-website, MIT), paraphrased, with the column named in parentheses                                                                                  |
| `ED_REGISTRATION`, `ED_OUT`, `MEDS_BIRTH`, `MEDS_DEATH`, `SUBJECT_WEIGHT_AT_INFUSION//KG`                                                                                            | `_self` templates, the code rendered from a constant `_table.cols` column | as above; `MEDS_BIRTH` and `MEDS_DEATH` are the MEDS standard codes                                                                                                                                                                                                   |

Literal codes take the detour through a constant column because MEDS-Extract attaches
metadata only through a code's components and rejects a `_metadata` block on a code with
none. The pre-existing `codes.parquet` it suggests for that case is not read by the
standard pipeline (`extract_code_metadata` reads it from the output of
`split_and_shard_subjects`, which writes none). The constant changes nothing in the data;
only the codes' `code_template` names the column.

Every parent the ETL writes is read from MIMIC-IV's own tables or the mimic-code maps;
links that need an external ontology (the OMOP Gender tags, the ICD pack hierarchy) come from
suMEDS instead (next section).

On the demo (2.2), 10,250 of 10,254 codes are described and 7,899 have a parent code. What
is left uncovered, and why:

- **Labs missing from mimic-code's lab map.** Lab descriptions come from
    `d_labitems_to_loinc.csv`, which lacks 28 of the demo's 1,622 `d_labitems` (all
    `53xxx`; one occurs in the demo's lab events, giving the four undescribed codes). A second
    description block over `hosp/d_labitems` would fill them, but MEDS-Extract has no
    precedence between blocks: it joins both strings, with a newline, into the description
    of every lab code.
- **No vocabulary parent:** medications whose order carries no NDC; ICU items absent from
    the mimic-code maps; admission, discharge, transfer, ICU-stay, demographic and OMR codes;
    CPT codes (`HCPCS//…` is keyed on MIMIC's short description, which for CPT is only the
    section name). The `DRG` parents use `MS-DRG/` and `APR-DRG/` (see [DRG](#drg)), which
    vocabulary tooling does not resolve; OMOP's own id for MS-DRGs is `DRG`.

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

For MIMIC-IV, the rules add:

- `GENDER//F` and `GENDER//M` get the OMOP Gender codes `Gender//F` and `Gender//M` (a tag:
    Gender has no public pack; with `--athena-csv` suMEDS validates them against Athena).
- `DIAGNOSIS//ICD//{9,10}//…` and `PROCEDURE//ICD//{9,10}//…` are matched to the dotted ICD-9-CM,
    ICD-10-CM, ICD-9 procedure or ICD-10-PCS code, which fills `vocabulary_id` and `concept_code`.
    The ETL's own reference (`ICD10CM/I50.23`) stays first in `parent_codes`, and every ancestor
    in the pack follows it (`ICD10CM//I50.2`, `ICD10CM//I50`).
- Same-dataset parents: an ICD code of the table becomes the parent of another when its
    vocabulary code is the nearest ancestor present (`DIAGNOSIS//ICD//10//I5023` gets
    `DIAGNOSIS//ICD//10//I502` when both occur).
- `parent_names` maps each vocabulary code in `parent_codes` that a pack knows to its official
    name, including the ETL's own references. Lab and fluid-output codes whose parent is a
    LOINC code (`LOINC/2160-0`) also get `vocabulary_id` `LOINC` and the `concept_code`.

On the demo (2.2), the published packs add parents to 1,779 codes (1,777 of its 1,826 ICD codes, 4
of them with a same-dataset parent, and both Gender codes) and names to 3,222 codes. The ETL alone
does not emit any of this: a plain `meds-extract-run` build has no Gender parents, no ICD ancestors,
no same-dataset parents and no names. suMEDS enrichment never removes or rewrites existing parents
or descriptions, so the ETL's own parents (`ICD10CM/…`, `LOINC/…`, `NDC/…`, RxNorm, SNOMED, DRG) and
its descriptions from MIMIC-IV's dictionary tables stay as they are.

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
[this github issue](https://github.com/Medical-Event-Data-Standard/MEDS_transforms/issues/235) for tracking on ensuring these
directories are automatically cleaned up in the future.

## Summary statistics

[`tests/regression/demo_summary_stats.json`](tests/regression/demo_summary_stats.json) is a
[meds_summary_stats](https://github.com/Medical-Event-Data-Standard/meds_summary_stats) fingerprint of
the MEDS dataset this ETL builds from the open MIMIC-IV demo: subject, event and measurement counts,
digests of the code vocabulary, per-subject, value and time distributions, code-metadata coverage, the
splits and the schema. It names no code and reports no cell of fewer than 20 subjects and no minimum or
maximum, which is what makes it safe to publish. The Tests workflow rebuilds the demo on every pull
request and fails when the result no longer matches the baseline, as decided by
[`tests/regression/policy.yaml`](tests/regression/policy.yaml).

`scripts/summary_stats.sh` regenerates a baseline from a build of this checkout. It prints how the
statistics changed against the committed baseline before overwriting it, and `--compare` checks a build
without writing anything:

```bash
# The demo: downloaded and built with this checkout
scripts/summary_stats.sh demo
# The full release, built from a copy on disk, or summarized from an existing build
scripts/summary_stats.sh full --input-dir $RAW_INPUT_DIR
scripts/summary_stats.sh full --meds-dir $MEDS_OUTPUT_DIR
```

[`tests/regression/full_summary_stats.json`](tests/regression/full_summary_stats.json) is the same
fingerprint of the full MIMIC-IV 3.1 release. Because it is committed next to the ETL, its diff shows
what an ETL change did to the whole credentialed release, and two ETL versions can be compared without
access to the data. CI cannot rebuild the credentialed release, so regenerate it with
`scripts/summary_stats.sh full` in every pull request that changes the output.

Both baselines are built with `skip_stages=[sumeds_enrich]`: they fingerprint what the ETL writes, which
does not depend on network access or on the suMEDS pack release. Summarize a build made the same way.

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
