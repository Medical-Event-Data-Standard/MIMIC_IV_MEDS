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
    (`data/` and `metadata/`). Each stage writes its intermediate output to
    `$MEDS_OUTPUT_DIR/<stage>/`, and those directories are deleted once the whole run has succeeded
    (see below).
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

Intermediate stage outputs are deleted after a successful run, by default: the spec sets
MEDS-Extract's `etl.cleanup_intermediates: true`, so a finished `$MEDS_OUTPUT_DIR` holds only `data/`,
`metadata/`, `.logs/` and `.meds_extract_run/`. A failed run keeps them, so the same command resumes
where it stopped, and the same command on a finished output still does nothing. To keep them (to look
at a stage's output; `$MEDS_OUTPUT_DIR/.logs/pipeline.log` keeps the stage logs either way), add
`"overrides=['cleanup_intermediates=false']"`. Cleanup never deletes raw data: a download into the
default `$MEDS_OUTPUT_DIR/.meds_extract_run/raw_input` stays, and `download_dest_dir=` elsewhere lets
other output directories reuse it.

Budget for the download rather than the extraction: the raw release is 41 files and 9.9 GiB
from PhysioNet, which rate-limits each connection to roughly 50 KB/s, while the extraction
itself is ~32 minutes and ~20 GB of RAM. Raise `download_concurrency` and keep the raw data
with `download_dest_dir`; see [Expected runtime and compute needs](#expected-runtime-and-compute-needs).

## How this ETL is defined

The entire ETL is one file —
[`src/MIMIC_IV_MEDS/event_configs.yaml`](src/MIMIC_IV_MEDS/event_configs.yaml)
— written in MESSY (MEDS-Extract Specification Syntax YAML):

- a `sources:` block declaring the release versions and where to fetch the raw data
    (PhysioNet for MIMIC-IV itself, checksum-pinned GitHub URLs for the
    [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps);
- an `etl:` block with the run's few knobs, the roles of the extension columns for
    `metadata/dataset.json`, and the three suMEDS stages that run after the pipeline, in this
    order: `sumeds_link_unit_siblings`, `sumeds_normalize_units` and the suMEDS enrichment (the two
    suMEDS unit stages are described under [Units](#units));
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
classifications, so each code carries the bare identifier as a parent — `MS-DRG//003`,
`APR-DRG//047` — via a `_self` metadata block.

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

`ordercategorydescription` (5 values) and `statusdescription` (6 values) on `icu/inputevents`
stay extension columns: they are closed enums rather than text, and they cannot both occupy the
single `text_value` slot on `input_end` (100% of rows carry both). See
[#15](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/15). The infusion units
(`rateuom`, `amountuom`) are the codes' `unit` in `metadata/codes.parquet`, and always the code's
last component; the events have no `unit` column (see [Units](#units)).

### ICU chart events

`icu/chartevents` is coded `ICU_CHART//{itemid}//{valueuom}` (`UNK` when the unit is null), for
example `ICU_CHART//220045//bpm`. These are values charted at the ICU bedside: vital signs, scores,
assessments, device settings and point-of-care measurements, keyed on `icu/d_items`. Hospital
laboratory results come from `hosp/labevents` and keep their own codes, `LAB//RESULT//…` and
`LAB//SPECIMEN_COLLECTED//…`, so `^LAB//` selects hospital labs only and `^ICU_CHART//` selects
ICU charting only. The prefix sits beside the other ICU tables' own prefixes (`ICU_ADMISSION`,
`INFUSION//START`, `PROCEDURE//START`, `SUBJECT_FLUID_OUTPUT`).

**This changes the chartevents vocabulary.** Up to 0.3.0 the same events were coded
`LAB//{itemid}//{valueuom}`. Only the prefix changed (the itemid and unit segments are as before),
so an old code maps to the new one by replacing a leading `LAB//` followed by a digit with
`ICU_CHART//`. Cohorts, predicates and vocabularies built against the old codes need that rename
([#64](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/64)).

## Code metadata

`metadata/codes.parquet` gives every code a `description` and, where a standard vocabulary
applies, `parent_codes` in OMOP form (`LOINC//2160-0`, `ICD10CM//I10`, `NDC//…`), so tooling
that maps codes across datasets or resolves them through OHDSI Athena has something to work
with. All of it is declared in `event_configs.yaml`, through three routes:

| Codes                                                                                                                                                                                      | Route                                                   | Source                                                                                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DIAGNOSIS`, `PROCEDURE//ICD`, `HCPCS`, ICU itemid codes (`ICU_CHART//<itemid>`, `PROCEDURE//START`/`END`, `INFUSION_*`, `SUBJECT_FLUID_OUTPUT`), `LAB//RESULT`, `LAB//SPECIMEN_COLLECTED` | `_metadata` joins against dictionary tables at run time | MIMIC-IV's own `d_icd_diagnoses`, `d_icd_procedures`, `d_hcpcs` and `d_items`; the [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (MIT, tag v2.4.0, checksum-pinned under `sources:`) for lab descriptions and the LOINC, SNOMED and RxNorm parents |
| every other component-coded event (admissions, demographics, transfers, ICU stays, DRGs, medications, OMR)                                                                                 | `_self` templates over the code's own components        | the [MIMIC-IV documentation](https://mimic.mit.edu/docs/iv/) of the column each component comes from (MIT-LCP/mimic-website, MIT), paraphrased, with the column named in parentheses                                                                                  |
| `ED_REGISTRATION`, `ED_OUT`, `MEDS_BIRTH`, `MEDS_DEATH`, `SUBJECT_WEIGHT_AT_INFUSION`                                                                                                      | `_self` text on the literal code                        | as above; `MEDS_BIRTH` and `MEDS_DEATH` are the MEDS standard codes                                                                                                                                                                                                   |

Literal codes are written as plain literals (`code: MEDS_BIRTH`) with a `_self` block:
MEDS-Extract 0.8.1 and later attach `_self` metadata to literal codes
([MEDS_extract#166](https://github.com/mmcdermott/MEDS_extract/issues/166)), and their
`code_template` is the literal itself.

Every parent the ETL writes is read from MIMIC-IV's own tables or the mimic-code maps;
links that need an external ontology (the OMOP Gender tags, the ICD pack hierarchy) come from
suMEDS instead (next section).

On the demo (2.2), 10,361 of 10,366 codes are described and 8,028 have a parent code. What
is left uncovered, and why:

- **Labs missing from mimic-code's lab map.** Lab descriptions come from
    `d_labitems_to_loinc.csv`, which lacks 28 of the demo's 1,622 `d_labitems` (all
    `53xxx`; one occurs in the demo's lab events, giving four undescribed codes, plus the stem
    `LAB//RESULT//53153` that two of them are linked to). A second
    description block over `hosp/d_labitems` would fill them, but MEDS-Extract has no
    precedence between blocks: it joins both strings, with a newline, into the description
    of every lab code.
- **No vocabulary parent:** medications whose order carries no NDC; ICU items absent from
    the mimic-code maps; admission, discharge, transfer, ICU-stay, demographic and OMR codes;
    CPT codes (`HCPCS//…` is keyed on MIMIC's short description, which for CPT is only the
    section name). The `DRG` parents use `MS-DRG//` and `APR-DRG//` (see [DRG](#drg)), which
    vocabulary tooling does not resolve; OMOP's own id for MS-DRGs is `DRG`.

### Units

**A code's unit is in `metadata/codes.parquet`, and only there: the events have no `unit` column.**
Every code has exactly one unit (or none), so an event's unit is its code's. To get it, join the
events to `codes.parquet` on `code`:

```python
import polars as pl

codes = pl.scan_parquet(f"{MEDS_DIR}/metadata/codes.parquet").select(
    "code", "unit", "unit_source_value"
)
events = pl.scan_parquet(f"{MEDS_DIR}/data/**/*.parquet").join(
    codes, on="code", how="left"
)
```

`codes.parquet` has two unit columns, both Strings, null for a code without a unit:

| Column              | What it holds                                                                                                                                                                                                                                                                      |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `unit`              | the unit as a [UCUM](https://ucum.org/) code (`mg/dL`, `ug`, `mL/h`, `{beats}/min`, `[lb_av]`), written by suMEDS's `sumeds_normalize_units` post-stage from the unit as recorded; a spelling suMEDS does not know is kept as it is (on the demo, 63 of the 949 codes with a unit) |
| `unit_source_value` | the unit as recorded in MIMIC-IV (`mcg`, `mL/hour`, `bpm`, `lbs.`), stripped of surrounding whitespace                                                                                                                                                                             |

Codes keep the unit as recorded: a code that ends in its unit ends in that spelling, so the code
alone tells an item's units apart (`INFUSION//END//225150//mcg`, whose `unit` is `ug` and
`unit_source_value` `mcg`; see below for which codes end in their unit).

**Breaking change** ([#102](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/pull/102)): before, the infusion events carried their unit in an event
`unit` column (`rateuom` on `INFUSION_START`, `amountuom` on `INFUSION_END`), listed in
`metadata/dataset.json` under `other_extension_columns`; no other event had one. That column is gone.
It held nothing the code did not: MEDS-Extract accepts a code's unit only if all its events agree, so
on the demo every one of the 31,442 events with a unit had its code's `unit_source_value`. Readers of
the events' `unit` read the code's instead, through the join above. A code's unit is now in one place,
as in the sibling MEDS ETLs (eICU, HiRID, AUMCdb, SICdb and others), none of which writes an event
`unit`.

MEDS does not standardise these columns. MEDS-Extract (0.9 and later) writes `unit` from the
config's `_metadata` `unit:` fields: the code's one unit, stripped of surrounding whitespace (a blank
unit gives none), and it fails the run, naming the codes, if a code has more than one. The spec's
first post-stage, suMEDS's
[`sumeds_link_unit_siblings`](https://github.com/rvandewater/suMEDS/blob/main/docs/unit_siblings.md),
then fills units from item labels and links unit siblings (both below), still in the spelling as
recorded. Next,
suMEDS's `sumeds_normalize_units` writes `unit` as UCUM and keeps the recorded spelling in
`unit_source_value`. Both run before the suMEDS enrichment, so a retried enrichment reads the same
file.

There are no code modifier columns (`code_modifier_columns` in `metadata/dataset.json`): every code
has one unit, so the code metadata is keyed on the code alone. Declaring `unit` a modifier made tools
that join code metadata on the code and its modifiers, such as suMEDS's summary, miss the metadata of
every code whose events have no `unit` (on the demo, 241 of the 1,129 codes the summary released lost
their description; none does without the modifier).

`sumeds_link_unit_siblings` also links codes that differ only in their unit component: where two or
more codes are the same code followed by `//<unit>` (an item recorded in several units, such as
`INFUSION//END//225150//mcg` and `INFUSION//END//225150//mg` on the demo), each gets that code without
the unit (`INFUSION//END//225150`) appended to its `parent_codes`. A code whose unit was not recorded
(`…//UNK`) joins its item's siblings if one of them has a unit (`ICU_CHART//220545//%` and
`ICU_CHART//220545//UNK`), but two `…//UNK`-style codes alone are never a group. The parent gets a row of its
own in `codes.parquet`, which has no events: the siblings' description if they all have the same one
(or the same one but for the unit each states, which is left out), the parent codes they all share,
and no unit. On the demo this gives 116 groups, 232 codes a parent and 116 new rows (96 of the
groups pair a unit with `…//UNK`). This shared stage replaced the package's own `mimic_iv_unit_metadata`
(suMEDS 0.6.6); the spec gives it the options below, and the suMEDS defaults otherwise
(`link_unknown_units: true`, `stem_description: without_unit`):

| Option                     | Value                                                                                                    |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| `description_unit_pattern` | `\s[(\[]([^()\[\]]+)[)\]]\s*$`, a bracketed unit at the end of a label                                   |
| `description_units`        | the bracketed units `icu/d_items` labels use (listed in `event_configs.yaml`)                            |
| `fill_units`               | `unknown_component`: only `…//UNK` codes get a unit from their label                                     |
| `link_single_units`        | `true`: a code ending in its unit gets its stem also when its item has one unit                          |
| `unknown_unit_prefixes`    | the infusion codes: an `INFUSION//START//<itemid>//UNK` start without a rate (a bolus) gets its stem too |

With the last two, every infusion start pairs with its end by stem, swapping `START` for `END`
(`INFUSION//START//225158//mL/hour` and `INFUSION//END//225158//mL` both get `…//225158`;
alignEHR#53), and every other code that ends in its unit (`ICU_CHART//<itemid>//<unit>`,
`LAB//RESULT//…//<unit>`, `SUBJECT_FLUID_OUTPUT//…//<unit>`) gets its unit-free stem as well; `…//UNK` outside the infusion codes
gets none, as there it is no unit. On the demo the two options add 890 codes of a single unit to the
232 above, so 1,122 codes get a stem parent and 1,006 stem rows are added (159 `INFUSION//START`, 159
`INFUSION//END`, 298 `ICU_CHART`, 351 `LAB//RESULT` and 39 `SUBJECT_FLUID_OUTPUT`).

| Codes                                                                          | unit as recorded from                                                                                                                  |
| ------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------- |
| `LAB//RESULT`, `ICU_CHART//<itemid>`, `SUBJECT_FLUID_OUTPUT`                   | `valueuom`, the code's last component (stripped; missing or blank is `UNK`), so one unit per code (none for `…//UNK`)                  |
| OMR `BMI (kg/m2)`, `Weight (Lbs)`, `Height (Inches)`                           | the unit in parentheses in `result_name`                                                                                               |
| `INFUSION//START//<itemid>//<rateuom>`, `INFUSION//END//<itemid>//<amountuom>` | `rateuom` and `amountuom`, the code's last component, so one unit per code (none for `…//UNK`)                                         |
| `SUBJECT_WEIGHT_AT_INFUSION`                                                   | `kg`, from the [documentation of `patientweight`](https://mimic.mit.edu/docs/iv/modules/icu/inputevents/) (MIT-LCP/mimic-website, MIT) |

The unit in a lab, chart or output code is `valueuom` stripped of surrounding whitespace; missing or
blank is `UNK`. MEDS-Extract compares units the same way when it writes `unit` (stripped, blanks
dropped), so ` mg` and `mg` give one code, with one unit. The config computes it once per table, as
the `_table.cols` helper `_value_unit`. On the demo, lab item 51498 records its unit as a single
space on all 339 rows, which gives `LAB//RESULT//51498//UNK`.

Where a charted item has no unit in the data (`ICU_CHART//<itemid>//UNK`), the unit its MIMIC-IV label
states is used instead: a label ending in a bracketed unit, such as `Admission Weight (lbs.)`. The
same post-stage does this, after the data and the code, which always win, and only for the
bracketed units that `icu/d_items` labels use (`description_units` in `event_configs.yaml`, read off the public
demo); other bracketed endings such as `(serum)` or a bag size `(500ml)` are not units, and no
`hosp/d_labitems` label states one. The code is not changed, since the label gives the item one
unit. Codes without a recorded unit (scores such as the GCS, the OMR blood pressures, `eGFR`) get
none. On the full 3.1 release, 77% of numeric events have a code with a unit.

**Infusion codes always end in their unit.** An infusion start is
`INFUSION//START//<itemid>//<rateuom>`, valued by its rate, and an end is
`INFUSION//END//<itemid>//<amountuom>`, valued by its amount, written like the `LAB` and
`SUBJECT_FLUID_OUTPUT` codes. A code then has one unit by construction, whatever units an itemid is
recorded in, and the config needs no stage of its own to find the itemids that mix units. The unit
in the code is stripped of surrounding whitespace, and a missing or blank one is `UNK`, which matches
how MEDS-Extract compares units when it writes `unit` (stripped, blanks dropped), so ` mg` and `mg`
give one code. A start without a unit is `INFUSION//START//<itemid>//UNK`: `rateuom` is null exactly
when `rate` is, so these are the starts without a rate, and they have no unit. `amountuom` is never
null. On the demo no `rateuom` or `amountuom` is blank or padded with whitespace.

Every infusion code, `…//UNK` included, gets the unit-free code `INFUSION//START//<itemid>`
(`INFUSION//END//<itemid>`) as its last parent from `sumeds_link_unit_siblings` (`link_single_units`
and `unknown_unit_prefixes`, see above), with a row in `codes.parquet` but no events; a code list
naming it then finds no events rather than silently only some of the itemid's units. To select every
infusion of an itemid, match the code without its last component, or the parent.

On the demo there are 350 infusion codes (177 starts, 101 of them `…//UNK`, and 173 ends) and 318
unit-free parents (159 starts, 159 ends), against 332 codes and 14 parents when codes ended in their
unit only where an itemid mixed units (see [Breaking changes](#breaking-changes)). Events, values and
times are unchanged, and each event's unit is its code's (see above).

**Pairing a start with its end.** Every interval is two codes, `<FAMILY>//START//…` and
`<FAMILY>//END//…` (pharmacy orders `MEDICATION//START|END//<drug>//<ndc>`, procedures
`PROCEDURE//START|END//<itemid>`, infusions `INFUSION//START|END//<itemid>//<unit>`), the spelling the
MEDS ETLs move to ([alignEHR#53](https://github.com/rvandewater/alignEHR/issues/53)). Both codes carry
what says which interval it is; only the unit, that of the value recorded at that event, may differ. To
pair them, take the code without its unit (the parent `sumeds_link_unit_siblings` gives every
infusion code) and swap `START` for `END`: `INFUSION//START//225150//mcg/kg/hour`
pairs with `INFUSION//END//225150//mcg` and `INFUSION//END//225150//mg` on the demo, whose parent is
`INFUSION//END//225150`.

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
- `DIAGNOSIS//ICD//{9CM,10CM}//…` and `PROCEDURE//ICD//{9CM,10PCS}//…` are matched to the dotted ICD-9-CM,
    ICD-10-CM, ICD-9 procedure or ICD-10-PCS code, which fills `vocabulary_id` and `concept_code`.
    The ETL's own reference (`ICD10CM//I50.23`) stays first in `parent_codes`, and every ancestor
    in the pack follows it (`ICD10CM//I50.2`, `ICD10CM//I50`).
- Same-dataset parents: an ICD code of the table becomes the parent of another when its
    vocabulary code is the nearest ancestor present (`DIAGNOSIS//ICD//10CM//I5023` gets
    `DIAGNOSIS//ICD//10CM//I502` when both occur).
- `parent_names` maps each vocabulary code in `parent_codes` that a pack knows to its official
    name, including the ETL's own references. Lab and fluid-output codes whose parent is a
    LOINC code (`LOINC//2160-0`) also get `vocabulary_id` `LOINC` and the `concept_code`.

On the demo (2.2), the published packs add parents to 1,779 codes (1,777 of its 1,826 ICD codes, 4
of them with a same-dataset parent, and both Gender codes) and names to 3,222 codes. The ETL alone
does not emit any of this: a plain `meds-extract-run` build has no Gender parents, no ICD ancestors,
no same-dataset parents and no names. suMEDS enrichment never removes or rewrites existing parents
or descriptions, so the ETL's own parents (`ICD10CM//…`, `LOINC//…`, `NDC//…`, RxNorm, SNOMED, DRG) and
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

The written config carries the spec's `cleanup_intermediates: true`, so this run deletes the stage
directories at the end too; add `--overrides cleanup_intermediates=false` to keep them. Only
`meds-extract-run` puts back the stage done markers that the cleanup deletes, so a later
`meds-extract-run` on an output that `MEDS_transform-pipeline` cleaned up runs every stage again.

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

|                   |                                      |
| ----------------- | ------------------------------------ |
| wall time         | **32 min**                           |
| peak RSS          | **20.3 GB**                          |
| MEDS data output  | 4.5 GB (366 shards)                  |
| metadata output   | 8.6 MB                               |
| whole output tree | 15 GB (with per-stage intermediates) |

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

The 15 GB tree was measured with the intermediate stage outputs kept, against 4.5 GB of actual MEDS
data. A successful run now deletes them (see [Usage](#usage)), but they exist until the last stage has
finished, so budget the full 15 GB of disk for the run.

## Summary statistics

[`tests/regression/demo_summary_stats.json`](tests/regression/demo_summary_stats.json) is a
[meds_summary_stats](https://github.com/Medical-Event-Data-Standard/meds_summary_stats) fingerprint of
the MEDS dataset this ETL builds from the open MIMIC-IV demo: subject, event and measurement counts,
digests of the code vocabulary, per-subject, value and time distributions, code-metadata coverage, the
splits and the schema. It names no code and reports no cell of fewer than 20 subjects and no minimum or
maximum, which is what makes it safe to publish. The Tests workflow rebuilds the demo on every pull
request and fails when the result no longer matches the baseline, as decided by meds_summary_stats's
built-in [`etl-regression` policy](https://github.com/Medical-Event-Data-Standard/meds_summary_stats/blob/main/docs/etl-baselines.md).

`baseline_MEDS_summary_stats` (from meds_summary_stats) regenerates a baseline from a build of this
checkout, as `[tool.meds_summary_stats.baselines]` in [`pyproject.toml`](pyproject.toml) describes it. It
prints how the statistics changed against the committed baseline before overwriting it, and `--compare`
checks a build without writing anything. Pin the version the Tests workflow uses:

```bash
# The demo: downloaded and built with this checkout
uvx --from meds_summary_stats==0.0.5 baseline_MEDS_summary_stats demo
# The full release, built from a copy on disk, or summarized from an existing build
uvx --from meds_summary_stats==0.0.5 baseline_MEDS_summary_stats full --input-dir $RAW_INPUT_DIR
uvx --from meds_summary_stats==0.0.5 baseline_MEDS_summary_stats full --meds-dir $MEDS_OUTPUT_DIR
```

[`tests/regression/full_summary_stats.json`](tests/regression/full_summary_stats.json) is the same
fingerprint of the full MIMIC-IV 3.1 release. Because it is committed next to the ETL, its diff shows
what an ETL change did to the whole credentialed release, and two ETL versions can be compared without
access to the data. CI cannot rebuild the credentialed release, so regenerate it with
`baseline_MEDS_summary_stats full` in every pull request that changes the output; the Summary stats
workflow reminds a pull request that changes `src/` without it.

Both baselines describe the dataset `meds-extract-run spec=MIMIC-IV` builds, suMEDS enrichment
included, so their code-metadata coverage counts the ontology parents the enrichment adds.
`baseline_MEDS_summary_stats` builds with `strict` enrichment
(`"overrides=['++stage_cfg.strict=true']"`), which fetches the suMEDS ontology packs over the network,
and refuses to summarize a build whose `metadata/dataset.json` does not record
`sumeds_enrichment.enriched: true` (`require_dataset_metadata` in `pyproject.toml`); enrich such a build
with `suMEDS-enrich $MEDS_OUTPUT_DIR` first. The demo cohort the Tests workflow builds is enriched the same way. A suMEDS upgrade can move the
enrichment statistics too, so check it with `--compare`.

## Breaking changes

Changes that alter codes, columns or metadata that cohorts, predicates, tokenizers or downstream
mappings may depend on, newest release first.

### Unreleased (next release, 0.4.0)

Counts are from the public MIMIC-IV demo; the full-release counts need a credentialed rebuild. Every
event keeps its subject, time, values and other columns: only codes, the code metadata and the run's
leftovers change.

**Codes**

- **ICU chart events are `ICU_CHART//…`** ([#64](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/64)).
    `icu/chartevents` was coded `LAB//{itemid}//{valueuom}` up to 0.3.0 and is now
    `ICU_CHART//{itemid}//{valueuom}`; `LAB//…` is hospital labs only. Migrate by replacing
    `^LAB//(?=\d)` with `ICU_CHART//` (see [ICU chart events](#icu-chart-events)).

- **ICD codes name their edition, `ICD//<edition>//<code>`.** `DIAGNOSIS//ICD//9//<code>` and
    `DIAGNOSIS//ICD//10//<code>` are now `DIAGNOSIS//ICD//9CM//<code>` and
    `DIAGNOSIS//ICD//10CM//<code>`; `PROCEDURE//ICD//9//<code>` and `PROCEDURE//ICD//10//<code>` are
    now `PROCEDURE//ICD//9CM//<code>` and `PROCEDURE//ICD//10PCS//<code>`. MIMIC-IV's `icd_version` 9
    and 10 are the US billing code sets: ICD-9-CM (diagnoses, and volume 3 for procedures), ICD-10-CM
    and ICD-10-PCS, the vocabularies the parents already name (`ICD9CM`, `ICD9Proc`, `ICD10CM`,
    `ICD10PCS`). Other MEDS ETLs use the same layout (`ICD//10GM`, `ICD//10CN`, ...). The code after
    the edition is unchanged (undotted, as MIMIC-IV records it). To migrate, rename with
    `^DIAGNOSIS//ICD//(9|10)//` → `DIAGNOSIS//ICD//\1CM//`, `^PROCEDURE//ICD//9//` →
    `PROCEDURE//ICD//9CM//` and `^PROCEDURE//ICD//10//` → `PROCEDURE//ICD//10PCS//`.

- **Interval ends are `<FAMILY>//START//…` and `<FAMILY>//END//…`**
    ([alignEHR#53](https://github.com/rvandewater/alignEHR/issues/53)): pharmacy order ends were
    `MEDICATION//STOP//…`, now `MEDICATION//END//…`, and infusions were `INFUSION_START//…` and
    `INFUSION_END//…`, now `INFUSION//START//…` and `INFUSION//END//…` (see
    [Pairing a start with its end](#units)). Migrate with `^MEDICATION//STOP//` → `MEDICATION//END//`;
    for infusions, see the next entry.

- **Every infusion code ends in its unit**: `INFUSION//START//<itemid>//<rateuom>` and
    `INFUSION//END//<itemid>//<amountuom>`, with the unit stripped of surrounding whitespace and
    `//UNK` where it is missing or blank (for starts, exactly those without a rate). Up to 0.3.0 the
    codes were `INFUSION_START//<itemid>` and `INFUSION_END//<itemid>`, with the unit in the events'
    `unit` column. Every code now has one unit by construction, as the `LAB` codes do, and the package's
    own `mimic_iv_unit_keys` pre-stage, which found the itemids that mix units, is gone. On the demo
    the 318 infusion codes of 0.3.0 (159 itemids, each with a start and an end code) become 350: 177
    starts, 101 of them `…//UNK` (18 itemids have starts with and without a rate, so both a
    `…//<rateuom>` and a `…//UNK` start), and 173 ends (14 itemids record their amounts in two units). Each code's `unit`, `unit_source_value`, description and other parents are
    unchanged. Migrate a 0.3.0 event with `^INFUSION_(START|END)//(\d+)$` → `INFUSION//\1//\2//<unit>`,
    where `<unit>` is the event's `unit` stripped, or `UNK` if it is null or blank. To compare with
    codes that name no unit, strip the last component (`^(INFUSION//(START|END)//\d+)//.*$` → `\1`),
    or follow `parent_codes` to the unit-free stem (see below).

- **The unit in a lab, chart or output code is stripped of surrounding whitespace, and a blank one is
    `UNK`.** This covers `LAB//RESULT//{itemid}//{valueuom}`, `ICU_CHART//{itemid}//{valueuom}` and
    `SUBJECT_FLUID_OUTPUT//{itemid}//{valueuom}`. Before, the unit was copied as recorded, so a padded
    unit gave a code of its own and a blank one a code ending in whitespace, with no `unit`. On the
    demo, lab item 51498 records its unit as a single space (339 events), so its code was
    `LAB//RESULT//51498//` followed by a space; it is now `LAB//RESULT//51498//UNK`. No other demo code
    changes. To migrate, strip the last part of an old code and write `UNK` for an empty one:
    `^(.*//)\s*(\S(?:.*\S)?)?\s*$` → `\1\2`, then `//$` → `//UNK`.

- **`SUBJECT_WEIGHT_AT_INFUSION//KG` is `SUBJECT_WEIGHT_AT_INFUSION`**, eICU's code for the same
    value; its unit, `kg`, is in `metadata/codes.parquet`. Migrate by replacing
    `SUBJECT_WEIGHT_AT_INFUSION//KG` with `SUBJECT_WEIGHT_AT_INFUSION`.

**Events and code metadata**

- **The events have no `unit` column.** Every code has one unit, which is in `metadata/codes.parquet`
    only; join the events to it on `code` (see [Units](#units)). `unit` is no longer a code modifier
    (`code_modifier_columns` is empty).

- **`codes.parquet` `unit` is a UCUM code** (`mcg` → `ug`, `mL/hour` → `mL/h`); the unit as recorded
    is in the new column `unit_source_value`. Code strings keep the recorded spelling.

- **Vocabulary parents are spelled `VOCABULARY//CODE`.** The `parent_codes` the ETL writes itself
    (`ICD9CM`, `ICD10CM`, `ICD9Proc`, `ICD10PCS`, `NDC`, `MS-DRG`, `APR-DRG`, and the LOINC, SNOMED
    and RxNorm parents from the mimic-code maps) used one slash, `ICD10CM/I50.23`; they now use two,
    `ICD10CM//I50.23`, the spelling suMEDS already writes for the ancestors it adds. To migrate, respell
    old parents with the regex `^([^/]+)/(?!/)` → `\1//` (only the first slash changes; codes after it
    are kept as they are). suMEDS and alignEHR already read both spellings as the same parent.

- **`codes.parquet` has unit-free stem rows, without events**
    ([#114](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/pull/114)). suMEDS's
    `sumeds_link_unit_siblings` gives every code that ends in its unit the code without the unit as its
    last `parent_codes` entry, and that stem a row of its own (no unit, no events): where an item is
    recorded in several units, with `link_single_units` also where it has one, and with
    `unknown_unit_prefixes` also for an infusion code without a unit (see
    [Code metadata](#code-metadata)). So every infusion start pairs with its end by stem. On the demo,
    1,122 codes get a stem parent and 1,006 stem rows are added (159 `INFUSION//START`, 159
    `INFUSION//END`, 298 `ICU_CHART`, 351 `LAB//RESULT`, 39 `SUBJECT_FLUID_OUTPUT`). Nothing needs
    migrating unless a tool expects every `codes.parquet` row to have events or a unit: drop the stem
    rows (`code_template` and `unit` null), or ignore a parent that is the code without its last `//`
    component.

**Running the ETL**

- **Intermediate stage outputs are no longer kept.** Up to 0.3.0, a run left each stage's output in
    `<output_dir>/<stage>/` (`convert_to_parquet/`, `convert_to_MEDS_events/`, …, about 10 GB of the
    full release's 15 GB tree). The spec now sets MEDS-Extract's `etl.cleanup_intermediates: true`
    (MEDS-Extract 0.9.2 or later), so a successful run deletes those directories and keeps only
    `data/`, `metadata/`, `.logs/` and `.meds_extract_run/`. The MEDS data and metadata are
    unchanged. To migrate, add `"overrides=['cleanup_intermediates=false']"` to `meds-extract-run`
    wherever you read a stage directory after the run.

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
