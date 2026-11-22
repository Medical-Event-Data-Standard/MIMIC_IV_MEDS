# The base MIMIC-IV ETL

This page describes `spec=MIMIC-IV`: which MIMIC-IV tables it reads, how codes and times are built,
and the reasoning behind the less obvious decisions. Every extension spec contains this ETL
unchanged, so all of it applies to the extensions too.

Figures on this page are from the full MIMIC-IV 3.1 release unless marked as demo.

## How the ETL is defined

The whole ETL is one file,
[`src/MIMIC_IV_MEDS/event_configs.yaml`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/src/MIMIC_IV_MEDS/event_configs.yaml),
written in MESSY (MEDS-Extract Specification Syntax YAML). Expressions in it use the
[dftly](https://github.com/mmcdermott/dftly) language. It has three kinds of block:

- **`sources:`** says where the raw data comes from: the release versions (`3.1` for `dataset`,
    `2.2` for `demo`), PhysioNet for MIMIC-IV, and checksum-pinned GitHub URLs for ten
    [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (tag `v2.4.0`). It is consumed
    by the downloader and stripped before extraction, so credentials never reach the logs or the
    config copy in the output.
- **`etl:`** holds the run's few settings: the dataset name, `n_subjects_per_shard: 1000`,
    `cleanup_intermediates: true` (the stage outputs are deleted after a successful run; see
    [Getting started](getting-started.md#output-layout)), the roles of the extension columns for
    `metadata/dataset.json` (see [Extension columns in dataset.json](#extension-columns-in-datasetjson)),
    and the three suMEDS stages that run after the pipeline (`post_stages`), in this order:
    `sumeds_link_unit_siblings` and `sumeds_normalize_units` (see [Units](#units)), then the
    enrichment (see [Enrichment with suMEDS](#enrichment-with-sumeds)).
- **One block per raw table.** Each lists the events extracted from that table, with any joins,
    derived columns and the metadata programs that attach descriptions and parent codes.

`pyproject.toml` registers this file with MEDS-Extract under the name `MIMIC-IV`. That is what makes
`spec=MIMIC-IV` work from any directory, and it supplies the version written to
`metadata/dataset.json` as `<MIMIC-IV release>:<package version>`. See
[MEDS-Extract's documentation](https://github.com/mmcdermott/MEDS_extract) for the syntax. The spec
needs MEDS-Extract 0.9.2 or later: 0.9 writes the `unit` code metadata as one String per code, 0.9.2
reads `etl.cleanup_intermediates`, and the spec also relies on 0.8.1, which names joins
`_table.joins` (renamed from `_table.join` in 0.8), runs its `post_stages` and writes the
`dataset.json` fields of its `etl:` block.

## Tables and events

| Source table          | Code                                                                             | Time                                    | Value and extension columns                                                           |
| --------------------- | -------------------------------------------------------------------------------- | --------------------------------------- | ------------------------------------------------------------------------------------- |
| `hosp/patients`       | `GENDER//{gender}`                                                               | none (static)                           | —                                                                                     |
| `hosp/patients`       | `MEDS_BIRTH`                                                                     | 1 January of `anchor_year − anchor_age` | —                                                                                     |
| `hosp/patients`       | `MEDS_DEATH`                                                                     | `admissions.deathtime`, else `dod`      | —                                                                                     |
| `hosp/admissions`     | `HOSPITAL_ADMISSION//{admission_type}//{admission_location}`                     | `admittime`                             | `hadm_id`                                                                             |
| `hosp/admissions`     | `INSURANCE//…`, `LANGUAGE//…`, `MARITAL_STATUS//…`, `RACE//…`                    | `admittime`                             | `hadm_id`                                                                             |
| `hosp/admissions`     | `ED_REGISTRATION`, `ED_OUT`                                                      | `edregtime`, `edouttime`                | —                                                                                     |
| `hosp/admissions`     | `HOSPITAL_DISCHARGE//{discharge_location}`                                       | `dischtime`                             | `hadm_id`                                                                             |
| `hosp/transfers`      | `TRANSFER_TO//{eventtype}//{careunit}`                                           | `intime`                                | `hadm_id`                                                                             |
| `hosp/diagnoses_icd`  | `DIAGNOSIS//ICD//{9CM,10CM}//{icd_code}`                                         | admission `dischtime`                   | `hadm_id`                                                                             |
| `hosp/procedures_icd` | `PROCEDURE//ICD//{9CM,10PCS}//{icd_code}`                                        | admission `dischtime`                   | `hadm_id`                                                                             |
| `hosp/drgcodes`       | `DRG//{drg_type}//{drg_code}`                                                    | admission `dischtime`                   | DRG title in `text_value`; `drg_severity`, `drg_mortality`                            |
| `hosp/hcpcsevents`    | `HCPCS//{short_description}`                                                     | `chartdate` (day)                       | `hadm_id`                                                                             |
| `hosp/labevents`      | `LAB//SPECIMEN_COLLECTED//{itemid}//{priority}`                                  | `charttime`                             | `hadm_id`                                                                             |
| `hosp/labevents`      | `LAB//RESULT//{itemid}//{valueuom}` (unit stripped; `UNK` if missing or blank)   | `storetime`                             | `valuenum` → `numeric_value`, `value` → `text_value`                                  |
| `hosp/omr`            | `{result_name}`, `{result_name}//SYSTOLIC`, `{result_name}//DIASTOLIC`           | `chartdate` (day)                       | number in `numeric_value`, other results in `text_value`; `seq_num` on blood pressure |
| `hosp/emar`           | `MEDICATION//{medication}//{event_txt}//{ndc}`                                   | `charttime`                             | `hadm_id`, `emar_id`, `emar_seq`                                                      |
| `hosp/pharmacy`       | `MEDICATION//START//{medication}//{ndc}`, `MEDICATION//END//{medication}//{ndc}` | `starttime`, `stoptime`                 | `route`, `frequency`, `doses_per_24_hrs` (start only), `poe_id`                       |
| `icu/icustays`        | `ICU_ADMISSION//{first_careunit}`, `ICU_DISCHARGE//{last_careunit}`              | `intime`, `outtime`                     | `hadm_id`, `icustay_id`                                                               |
| `icu/chartevents`     | `ICU_CHART//{itemid}//{valueuom}`                                                | `charttime`                             | `valuenum` → `numeric_value`, `value` → `text_value`; `hadm_id`, `icustay_id`         |
| `icu/procedureevents` | `PROCEDURE//START//{itemid}`, `PROCEDURE//END//{itemid}`                         | `starttime`, `endtime`                  | `hadm_id`, `icustay_id`                                                               |
| `icu/inputevents`     | `INFUSION//START//{itemid}//{rateuom}`                                           | `starttime`                             | `rate` → `numeric_value`; `order_id`, `link_order_id`, `ordercategorydescription`     |
| `icu/inputevents`     | `INFUSION//END//{itemid}//{amountuom}`                                           | `endtime`                               | `amount` → `numeric_value`; `ordercategorydescription`, `statusdescription`           |
| `icu/inputevents`     | `SUBJECT_WEIGHT_AT_INFUSION`                                                     | `starttime`                             | `patientweight` → `numeric_value`                                                     |
| `icu/outputevents`    | `SUBJECT_FLUID_OUTPUT//{itemid}//{valueuom}`                                     | `charttime`                             | `value` → `numeric_value`; `hadm_id`, `icustay_id`                                    |

The events have no `unit` column: every code has one unit (or none), in `metadata/codes.parquet`
(see [Units](#units)). The units in lab, chart, output and infusion codes are stripped of surrounding
whitespace, and `UNK` where missing or blank.

Tables read only to supply joins or metadata: `hosp/prescriptions` (the NDC), `hosp/d_icd_diagnoses`,
`hosp/d_icd_procedures`, `hosp/d_hcpcs`, `icu/d_items` and the mimic-code concept maps.

**Not read at all:** `hosp/microbiologyevents`, `hosp/emar_detail`, `hosp/poe`,
`hosp/poe_detail`, `hosp/services`, `hosp/provider`, `hosp/d_labitems`, `icu/datetimeevents`,
`icu/ingredientevents` and `icu/caregiver`. Of these, only `d_labitems` has a recorded reason (see
[Code metadata](#code-metadata)); the others are simply not extracted yet. `hosp/services` in
particular would be the timestamped source for the service a patient was under.

## Code conventions

- **Components are joined with `//`.** The first component names the kind of event
    (`LAB`, `DIAGNOSIS`, `MEDICATION`, …); the others come from the row. The exception is
    `hosp/omr`, whose codes are the bare `result_name` (e.g. `Weight (Lbs)`), with `//SYSTOLIC` or
    `//DIASTOLIC` added for blood pressures.
- **ICU chart events are coded `ICU_CHART//…`**, for example `ICU_CHART//220045//bpm`: values
    charted at the ICU bedside (vital signs, scores, assessments, device settings, point-of-care
    values), keyed on `icu/d_items`. Hospital lab results keep `LAB//RESULT//…` and
    `LAB//SPECIMEN_COLLECTED//…`, so `^LAB//` selects hospital labs only and `^ICU_CHART//` ICU
    charting only. Up to 0.3.0 chart events were `LAB//{itemid}//{valueuom}`
    ([#64](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/64)); only the prefix
    changed.
- **ICD codes name their edition**, `ICD//<edition>//<code>`: MIMIC-IV's `icd_version` 9 and 10 are
    the US billing code sets, so diagnoses are `DIAGNOSIS//ICD//9CM//…` and `DIAGNOSIS//ICD//10CM//…`
    and procedures `PROCEDURE//ICD//9CM//…` (ICD-9-CM volume 3) and `PROCEDURE//ICD//10PCS//…`. Any
    other version is written as recorded. The code after the edition stays undotted, as recorded.
- **An interval is two codes, `<FAMILY>//START//…` and `<FAMILY>//END//…`**: pharmacy orders
    `MEDICATION//START|END//<drug>//<ndc>`, procedures `PROCEDURE//START|END//<itemid>`, infusions
    `INFUSION//START|END//<itemid>//<unit>`
    ([alignEHR#53](https://github.com/rvandewater/alignEHR/issues/53)). Both carry what says which
    interval it is, so swapping `START` for `END` pairs them (for infusions, after dropping the unit;
    see [Units](#units)).
- **Lab, chart, output and infusion codes end in their unit**, as recorded, stripped of surrounding
    whitespace and `UNK` where missing or blank, so every code has one unit (see [Units](#units)).
- **A null component becomes `UNK`** in most composite codes (via dftly's `??` operator), so the
    row is kept: e.g. `HOSPITAL_DISCHARGE//UNK`, `LAB//RESULT//50912//UNK`. This keeps the pre-0.7
    behaviour; since MEDS-Extract 0.7 a bare null component drops the row.
- **Demographics are the deliberate exception.** A missing insurance, language, marital status or
    race produces *no* event rather than `INSURANCE//UNK` (see [Demographics](#demographics)).
- **A null code drops the row.** Several blocks use this on purpose to route rows between blocks
    (see [OMR](#omr)). MEDS-Extract logs these as `dropped N/M rows with null code`, which is expected
    and is not data loss.
- **Literal codes are plain literals.** `ED_REGISTRATION`, `ED_OUT`, `MEDS_BIRTH`, `MEDS_DEATH` and
    `SUBJECT_WEIGHT_AT_INFUSION` are written as literals (`code: MEDS_BIRTH`) with a `_self` metadata
    block: MEDS-Extract 0.8.1 and later attach `_self` metadata to literal codes
    ([MEDS_extract#166](https://github.com/mmcdermott/MEDS_extract/issues/166)). A row whose time is
    null still yields no event. `tests/code_metadata_test.py` pins the literal codes of every spec.
- `MEDS_BIRTH` and `MEDS_DEATH` are the MEDS standard codes.

## Timing rules

The general rule is to time an event when the information **became available**, not when the
underlying fact happened. Placing anything earlier leaks it into prediction windows that could not
have seen it.

- **Admission-level billing codes are timed at discharge.** `DIAGNOSIS`, `PROCEDURE//ICD` and `DRG`
    are assigned at or after discharge, so they are placed at `hosp/admissions.dischtime` (joined
    on `hadm_id`), not at the start of the stay (see
    [#25](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/25)).
- **Lab results are timed when they were available.** `LAB//RESULT` uses `storetime`, when the
    result was released to the lab system. `charttime` (usually when the specimen was taken, often
    much earlier) is used for the separate `LAB//SPECIMEN_COLLECTED` event (see
    [#30](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/30)).
- **Demographics are co-timed with the admission** (`admittime`). MIMIC records no separate time
    for them.
- **Death** uses the precise in-hospital `deathtime` where there is one (the earliest across the
    subject's admissions, via an aggregated join) and otherwise the date-only `dod`, which includes
    deaths up to one year after discharge from state records. Living subjects get no death event.
- **Birth** is 1 January of the year of birth, `anchor_year − anchor_age`. MIMIC gives no more than
    the year, in its shifted calendar, and records ages over 89 as 91.
- **Gender is static**: its time is null.
- **Day-resolution tables.** `hosp/omr` and `hosp/hcpcsevents` have only a date, so their events sit
    at midnight. For HCPCS this places an admission's billing codes inside the stay, which is
    discussed as possible leakage in
    [#65](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/65) (open).
- **Mixed formats.** `hosp/pharmacy` start and stop times mix datetimes and bare dates; both are
    parsed, taking whichever matches.

## Modelling decisions

MIMIC has many columns that are neither the measurement nor metadata about it. Where each one lands
(in the code, in `numeric_value`/`text_value`, as an extension column, or nowhere) is a modelling
decision, and these are the ones made so far.

### Demographics

`insurance`, `language`, `marital_status` and `race` describe the subject, not the admission. They
are emitted as their own events (`INSURANCE//…`, `LANGUAGE//…`, `MARITAL_STATUS//…`, `RACE//…`),
not carried as extension columns.

Their nulls are **not** turned into `UNK`. Since MEDS-Extract 0.7 a null code component drops the row, so a
missing demographic produces no event. That is the honest encoding, and it avoids a `RACE//UNK` code
that would read as an observed category. Null rates: insurance 1.71%, language 0.14%,
marital_status 2.49%, race 0%.

### DRG

- **`drg_type: HCFA` means MS-DRG**, not the older CMS-DRG the name suggests. 302 distinct HCFA codes
    are above 579, a range used only by MS-DRG (which replaced CMS-DRG in FY2008; MIMIC-IV spans
    2008–2022). `APR` is Solventum's proprietary APR-DRG. Each code gets its bare identifier as a
    parent, `MS-DRG//003` or `APR-DRG//047`. The DRG number is zero-padded to three digits, since the
    raw column is an integer.
- **The DRG title is not part of the code.** It is not a function of the code: 87.8% of HCFA
    `(drg_type, drg_code)` pairs carry more than one title, up to five, differing only in spelling
    (`W MCC` vs `WITH MCC`, `&` vs `AND`, truncation near 72 characters). Putting it in the code split
    single DRGs across several codes and made the HCFA vocabulary **1.98×** larger (1,557 codes for 787
    DRGs). The title goes in `text_value` instead.
- **`drg_severity` and `drg_mortality` stay extension columns.** They are the APR-DRG
    severity-of-illness and risk-of-mortality subclasses, computed by the grouper from the coded
    diagnoses. They are model output, not observations of the patient, so they are not MEDS
    measurements, but they remain available for cohort selection. They are not redundant: 278 of 300
    APR codes span all four severity levels, and knowing the severity still leaves 66% of mortality's
    entropy. Both are null on HCFA rows. The DRG itself is also assigned by a grouper, but it is an
    administrative fact with consequences (it is what was billed), whereas the subclasses are
    gradations of that assignment.

### OMR

`hosp/omr.result_value` is a string holding two kinds of result: a single number (weight, height,
BMI, …) and a blood pressure written `systolic/diastolic`. The raw 3.1 table has 7.75 M rows: 4.91 M
plain numbers, 2.84 M blood pressures and 1,133 other strings (such as a censored eGFR `>60`).

- **Numbers go in `numeric_value`.** A plain decimal is stored as a number under the bare code, with
    `text_value` left null. Earlier versions kept the string in `text_value`, which hid these values
    from everything downstream that reads numbers.
- **A blood pressure becomes two events**, `<result_name>//SYSTOLIC` and `<result_name>//DIASTOLIC`,
    each with its number, the same way `icu/chartevents` keeps systolic and diastolic as two items.
- **Anything else stays in `text_value`** under the bare code, so a code such as `eGFR` can have
    both numeric and text rows.
- **Three blocks split the rows** by giving each row a null code in the blocks it does not belong
    to. The row classes are computed once, in `_table.cols`, so the three blocks cannot disagree.
    Expect three `dropped N/M rows with null code` warnings per shard for this table.
- **`seq_num` is kept on the two blood-pressure blocks.** OMR is dated to the day, so several
    readings on one day share a timestamp. `seq_num` is unique per subject, day and result name. It
    keeps each systolic/diastolic pair together and stops the merge stage's `unique_by: "*"`
    de-duplication from collapsing same-day readings that happen to have the same value. 4.6% of
    blood-pressure subject-days have more than one reading (123,740 of 2,710,497), and 12,648 readings
    would otherwise collapse. The bare `omr` block does not carry it, so same-day repeat weights are
    still de-duplicated. After merging, the table yields 7.63 M distinct events.

!!! warning "Vocabulary change"

    This changed the OMR vocabulary: `Blood Pressure` became the `//SYSTOLIC` and `//DIASTOLIC`
    codes, and weight, BMI and height moved from `text_value` to `numeric_value`. Cohorts built
    before and after the change are not comparable on these codes.

### Order modifiers

- **`priority` (`ROUTINE`/`STAT`) is in `LAB//SPECIMEN_COLLECTED` only**, not in `LAB//RESULT`.
    Priority describes how the specimen was collected; the result is the same measurement however
    urgently it was drawn. Nothing is lost: `charttime` is never null across all 158 M rows, so the
    collection event is always emitted. Priority is 4.8% null and becomes `UNK`.
- **`route` and `frequency` on `hosp/pharmacy` stay extension columns.** Adding them to the code
    takes `MEDICATION//START` from 22,539 to 125,381 codes (**5.56×**, with 56,248 codes seen only
    once); `frequency` alone accounts for 4.53× across 177 values. That fragments the medication
    vocabulary far more than it adds detail.
- **`ordercategorydescription` and `statusdescription` on `icu/inputevents` stay extension
    columns.** They are closed enums (5 and 6 values), not text, and they cannot both go in the
    single `text_value` slot on `INFUSION//END` (100% of rows have both). See
    [#15](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/15). The infusion units
    (`rateuom`, `amountuom`) are the codes' last component and their `unit` in
    `metadata/codes.parquet`; the events have no `unit` column (see [Units](#units)).

### Medications and the NDC

`hosp/emar` and `hosp/pharmacy` events carry the NDC of the dispensed drug, taken from
`hosp/prescriptions`.

- **The join is restricted to the `MAIN` drug line.** `prescriptions` stores one order as several
    rows: `drug_type` MAIN (the drug), BASE (the diluent bag) and ADDITIVE. Joining on `pharmacy_id`
    alone fans out, since 18.7% of pharmacy ids have more than one row. That would inflate eMAR events
    by about 15%, i.e. invent administrations. Joined on `(pharmacy_id, 'MAIN')`, the join is exactly
    one to one.
- **The NDC is part of the code** (`MEDICATION//…//{ndc}`), because parent codes can only be
    attached through a code's own components. The drug name stays in the code, so it remains readable.
    `'0'` (prescriptions' "no NDC" value) and an unmatched join both become `UNK`.
- **The parent is the NDC itself** (`NDC//<ndc>`), with no parent for `UNK`. MIMIC ships no RxNorm
    identifiers, and mapping NDC to RxNorm needs an external vocabulary whose free subset covers only
    currently marketed products; many of MIMIC's 2008–2022 NDCs are retired. Users who want RxNorm can
    map the NDC themselves.

## Code metadata

`metadata/codes.parquet` gives every code a `description` and, where a standard vocabulary applies,
`parent_codes` in OMOP form (`LOINC//2160-0`, `ICD10CM//I10`, `NDC//…`), so tools that map codes across
datasets or look them up in OHDSI Athena have something to work with. It comes from three routes,
all declared in the spec:

| Codes                                                                                                           | Route                                                   | Source                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DIAGNOSIS`, `PROCEDURE//ICD`, `HCPCS`, ICU itemid codes, `LAB//RESULT`, `LAB//SPECIMEN_COLLECTED`              | `_metadata` joins against dictionary tables at run time | MIMIC-IV's `d_icd_diagnoses`, `d_icd_procedures`, `d_hcpcs` and `d_items`; the [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (MIT licence, tag v2.4.0) for lab descriptions and LOINC, SNOMED and RxNorm parents |
| Every other code built from components (admissions, demographics, transfers, ICU stays, DRGs, medications, OMR) | `_self` templates over the code's own components        | The [MIMIC-IV documentation](https://mimic.mit.edu/docs/iv/) of the column each component comes from (MIT-LCP/mimic-website, MIT licence), paraphrased, with the column named in parentheses                                        |
| `ED_REGISTRATION`, `ED_OUT`, `MEDS_BIRTH`, `MEDS_DEATH`, `SUBJECT_WEIGHT_AT_INFUSION`                           | `_self` text on the literal code                        | As above; `MEDS_BIRTH` and `MEDS_DEATH` are MEDS standard codes                                                                                                                                                                     |

Details worth knowing:

- **ICD parents are dotted.** `ICD9CM//…`, `ICD10CM//…`, `ICD9Proc//…` and `ICD10PCS//…` parents insert
    the dot MIMIC strips (ICD-9 E-codes after position 4, other ICD-9 diagnoses after 3, ICD-9
    procedures after 2, ICD-10-CM after 3, ICD-10-PCS never).
- **Partial matches are deliberate.** Lab, chartevents and outputevents descriptions are joined on
    the `itemid` alone, not on the unit, so every unit variant of an item gets the same description.
    Keying on the unit as well matched only 84–90% of `(itemid, unit)` pairs, because the
    dictionaries' units disagree with the charted ones.
- **One description source per code.** When two `_metadata` blocks both emit a description,
    MEDS-Extract joins them with a newline. So `icu/d_items` alone supplies descriptions for ICU
    items, and the OMOP concept maps supply only parents. The demo e2e test fails on any multi-line
    description.
- **No inferred parents.** Every parent the ETL writes is read from MIMIC-IV's own tables or the
    mimic-code maps. Links that need an external ontology, such as the OMOP Gender tags of
    `GENDER//F` and `GENDER//M` or the ICD hierarchy, come from suMEDS instead (see
    [Enrichment with suMEDS](#enrichment-with-sumeds)). A plain ETL build has no parent on the
    Gender codes.
- **Vocabulary parents are spelled `VOCABULARY//CODE`** (`ICD10CM//I50.23`), the spelling suMEDS
    writes for the ancestors it adds. Up to 0.3.0 the ETL's own parents used one slash
    (`ICD10CM/I50.23`).

**Coverage on the demo (2.2):** 10,361 of 10,366 codes have a description and 8,028 have a parent
code. What is not covered, and why:

- **Labs missing from mimic-code's lab map.** Lab descriptions come from `d_labitems_to_loinc.csv`,
    which lacks 28 of the demo's 1,622 `d_labitems` (all `53xxx`). One of them occurs in the demo's
    lab events, giving four undescribed codes, plus the stem `LAB//RESULT//53153` that two of them
    are linked to (see [Units](#units)). A second description block over `hosp/d_labitems`
    would fill them, but MEDS-Extract has no precedence between blocks: it would join both strings,
    with a newline, into the description of *every* lab code.
- **No vocabulary parent:** medications whose order has no NDC; ICU items absent from the mimic-code
    maps; admission, discharge, transfer, ICU-stay, demographic and OMR codes; and CPT codes.
    `HCPCS//…` is keyed on MIMIC's short description, which for CPT codes is only the section name.
    MIMIC-IV distributes no CPT descriptions, for licensing reasons.
- **DRG parents use `MS-DRG//` and `APR-DRG//`**, which vocabulary tools do not resolve; OMOP's own
    vocabulary id for MS-DRGs is `DRG`.

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
unit only where an itemid mixed units (see [Breaking changes](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS#breaking-changes)). Events, values and
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

## Enrichment with suMEDS

The ETL's code metadata stops at what MIMIC-IV and the mimic-code maps say. External-ontology links
are added after the pipeline by [suMEDS](https://github.com/rvandewater/SumMEDS), whose
`sumeds_enrich` stage the spec declares as a post-stage (`etl.post_stages`). It reads the rules this
package ships in
[`src/MIMIC_IV_MEDS/sumeds_rules.yaml`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/src/MIMIC_IV_MEDS/sumeds_rules.yaml),
registered in suMEDS's `sumeds.rules` entry-point group under the spec's name, `MIMIC-IV`, and looks
codes up in suMEDS's versioned ontology packs (ICD-10-CM, ICD-10-PCS, ICD-9-CM, ICD-9 procedures,
LOINC). [Getting started](getting-started.md#code-metadata-enrichment) covers running it, skipping it
and finishing a failed enrichment later.

For MIMIC-IV, the rules add:

- **Gender tags.** `GENDER//F` and `GENDER//M` get the OMOP Gender codes `Gender//F` and `Gender//M`.
    MIMIC-IV records `hosp/patients.gender` as `F`/`M`, which are the OMOP Gender vocabulary's own
    concept codes for FEMALE (8532) and MALE (8507). Gender has no public pack, so the code is a tag;
    with an Athena download configured, suMEDS validates it.
- **ICD vocabulary codes and ancestors.** `DIAGNOSIS//ICD//{9CM,10CM}//…` and `PROCEDURE//ICD//{9CM,10PCS}//…`
    are matched to the dotted ICD-9-CM, ICD-10-CM, ICD-9 procedure or ICD-10-PCS code (MIMIC-IV
    stores ICD codes without the dot), which fills `vocabulary_id` and `concept_code`. The ETL's own
    reference (`ICD10CM//I50.23`) stays first in `parent_codes`, and every ancestor in the pack follows
    it (`ICD10CM//I50.2`, `ICD10CM//I50`).
- **Same-dataset parents.** An ICD code of the dataset becomes the parent of another when its
    vocabulary code is the nearest ancestor present: `DIAGNOSIS//ICD//10CM//I5023` gets
    `DIAGNOSIS//ICD//10CM//I502` when both occur.
- **Names.** `parent_names` maps each vocabulary code in `parent_codes` that a pack knows to its
    official name, including the ETL's own references. Lab and fluid-output codes whose parent is a
    LOINC code (`LOINC//2160-0`) also get `vocabulary_id` `LOINC` and the `concept_code`.

Enrichment is **non-destructive**: it never removes or rewrites an existing parent or description, so
the ETL's own parents (`ICD10CM//…`, `LOINC//…`, `NDC//…`, RxNorm, SNOMED, DRG) and its descriptions from
MIMIC-IV's dictionary tables stay as they are, and the data shards are not touched.
`tests/run_with_enrichment_test.py` checks this against the demo with a small synthetic pack.

On the demo (2.2), the published packs add parents to 1,779 codes (1,777 of its 1,826 ICD codes, 4 of
them with a same-dataset parent, and both Gender codes) and names to 3,222 codes. A plain build has
none of this: no Gender parents, no ICD ancestors, no same-dataset parents and no names.

The ETL's own references and the pack ancestors suMEDS appends are spelled alike, with two slashes
(`ICD10CM//I50.23`, `ICD10CM//I50.2`).

## Extension columns in dataset.json

MEDS lets `metadata/dataset.json` say what each extension column is. The spec's `etl:` block assigns
one role, which MEDS-Extract writes into `dataset.json`:

| Role                    | Columns                                                                   | Why                                                      |
| ----------------------- | ------------------------------------------------------------------------- | -------------------------------------------------------- |
| `raw_source_id_columns` | `hadm_id`, `icustay_id`, `emar_id`, `poe_id`, `order_id`, `link_order_id` | identifiers that point back into the raw MIMIC-IV tables |

Every other extension column (such as `seq_num`, `drg_severity`, `route` and MEDS-Extract's
`source_block`) is listed under `other_extension_columns`, which MEDS-Extract derives from the data.
The spec sets no `license` or `description_uri`, because the one spec builds both the credentialed
release and the open demo, whose licences and PhysioNet pages differ.

There are no code modifier columns (`code_modifier_columns` is empty): every code has one unit, which
is in `codes.parquet`, so the code metadata is keyed on the code alone (see [Units](#units)). Up to
0.3.0 `unit` was a code modifier, carried by the infusion events.

## Caveats

- **ED-only patients have nearly empty timelines.** The base spec sees the ED only through
    `ED_REGISTRATION`/`ED_OUT` (admitted patients only) and the `TRANSFER_TO//ED//…` rows. According
    to the MIMIC-IV 3.x release notes, 141,175 of 364,627 subjects were seen only in the ED. The
    [MIMIC-IV-ED extension](extensions/index.md) (in progress) addresses this.
- **Free-text notes are not included.** MIMIC-IV-Note is a separate project. The
    [MIMIC-IV-MNE extension](extensions/mne.md) adds structured content extracted from its discharge
    summaries.
- **Death outside the hospital** comes from state records via `dod`. It has only a date and covers
    deaths only up to one year after hospital discharge.
- **Shifted calendar.** All MIMIC-IV dates are shifted per patient. Only `anchor_year_group` ties a
    patient to a real three-year window, so calendar-time comparisons across patients are
    approximate.
- **Intermediate outputs** are deleted after a successful run (`etl.cleanup_intermediates`), but
    exist until the last stage has finished; see [Getting started](getting-started.md#output-layout).
