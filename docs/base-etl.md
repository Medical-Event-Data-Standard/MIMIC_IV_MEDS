# The base MIMIC-IV ETL

This page describes `spec=MIMIC-IV`: which MIMIC-IV tables it reads, how codes and times are built,
and the reasoning behind the less obvious decisions. Every extension spec contains this ETL
unchanged, so all of it applies to the extensions too.

Figures on this page are from the full MIMIC-IV 3.1 release unless marked as demo.

## How the ETL is defined

There is no Python in this package. The whole ETL is one file,
[`src/MIMIC_IV_MEDS/event_configs.yaml`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/src/MIMIC_IV_MEDS/event_configs.yaml),
written in MESSY (MEDS-Extract Specification Syntax YAML). Expressions in it use the
[dftly](https://github.com/mmcdermott/dftly) language. It has three kinds of block:

- **`sources:`** says where the raw data comes from: the release versions (`3.1` for `dataset`,
    `2.2` for `demo`), PhysioNet for MIMIC-IV, and checksum-pinned GitHub URLs for ten
    [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (tag `v2.4.0`). It is consumed
    by the downloader and stripped before extraction, so credentials never reach the logs or the
    config copy in the output.
- **`etl:`** holds the run's few settings: the dataset name, `n_subjects_per_shard: 1000`, the roles
    of the extension columns for `metadata/dataset.json` (see
    [Extension columns in dataset.json](#extension-columns-in-datasetjson)), and the suMEDS
    enrichment that runs after the pipeline (`post_stages`; see
    [Enrichment with suMEDS](#enrichment-with-sumeds)).
- **One block per raw table.** Each lists the events extracted from that table, with any joins,
    derived columns and the metadata programs that attach descriptions and parent codes.

`pyproject.toml` registers this file with MEDS-Extract under the name `MIMIC-IV`. That is what makes
`spec=MIMIC-IV` work from any directory, and it supplies the version written to
`metadata/dataset.json` as `<MIMIC-IV release>:<package version>`. See
[MEDS-Extract's documentation](https://github.com/mmcdermott/MEDS_extract) for the syntax. The spec
needs MEDS-Extract 0.8.1 or later: it names its joins `_table.joins` (renamed from `_table.join` in
0.8), and it relies on 0.8.1 to run its `post_stages` and to write the `dataset.json` fields of its
`etl:` block.

## Tables and events

| Source table          | Code                                                                              | Time                                    | Value and extension columns                                                                         |
| --------------------- | --------------------------------------------------------------------------------- | --------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `hosp/patients`       | `GENDER//{gender}`                                                                | none (static)                           | —                                                                                                   |
| `hosp/patients`       | `MEDS_BIRTH`                                                                      | 1 January of `anchor_year − anchor_age` | —                                                                                                   |
| `hosp/patients`       | `MEDS_DEATH`                                                                      | `admissions.deathtime`, else `dod`      | —                                                                                                   |
| `hosp/admissions`     | `HOSPITAL_ADMISSION//{admission_type}//{admission_location}`                      | `admittime`                             | `hadm_id`                                                                                           |
| `hosp/admissions`     | `INSURANCE//…`, `LANGUAGE//…`, `MARITAL_STATUS//…`, `RACE//…`                     | `admittime`                             | `hadm_id`                                                                                           |
| `hosp/admissions`     | `ED_REGISTRATION`, `ED_OUT`                                                       | `edregtime`, `edouttime`                | —                                                                                                   |
| `hosp/admissions`     | `HOSPITAL_DISCHARGE//{discharge_location}`                                        | `dischtime`                             | `hadm_id`                                                                                           |
| `hosp/transfers`      | `TRANSFER_TO//{eventtype}//{careunit}`                                            | `intime`                                | `hadm_id`                                                                                           |
| `hosp/diagnoses_icd`  | `DIAGNOSIS//ICD//{icd_version}//{icd_code}`                                       | admission `dischtime`                   | `hadm_id`                                                                                           |
| `hosp/procedures_icd` | `PROCEDURE//ICD//{icd_version}//{icd_code}`                                       | admission `dischtime`                   | `hadm_id`                                                                                           |
| `hosp/drgcodes`       | `DRG//{drg_type}//{drg_code}`                                                     | admission `dischtime`                   | DRG title in `text_value`; `drg_severity`, `drg_mortality`                                          |
| `hosp/hcpcsevents`    | `HCPCS//{short_description}`                                                      | `chartdate` (day)                       | `hadm_id`                                                                                           |
| `hosp/labevents`      | `LAB//SPECIMEN_COLLECTED//{itemid}//{priority}`                                   | `charttime`                             | `hadm_id`                                                                                           |
| `hosp/labevents`      | `LAB//RESULT//{itemid}//{valueuom}`                                               | `storetime`                             | `valuenum` → `numeric_value`, `value` → `text_value`                                                |
| `hosp/omr`            | `{result_name}`, `{result_name}//SYSTOLIC`, `{result_name}//DIASTOLIC`            | `chartdate` (day)                       | number in `numeric_value`, other results in `text_value`; `seq_num` on blood pressure               |
| `hosp/emar`           | `MEDICATION//{medication}//{event_txt}//{ndc}`                                    | `charttime`                             | `hadm_id`, `emar_id`, `emar_seq`                                                                    |
| `hosp/pharmacy`       | `MEDICATION//START//{medication}//{ndc}`, `MEDICATION//STOP//{medication}//{ndc}` | `starttime`, `stoptime`                 | `route`, `frequency`, `doses_per_24_hrs` (start only), `poe_id`                                     |
| `icu/icustays`        | `ICU_ADMISSION//{first_careunit}`, `ICU_DISCHARGE//{last_careunit}`               | `intime`, `outtime`                     | `hadm_id`, `icustay_id`                                                                             |
| `icu/chartevents`     | `LAB//{itemid}//{valueuom}`                                                       | `charttime`                             | `valuenum` → `numeric_value`, `value` → `text_value`; `hadm_id`, `icustay_id`                       |
| `icu/procedureevents` | `PROCEDURE//START//{itemid}`, `PROCEDURE//END//{itemid}`                          | `starttime`, `endtime`                  | `hadm_id`, `icustay_id`                                                                             |
| `icu/inputevents`     | `INFUSION_START//{itemid}`                                                        | `starttime`                             | `rate` → `numeric_value`; `unit` (rateuom), `order_id`, `link_order_id`, `ordercategorydescription` |
| `icu/inputevents`     | `INFUSION_END//{itemid}`                                                          | `endtime`                               | `amount` → `numeric_value`; `unit` (amountuom), `ordercategorydescription`, `statusdescription`     |
| `icu/inputevents`     | `SUBJECT_WEIGHT_AT_INFUSION//KG`                                                  | `starttime`                             | `patientweight` → `numeric_value`                                                                   |
| `icu/outputevents`    | `SUBJECT_FLUID_OUTPUT//{itemid}//{valueuom}`                                      | `charttime`                             | `value` → `numeric_value`; `hadm_id`, `icustay_id`                                                  |

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
- **`ICU chartevents` are coded `LAB//…`.** This is historical, and it mixes ICU charting with
    hospital labs under one prefix. Renaming it is discussed in
    [#64](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/64).
- **A null component becomes `UNK`** in most composite codes (via dftly's `??` operator), so the
    row is kept: e.g. `HOSPITAL_DISCHARGE//UNK`, `LAB//RESULT//50912//UNK`. This keeps the pre-0.7
    behaviour; since MEDS-Extract 0.7 a bare null component drops the row.
- **Demographics are the deliberate exception.** A missing insurance, language, marital status or
    race produces *no* event rather than `INSURANCE//UNK` (see [Demographics](#demographics)).
- **A null code drops the row.** Several blocks use this on purpose to route rows between blocks
    (see [OMR](#omr)). MEDS-Extract logs these as `dropped N/M rows with null code`, which is expected
    and is not data loss.
- **Literal codes go through a constant column.** `ED_REGISTRATION`, `ED_OUT`, `MEDS_BIRTH`,
    `MEDS_DEATH` and `SUBJECT_WEIGHT_AT_INFUSION//KG` are rendered from a constant `_table.cols`
    column rather than written as literals. MEDS-Extract attaches metadata only through a code's
    components and rejects a `_metadata` block on a code that has none. The constant gives them a
    component to attach a description to. It changes nothing in the data: the code strings are
    identical, and rows without a time are still dropped.
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
    parent, `MS-DRG/003` or `APR-DRG/047`. The DRG number is zero-padded to three digits, since the
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
- **`icu/inputevents` is unchanged for related reasons.** `ordercategorydescription` (5 values)
    and `statusdescription` (6 values) are closed enums, not text. They cannot both go in the single
    `text_value` slot on `INFUSION_END` (100% of rows have both). `rateuom` is 44.7% null, so adding it
    to the code would put `UNK` into half of all `INFUSION_START` codes. See
    [#15](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/15).

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
- **The parent is the NDC itself** (`NDC/<ndc>`), with no parent for `UNK`. MIMIC ships no RxNorm
    identifiers, and mapping NDC to RxNorm needs an external vocabulary whose free subset covers only
    currently marketed products; many of MIMIC's 2008–2022 NDCs are retired. Users who want RxNorm can
    map the NDC themselves.

## Code metadata

`metadata/codes.parquet` gives every code a `description` and, where a standard vocabulary applies,
`parent_codes` in OMOP form (`LOINC/2160-0`, `ICD10CM/I10`, `NDC/…`), so tools that map codes across
datasets or look them up in OHDSI Athena have something to work with. It comes from three routes,
all declared in the spec:

| Codes                                                                                                           | Route                                                   | Source                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DIAGNOSIS`, `PROCEDURE//ICD`, `HCPCS`, ICU itemid codes, `LAB//RESULT`, `LAB//SPECIMEN_COLLECTED`              | `_metadata` joins against dictionary tables at run time | MIMIC-IV's `d_icd_diagnoses`, `d_icd_procedures`, `d_hcpcs` and `d_items`; the [mimic-code](https://github.com/MIT-LCP/mimic-code) concept maps (MIT licence, tag v2.4.0) for lab descriptions and LOINC, SNOMED and RxNorm parents |
| Every other code built from components (admissions, demographics, transfers, ICU stays, DRGs, medications, OMR) | `_self` templates over the code's own components        | The [MIMIC-IV documentation](https://mimic.mit.edu/docs/iv/) of the column each component comes from (MIT-LCP/mimic-website, MIT licence), paraphrased, with the column named in parentheses                                        |
| `ED_REGISTRATION`, `ED_OUT`, `MEDS_BIRTH`, `MEDS_DEATH`, `SUBJECT_WEIGHT_AT_INFUSION//KG`                       | `_self` templates, via the constant column              | As above; `MEDS_BIRTH` and `MEDS_DEATH` are MEDS standard codes                                                                                                                                                                     |

Details worth knowing:

- **ICD parents are dotted.** `ICD9CM/…`, `ICD10CM/…`, `ICD9Proc/…` and `ICD10PCS/…` parents insert
    the dot MIMIC strips (ICD-9 E-codes after position 4, other ICD-9 diagnoses after 3, ICD-9
    procedures after 2, ICD-10-CM after 3, ICD-10-PCS never).
- **Partial matches are deliberate.** Lab, chartevents and outputevents metadata are joined on the
    `itemid` alone, not on the unit, so every unit variant of an item gets the same description.
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
- **Why a constant column for literal codes.** MEDS-Extract suggests a pre-existing
    `codes.parquet` for codes with no components, but the standard pipeline never reads it
    (`extract_code_metadata` looks for it in the output of `split_and_shard_subjects`, which writes
    none).

**Coverage on the demo (2.2), before enrichment:** 10,250 of 10,254 codes have a description and
7,899 have a parent code (the `code_metadata` section of the committed
[demo baseline](contributing.md#summary-statistics-baselines)). What is not covered, and why:

- **Labs missing from mimic-code's lab map.** Lab descriptions come from `d_labitems_to_loinc.csv`,
    which lacks 28 of the demo's 1,622 `d_labitems` (all `53xxx`). One of them occurs in the demo's
    lab events, giving the four undescribed codes. A second description block over `hosp/d_labitems`
    would fill them, but MEDS-Extract has no precedence between blocks: it would join both strings,
    with a newline, into the description of *every* lab code.
- **No vocabulary parent:** medications whose order has no NDC; ICU items absent from the mimic-code
    maps; admission, discharge, transfer, ICU-stay, demographic and OMR codes; and CPT codes.
    `HCPCS//…` is keyed on MIMIC's short description, which for CPT codes is only the section name.
    MIMIC-IV distributes no CPT descriptions, for licensing reasons.
- **DRG parents use `MS-DRG/` and `APR-DRG/`**, which vocabulary tools do not resolve; OMOP's own
    vocabulary id for MS-DRGs is `DRG`.

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
- **ICD vocabulary codes and ancestors.** `DIAGNOSIS//ICD//{9,10}//…` and `PROCEDURE//ICD//{9,10}//…`
    are matched to the dotted ICD-9-CM, ICD-10-CM, ICD-9 procedure or ICD-10-PCS code (MIMIC-IV
    stores ICD codes without the dot), which fills `vocabulary_id` and `concept_code`. The ETL's own
    reference (`ICD10CM/I50.23`) stays first in `parent_codes`, and every ancestor in the pack follows
    it (`ICD10CM//I50.2`, `ICD10CM//I50`).
- **Same-dataset parents.** An ICD code of the dataset becomes the parent of another when its
    vocabulary code is the nearest ancestor present: `DIAGNOSIS//ICD//10//I5023` gets
    `DIAGNOSIS//ICD//10//I502` when both occur.
- **Names.** `parent_names` maps each vocabulary code in `parent_codes` that a pack knows to its
    official name, including the ETL's own references. Lab and fluid-output codes whose parent is a
    LOINC code (`LOINC/2160-0`) also get `vocabulary_id` `LOINC` and the `concept_code`.

Enrichment is **non-destructive**: it never removes or rewrites an existing parent or description, so
the ETL's own parents (`ICD10CM/…`, `LOINC/…`, `NDC/…`, RxNorm, SNOMED, DRG) and its descriptions from
MIMIC-IV's dictionary tables stay as they are, and the data shards are not touched.
`tests/run_with_enrichment_test.py` checks this against the demo with a small synthetic pack.

On the demo (2.2), the published packs add parents to 1,779 codes (1,777 of its 1,826 ICD codes, 4 of
them with a same-dataset parent, and both Gender codes) and names to 3,222 codes. A plain build has
none of this: no Gender parents, no ICD ancestors, no same-dataset parents and no names.

Note that the two parent styles differ: the ETL's own references use one slash (`ICD10CM/I50.23`),
the pack ancestors suMEDS appends use two (`ICD10CM//I50.2`).

## Extension columns in dataset.json

MEDS lets `metadata/dataset.json` say what each extension column is. The spec's `etl:` block assigns
two roles, which MEDS-Extract 0.8.1 writes into `dataset.json`:

| Role                    | Columns                                                                   | Why                                                                                                                                 |
| ----------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `raw_source_id_columns` | `hadm_id`, `icustay_id`, `emar_id`, `poe_id`, `order_id`, `link_order_id` | identifiers that point back into the raw MIMIC-IV tables                                                                            |
| `code_modifier_columns` | `unit`                                                                    | the `icu/inputevents` unit (`rateuom` on `INFUSION_START`, `amountuom` on `INFUSION_END`), which qualifies the code's numeric value |

Every other extension column (such as `seq_num`, `drg_severity`, `route` and MEDS-Extract's
`source_block`) is listed under `other_extension_columns`, which MEDS-Extract derives from the data.
The spec sets no `license` or `description_uri`, because the one spec builds both the credentialed
release and the open demo, whose licences and PhysioNet pages differ.

`codes.parquet` is still keyed by `code` alone, as MEDS-Extract writes it, even though `unit` is
declared a code modifier. suMEDS 0.6.3 summarizes code metadata that way, which is why it is the
floor: 0.6.2 fails with `unable to find column "unit"`.

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
- **Intermediate outputs** are kept in the output directory; see
    [Getting started](getting-started.md#output-layout).
