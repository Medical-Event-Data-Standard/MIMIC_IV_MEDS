# MIMIC-IV-ED

## Summary

The spec `MIMIC-IV-ED` is the full `MIMIC-IV` ETL plus the six tables of
[MIMIC-IV-ED v2.2](https://physionet.org/content/mimic-iv-ed/2.2/): about 425k emergency department
(ED) stays at Beth Israel Deaconess Medical Center from 2011 to 2019. It adds, for each ED stay:

- arrival transport and ED disposition;
- triage: vital signs, pain, the Emergency Severity Index (ESI) acuity and the chief complaint;
- vital signs charted during the stay, with pain and heart rhythm;
- the ED's ICD-9/10 billing diagnoses;
- the medication reconciliation (what the patient was already taking);
- medications dispensed from the ED's automated dispensing cabinets (Pyxis).

Without it, the base ETL marks an ED visit only by its `TRANSFER_TO//ED//…` event (and
`ED_REGISTRATION`/`ED_OUT` for visits that led to an admission). That matters most for the 141,175
MIMIC-IV 3.1 subjects who never had a hospital admission: 79,370 of them gain ED events, a median
of 30 each.

`spec=MIMIC-IV` and its output are unchanged. ED subjects are a subset of MIMIC-IV's, so the
extension adds events, not subjects.

## Source

|            |                                                                                                                                                                                                                                                                |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dataset    | MIMIC-IV-ED v2.2 (released 2023-01-05), and the MIMIC-IV-ED Demo v2.2                                                                                                                                                                                          |
| URL        | <https://physionet.org/content/mimic-iv-ed/2.2/>, demo <https://physionet.org/content/mimic-iv-ed-demo/2.2/>                                                                                                                                                   |
| Access     | Full: credentialed PhysioNet account, CITI "Data or Specimens Only Research" training, and a signature on **this project's own** DUA (PhysioNet Credentialed Health Data License 1.5.0). Signing the MIMIC-IV DUA is not enough. Demo: open access, ODbL v1.0. |
| Downloaded | The six `ed/*.csv.gz` tables only (~120 MB full, 0.1 MB demo), via an `include` list                                                                                                                                                                           |

Please cite the data you use:

- Johnson, A., Bulgarelli, L., Pollard, T., Celi, L. A., Mark, R., & Horng, S. (2023).
    MIMIC-IV-ED (version 2.2). PhysioNet. <https://doi.org/10.13026/5ntk-km72>
- Johnson, A., Bulgarelli, L., Pollard, T., Celi, L. A., Horng, S., & Mark, R. (2023).
    MIMIC-IV-ED Demo (version 2.2). PhysioNet. <https://doi.org/10.13026/jzz5-vs76>
- The PhysioNet platform citation that both pages request: Pollard, T., Moody, B. E., Lehman, L.,
    Gow, B., Fernandes, C., Xie, C., Johnson, A., Mark, R. G., & Heldt, T. (2026). PhysioNet as a
    global platform for biomedical research. *Nature Health*.
    <https://doi.org/10.1038/s44360-026-00096-z>

ED-level prediction tasks on this data (hospitalisation, critical outcome, 72-hour revisit) are
benchmarked in Xie, F. et al. (2022), *Benchmarking emergency department prediction models with
machine learning and public electronic health records*, Scientific Data 9, 658,
<https://doi.org/10.1038/s41597-022-01782-9>. It predicts from triage-time information and treats what
happens at or after departure as outcomes, which is how the timing rules below place those events.

## How to run

```bash
# Needs a PhysioNet account approved for BOTH MIMIC-IV and MIMIC-IV-ED.
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
meds-extract-run spec=MIMIC-IV-ED output_dir=$MEDS_OUTPUT_DIR

# The public demos (MIMIC-IV demo 2.2 + MIMIC-IV-ED demo 2.2); no credentials needed.
meds-extract-run spec=MIMIC-IV-ED output_dir=$MEDS_OUTPUT_DIR dataset_key=demo
```

With `do_download=false`, put the ED release's `ed/` directory in `input_dir` next to `hosp/` and
`icu/`; that is where the download puts it. The output's `metadata/dataset.json` names the dataset
`MIMIC-IV-ED`, which tells an ED-enriched cohort apart from a plain `MIMIC-IV` one. Its
`dataset_version` is still the MIMIC-IV release (`3.1:<package version>`, or `2.2:…` for the demo),
because the MIMIC-IV download URL is built from that value.

The spec is generated: edit `src/MIMIC_IV_MEDS/extensions/ed.yaml` or
the base, then run `scripts/build_extension_specs.py`. CI runs the demo end to end
(`tests/e2e_ed_demo_test.py`).

## Tables → events

Every event carries `ed_stay_id` (the ED `stay_id`) and, when the stay led to an admission,
`hadm_id` (from `edstays`). `metadata/dataset.json` lists `ed_stay_id` under
`other_extension_columns`: a fragment can set only `etl.dataset_name`, so it cannot extend the
base's `raw_source_id_columns`.

The spec inherits the base's `sumeds_enrich` post-stage. Its rules match only the base's own code
patterns, but suMEDS also resolves the parents the ETL already sets, so ED diagnoses gain
`vocabulary_id`, `concept_code` and their ICD ancestors, and the vitals' LOINC parents gain names.
suMEDS has no NDC data, so medication codes are left as the ETL wrote them.

| Source table | Code                                                                                             | Time              | Value fields                                                                       |
| ------------ | ------------------------------------------------------------------------------------------------ | ----------------- | ---------------------------------------------------------------------------------- |
| `edstays`    | `ED//ARRIVAL//{arrival_transport}`                                                               | `intime`          | —                                                                                  |
| `edstays`    | `ED//DISPOSITION//{disposition}`                                                                 | `outtime`         | —                                                                                  |
| `triage`     | `ED//TRIAGE//{TEMPERATURE_F,HEART_RATE,RESPIRATORY_RATE,O2_SATURATION,BP_SYSTOLIC,BP_DIASTOLIC}` | `edstays.intime`  | `numeric_value` (°F, bpm, /min, %, mmHg); LOINC parents                            |
| `triage`     | `ED//TRIAGE//PAIN`                                                                               | `edstays.intime`  | `numeric_value` if a number, else `text_value`; LOINC/72514-3                      |
| `triage`     | `ED//TRIAGE//ACUITY`                                                                             | `edstays.intime`  | `numeric_value`, ESI 1 (most urgent) to 5; no parent                               |
| `triage`     | `ED//TRIAGE//CHIEF_COMPLAINT`                                                                    | `edstays.intime`  | `text_value`; LOINC/10154-3                                                        |
| `vitalsign`  | `ED//VITALSIGN//{same six vitals}`                                                               | `charttime`       | `numeric_value`; LOINC parents                                                     |
| `vitalsign`  | `ED//VITALSIGN//PAIN`                                                                            | `charttime`       | as for triage                                                                      |
| `vitalsign`  | `ED//VITALSIGN//RHYTHM`                                                                          | `charttime`       | `text_value`                                                                       |
| `diagnosis`  | `ED//DIAGNOSIS//ICD//{icd_version}//{icd_code}`                                                  | `edstays.outtime` | `seq_num`; description and `ICD9CM/`/`ICD10CM/` parent from `hosp/d_icd_diagnoses` |
| `medrecon`   | `ED//MEDRECON//{name}//{NDC, 11 digits, or UNK}`                                                 | `charttime`       | `gsn` column; `NDC/…` parent                                                       |
| `pyxis`      | `ED//PYXIS//{name}`                                                                              | `charttime`       | `med_rn`, `gsn` columns                                                            |

## ETL decisions

**A separate spec, not a flag.** MEDS-Extract 0.8 has no optional tables and fails on a configured
table whose raw file is missing. Users without ED access would therefore be locked out of the base
spec, so the ED tables live in a fragment that is composed into its own registered spec. The ED
spec keeps the `demo` bucket, which the MNE spec drops for lack of a demo.

**Every code starts with `ED//`.** There are three reasons:

- `ED//ARRIVAL` and `ED//DISPOSITION` are not ED-visit markers; the base already has one (see the
    caveats). They carry what the base lacks: how the patient arrived and how they left.
- A code shared with the base would get a second description from this spec, and MEDS-Extract joins
    the two with a newline. For diagnoses the ED and hospital codes also mean different things: the
    ED's billing diagnosis at ED departure versus the hospital's at discharge.
- One prefix lets you drop the whole module when you want results comparable with `spec=MIMIC-IV`,
    or to remove the calendar-period signal that ED coverage carries.

**Timing is leakage-safe.**

- Triage has no timestamp of its own and happens on arrival, so it is timed at `edstays.intime`.
- Diagnoses are assigned at or after ED departure, so they are timed at `edstays.outtime`, never at
    arrival. The disposition is only known then, so it shares that time.
- Vitals, medication reconciliation and dispensations keep their own `charttime`.

**Vitals are numbers, as recorded.** Temperature stays in °F, the unit the ED charts in (median
98.0), and the code says so (`TEMPERATURE_F`); MNE's `TEMPERATURE_C` is a different code. Values
are not range-checked or cleaned, just as `icu/chartevents` is not. The codes share their names and
LOINC parents with MNE's exam vitals. A null column emits no event.

**Triage and charted vitals get separate codes.** `ED//TRIAGE//…` and `ED//VITALSIGN//…` stay
apart, because triage values are the arrival snapshot that ED models and the Xie et al. benchmark
key on.

**`pain` is split, not dropped.** It is a string: 96.3% of non-null triage entries and 89.3% of
vital-sign entries are plain numbers. Those go to `numeric_value`. Anything else ("unable",
"sleeping", "0/10") stays in `text_value` under the same code, the rule `hosp/omr` follows.

**Acuity is a number.** The ESI is an ordinal 1–5 scale, the same kind of value as the pain score,
so it goes to `numeric_value` under one code. It is not split into five codes. It has no parent:
LOINC has no ESI term, and its triage-level codes are NAMCS survey items.

**Free text rides in `text_value`.** That covers `chiefcomplaint` (60k distinct strings) and
`rhythm` (1.2k distinct strings, charted in 3.8% of vital-sign rows). Neither is mapped to a
vocabulary, and putting them in the code would fragment the vocabulary.

**One event per medication or dispensation.** The vocabulary rows repeat the drug:

- `medrecon` repeats each medication once per FDB therapeutic class (`etc_rn`); 13.9% of
    medications have two or more classes.
- `pyxis` repeats each dispensation once per GSN (`gsn_rn`).

Only the first row of each is emitted. Emitting every row would count those medications two or
three times.

**Medication codes follow the base.**

- `medrecon` names are a pick list with some free entries (18.7k distinct), so its code takes the
    drug name and the NDC, the way `hosp/pharmacy` builds `MEDICATION//START//{name}//{ndc}`. The
    NDC is attached as an `NDC/` parent.
- The NDC is stored as an integer and loses its leading zeros, so the spec restores them to 11
    digits.
- `pyxis` names come from the cabinet formulary (1.1k distinct), so the code is the name alone.
- Both tables also carry the GSN (First Databank's Generic Sequence Number), but it is not in the
    public OHDSI vocabularies. It is kept as an extension column, not used as a parent.

**Repeated rows are kept apart.** Diagnoses carry `seq_num`, because 249 (stay, ICD code) pairs
repeat. Pyxis carries `med_rn`, which numbers a stay's dispensations; two same-minute dispensations
of one drug would otherwise collapse at the merge stage's `unique_by: "*"`. Vital-sign rows are
unique per (stay, `charttime`).

## Caveats and limitations

!!! warning "ED detail exists only for 2011–2019"

    MIMIC-IV 3.1 spans 2008–2022, so whether a subject has `ED//` events depends on the calendar
    period. Among 3.1 subjects, the share with an ED-2.2 stay is 29.8% in anchor years 2008–2010,
    71.6% / 88.5% / 86.8% across 2011–2019, and 0% in 2020–2022, which MIMIC-IV 3.x added.
    MIMIC-IV-ED was last released (2.2) before those years were added. A model can use the
    presence of `ED//` events as a proxy for time, so restrict cohorts to 2011–2019, or drop the
    `ED//` codes, when this matters.

!!! warning "Do not count ED visits with `ED//ARRIVAL`"

    The base already marks ED visits:

    - `TRANSFER_TO//ED//…` marks every ED stay, 2008–2022 (784,400 in 3.1).
    - `ED_REGISTRATION` and `ED_OUT` mark only stays that led to an admission.

    `ED//ARRIVAL` covers just the 425k stays in ED 2.2. It sits at the same time as their
    `TRANSFER_TO//ED` event (`intime` agrees for every matched stay), and at `ED_REGISTRATION` for
    99.7% of admitted ones. Count visits with `TRANSFER_TO//ED`; read `ED//ARRIVAL//…` as the
    transport mode.

- **Leakage.** Diagnoses and disposition sit at `outtime` by design. Any prediction made during the
    ED stay must cut off before it, or it will see the outcome. Charted times do not always fall
    inside the stay: 8.5% of vital-sign rows, 9.6% of dispensations and 1.1% of reconciliation
    rows are charted after `outtime`. They are kept as charted.
- **Free-text fields.**
    - Chief complaint, rhythm and non-numeric pain are unnormalised text in `text_value`.
    - 2.8% of numeric triage pain scores fall above 10.
    - 477 triage and 4,200 vital-sign temperatures are below 50 (Celsius entries or errors).
    - Other vitals have implausible maxima (for example a systolic pressure of 151,103).
- **Version mismatch with MIMIC-IV 3.1.**
    - MIMIC-IV-ED 2.2 was built against MIMIC-IV 2.2. The PhysioNet page says every ED stay is in
        `hosp/transfers`; against 3.1, 82 of 425,087 stays (0.02%) are not.
    - 39 of 205,504 ED subjects are not in 3.1's `hosp/patients`. Their ED events form timelines
        with nothing else on them.
    - 26 ED `hadm_id`s are not in 3.1's admissions.
- **5 diagnosis codes have no description.** They are absent from 3.1's `d_icd_diagnoses` (5 events
    of 899,050).
- **6 stays end before they start** (`outtime < intime`), so their diagnoses and disposition precede
    arrival. They are kept as recorded.

## Not extracted

- `edstays.gender`: `hosp/patients` has it for every subject (it agrees for 99.96% of ED stays).
- `edstays.race`: it agrees with `hosp/admissions.race` for 98.9% of linked stays. ED-only subjects
    therefore still have no `RACE//…` event. Re-emitting race under the base's `RACE//` code would
    give those codes a second, newline-joined description.
- Medication reconciliation's `etccode`/`etcdescription` (First Databank's Enhanced Therapeutic
    Class): proprietary, multi-valued for 13.9% of medications, and not in the public vocabularies.
- Further GSNs of a dispensation (`gsn_rn > 1`). Only the first is carried.
- The `icd_title` of `ed/diagnosis`: the description comes from `hosp/d_icd_diagnoses` instead,
    which matches the base's `DIAGNOSIS` codes. Four codes carry more than one ED title, which would
    otherwise produce newline-joined descriptions.

## Validation

**Full data (aggregates only, run locally).** This ran the six ED tables of the spec over the full
MIMIC-IV-ED 2.2, with the one MIMIC-IV 3.1 table they need (`hosp/d_icd_diagnoses`). It did not run
the whole MIMIC-IV ETL. Join rates against 3.1 were computed separately.

| Table                             |  Raw rows |         Events | ED stays | Subjects |  Codes |
| --------------------------------- | --------: | -------------: | -------: | -------: | -----: |
| `edstays` (arrival + disposition) |   425,087 |        850,174 |  425,087 |  205,504 |  5 + 8 |
| `triage`                          |   425,087 |      3,687,042 |  425,081 |  205,503 |      9 |
| `vitalsign`                       | 1,564,610 |      9,546,412 |  407,934 |  198,051 |      8 |
| `diagnosis`                       |   899,050 |        899,050 |  423,989 |  205,129 | 13,210 |
| `medrecon`                        | 2,987,342 |      2,598,365 |  307,196 |  141,464 | 29,485 |
| `pyxis`                           | 1,586,053 |      1,189,422 |  303,361 |  156,768 |  1,102 |
| **Total**                         |           | **18,770,465** |  425,087 |  205,504 | 43,827 |

- On MEDS-Extract 0.8.1, with the base's 1,000 subjects per shard, the run took **4.6 min**, with
    a **0.7 GB** peak RSS.

- No event has a null time, and none was dropped for one.

- **Dropped rows:**

    - Every dropped row is a null measurement (one row × column with no value) or the ETC/GSN
        fan-out (medrecon 388,977 rows; pyxis 396,631).
    - Event counts match the raw non-null counts exactly, so nothing collapsed at the merge stage.
    - Every row of the five child tables has its `edstays` stay.

- **Join rates against MIMIC-IV 3.1:**

    - `stay_id` in `transfers.transfer_id`: 99.98%. Every match has `eventtype = ED`, the same subject
        and the same `intime`.
    - `subject_id` in `patients`: 99.98%.
    - `hadm_id` in `admissions`: 99.99% of the 47.8% of stays that have one.
    - 99.76% of `ADMITTED` stays have a `hadm_id`.

- **Medians are plausible.**

    |         | Heart rate | Systolic BP | Diastolic BP | Resp. rate | SpO₂ | Temperature | Other         |
    | ------- | ---------- | ----------- | ------------ | ---------- | ---- | ----------- | ------------- |
    | Triage  | 84         | 133         | 77           | 18         | 99   | 98.0 °F     | ESI 3, pain 5 |
    | Charted | 79         | 126         | 72           | 18         | 98   | 98.1 °F     |               |

- **Codes.**

    - All 43,827 codes are in the code metadata.
    - Every code but the 5 diagnosis codes above has a description, and none is multi-line.
    - 37,322 carry a parent: ICD, LOINC, and NDC for 24,102 of the 29,485 medication-reconciliation
        codes.

- **ED-only subjects.** Of the 141,175 subjects in 3.1 with no hospital admission, 79,370 (56.2%)
    gain ED events, a median of 30 each. The rest had ED visits only outside 2011–2019.

**Demo (public; `dataset_key=demo`).**

- All 64 ED-demo subjects are MIMIC-IV-demo subjects, so the ED events extend existing timelines.

- All 222 ED stays match an ED transfer in the demo's `hosp/transfers`.

- All 172 `hadm_id`s are in its admissions.

- The run adds 12,328 `ED//` events to the base demo's ~1.03M events:

    | ARRIVAL | DISPOSITION | TRIAGE | VITALSIGN | DIAGNOSIS | MEDRECON | PYXIS |
    | ------: | ----------: | -----: | --------: | --------: | -------: | ----: |
    |     222 |         222 |  1,819 |     6,313 |       545 |    2,411 |   796 |

- It adds 1,266 codes, all described.

- Every MIMIC-IV demo subject has an admission, so the demo cannot show the ED-only gain.

- `tests/e2e_ed_demo_test.py` rebuilds every expected ED event from the raw demo tables with polars
    and compares them exactly with the spec's output, including times, values and extension columns.
