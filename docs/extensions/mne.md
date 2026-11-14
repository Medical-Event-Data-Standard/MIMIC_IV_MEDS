# MIMIC-IV-MNE (MIMIC-Note-Extract)

`spec=MIMIC-IV-MNE` is the full [base ETL](../base-etl.md) plus the structured discharge-summary
tables of MIMIC-IV-Ext MIMIC-Note-Extract (MNE).

## 1. Summary

MNE used rules plus an LLM to turn 13 sections of the MIMIC-IV-Note v2.2 discharge summaries into
tables keyed by `hadm_id`. This extension adds data that MIMIC-IV has no structured table for:

- past medical history (patient and family);
- home medications (on admission) and discharge medications;
- allergies, including an explicit "no known allergies";
- the physical examination: vital signs and per-organ-system findings. This gives vitals for most
    non-ICU admissions, where `hosp` has no inpatient vitals;
- chief complaint, clinician-written discharge diagnoses, major procedures and condition at
    discharge.

Against the full release it adds **17.6 M events** for 137,362 subjects. Every event is timed at the
admission's discharge, every code starts with `NOTE//`, and free text goes in `text_value`.

## 2. Source

| Field        | Value                                                                                                                                                                                                   |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Dataset      | MIMIC-IV-Ext MIMIC Note Extract: A Large-Scale Structured Dataset Extracted from MIMIC-IV-Note Discharge Summaries                                                                                      |
| Version      | 1.0 (published 21 October 2026)                                                                                                                                                                         |
| PhysioNet    | <https://physionet.org/content/mimic-iv-ext-note-extract/1.0/>                                                                                                                                          |
| DOI          | [10.13026/cq50-rd11](https://doi.org/10.13026/cq50-rd11)                                                                                                                                                |
| Access       | Credentialed PhysioNet users; CITI "Data or Specimens Only Research" training; PhysioNet Credentialed Health Data Use Agreement 1.5.0, signed **for this project**                                      |
| Licence      | PhysioNet Credentialed Health Data License 1.5.0                                                                                                                                                        |
| Paper        | Schwethelm, Kuntzer *et al.*, "MIMIC-Note-Extract: A Large-Scale Structured Dataset Extracted from MIMIC-IV-Note Discharge Summaries", [arXiv:2611.04685](https://arxiv.org/abs/2611.04685) (ML4H 2026) |
| Derived from | [MIMIC-IV-Note 2.2](https://physionet.org/content/mimic-iv-note/2.2/) discharge summaries; links to [MIMIC-IV](https://physionet.org/content/mimiciv/3.1/) via `subject_id`/`hadm_id`                   |
| Producer     | External contributors (not MIT-LCP)                                                                                                                                                                     |

Citations and BibTeX for the dataset, the paper, and MIMIC-IV-Note are in
[Sources and citations](../sources.md#mimic-note-extract).

## 3. How to run

```bash
# Needs a PhysioNet account approved for BOTH MIMIC-IV and MIMIC-Note-Extract.
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD
meds-extract-run spec=MIMIC-IV-MNE output_dir=$MEDS_OUTPUT_DIR
```

- **Download.** The spec's `dataset` bucket fetches MIMIC-IV 3.1 and then MNE 1.0 from
    `https://physionet.org/files/mimic-iv-ext-note-extract/1.0`. An `include` list fetches only the
    ten tables used (about 210 MB of the ~1.2 GB release; `discharge_sections.csv.gz` alone is
    845 MB). The list also keeps MNE's own `LICENSE`/`README` from overwriting MIMIC-IV's.
- **Local raw data.** With `do_download=false`, put MNE's `*.csv.gz` files at the root of
    `input_dir`, next to `hosp/` and `icu/`. This is where the download puts them.
- **No demo.** MNE has no demo subset, so the generated spec has no `demo` bucket and
    `dataset_key=demo` is not available.
- **Cost.** The MNE tables on their own take about 90 s and 1.4 GB peak memory to extract. The rest
    is the base ETL (see [runtime](../getting-started.md#runtime-and-compute)).
- **Checksums.** MNE's `SHA256SUMS.txt` separates hash and path with one space. `shasum -c` rejects
    that, but MEDS-Extract's downloader accepts it.

## 4. Tables → events

All events are timed at `hosp/admissions.dischtime` (joined on `hadm_id`) and carry `hadm_id`.

| MNE table                                           | Code                                                                                        | Values and extension columns                                                            |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| `past_medical_history`                              | `NOTE//PAST_MEDICAL_HISTORY//{PATIENT,FAMILY}//{category}//{CURRENT,PAST,UNK}`              | concept in `text_value`; `family_relation`; `seq_num`                                   |
| `medications_on_admission`, `discharge_medications` | `NOTE//MEDICATION//{ON_ADMISSION,AT_DISCHARGE}`                                             | drug name in `text_value`; `dose_value`, `dose_unit`, `route`, `prn`, `hold`, `seq_num` |
| `allergies`                                         | `NOTE//ALLERGY//{standardized allergen}`, `NOTE//ALLERGY//UNK`, `NOTE//ALLERGY//NONE_KNOWN` | allergy as written in `text_value` (not for `NONE_KNOWN`); `seq_num`                    |
| `physical_exam_vitals`                              | `NOTE//PHYSICAL_EXAM//{timepoint}//{vital}`                                                 | `numeric_value` in °C, bpm, mmHg, breaths/min, %, kg; LOINC parent codes                |
| `physical_exam_vitals`                              | `NOTE//PHYSICAL_EXAM//{timepoint}//O2_SUPPORT//{method}`                                    | flow in L/min in `numeric_value`, if stated                                             |
| `physical_exam_systems`                             | `NOTE//PHYSICAL_EXAM//{timepoint}//{system}//{NORMAL,ABNORMAL}`                             | abnormal findings in `text_value`, as a Postgres array literal                          |
| `discharge_condition`                               | `NOTE//DISCHARGE_CONDITION//{aspect}//{value}`                                              | none; one event per filled aspect (14 aspects)                                          |
| `discharge_diagnosis`                               | `NOTE//DISCHARGE_DIAGNOSIS//{PRIMARY,SECONDARY}`                                            | diagnosis in `text_value`; `seq_num`                                                    |
| `major_procedures`                                  | `NOTE//MAJOR_PROCEDURE//{THIS_ADMISSION,PREVIOUS}`                                          | procedure in `text_value`; `seq_num`                                                    |
| `chief_complaint`                                   | `NOTE//CHIEF_COMPLAINT//{FINDING,CONTEXT}`                                                  | complaint in `text_value`; `seq_num`                                                    |

`{timepoint}` is the exam's own `ADMISSION`, `DISCHARGE`, `INTERIM` or `UNKNOWN`. The vitals and
their LOINC parents are:

| Vital code part    | Unit        | Parent          |
| ------------------ | ----------- | --------------- |
| `TEMPERATURE_C`    | °C          | `LOINC/8310-5`  |
| `HEART_RATE`       | beats/min   | `LOINC/8867-4`  |
| `BP_SYSTOLIC`      | mmHg        | `LOINC/8480-6`  |
| `BP_DIASTOLIC`     | mmHg        | `LOINC/8462-4`  |
| `RESPIRATORY_RATE` | breaths/min | `LOINC/9279-1`  |
| `O2_SATURATION`    | %           | `LOINC/59408-5` |
| `WEIGHT_KG`        | kg          | `LOINC/29463-7` |

The discharge-condition aspects are `MENTAL_STATUS`, `LEVEL_OF_CONSCIOUSNESS`, `ACTIVITY_STATUS`,
`GENERAL_CONDITION`, `PAIN_STATUS`, `CARDIOVASCULAR_STATUS`, `VITAL_SIGNS_STATUS`, `FEVER_STATUS`,
`NUTRITION_STATUS`, `WOUND_PRESENCE`, `WOUND_INFECTION_STATUS`, `RESPIRATORY_STATUS`,
`ORIENTATION_STATUS` and `NEUROLOGICAL_STATUS`. Every code has a `_self` description that names the
MNE table and column it came from.

## 5. ETL decisions

**Every event is timed at `hosp/admissions.dischtime`.** MNE has no timestamps (the note's own times
are redacted), and all of it was written down at discharge. Placing any of it earlier in the stay
would leak information into prediction windows that could not have seen it. `DIAGNOSIS` and `DRG`
follow the same rule in the base ETL. Even an exam with timepoint `ADMISSION` is emitted at
discharge; the timepoint is kept in the code. In the exam data, ADMISSION is 54.5% of rows,
DISCHARGE 35.8%, UNKNOWN 8.9% and INTERIM 0.8%.

**Codes come only from MNE's closed enums; free text goes in `text_value`.** MNE does not map its
free text to any vocabulary. It has 234 k distinct history concepts, 257 k diagnosis titles, 123 k
procedure titles and 50 k admission-medication names. Putting them in the code would fragment the
vocabulary far more than the DRG titles did in the base ETL (see
[DRG](../base-etl.md#drg)). Allergies are the exception: MNE's standardized `title_std` (3,021 labels)
is a closed set, so it goes in the code.

**Every code starts with `NOTE//`.** The events come from LLM extraction, with item-level accuracy of
92.2–99.8% per section in the MNE paper. The prefix lets users drop all of them at once.

**`seq_num` is kept on every multi-row table**, as on the base `omr` blood-pressure events. 66 k
history rows (1.8%) and 17 k home-medication rows (0.7%) repeat within one admission except for
`seq_num`. Without it, the merge stage's `unique_by: "*"` would collapse them.

**"No known allergies" gets its own code, `NOTE//ALLERGY//NONE_KNOWN`.** MNE stores it as an allergy
row: 126 k rows (20.8% of the table), one per admission, always with a null `title_std`. Read
naively, it is an allergen. A row is classed as none-known only if it has no `title_std` and its text
matches "no known", "NKDA", "NKA" or "no allerg", so a standardized allergen can never be
misclassified. Real allergens without a standardized label (2,012 rows) go to `NOTE//ALLERGY//UNK`,
with the allergy as written in `text_value`.

**`NOT_ASSESSED` exam systems are not emitted** (36% of `physical_exam_systems`). They record that the
summary said nothing about the system, and MNE's authors warn that a missing entry is not evidence of
a normal finding.

**One event per filled column.** `physical_exam_vitals` and `discharge_condition` have one row per
admission (and timepoint), with one column per vital or aspect. Each filled column becomes its own
event, and a null column produces none.

**Vitals are normalized.**

- Temperature is taken from `temperature_c` only. `temperature_f` is present exactly when
    `temperature_c` is and always converts to it.
- Weight is given in kg or lbs and is normalized to kg. A weight with no unit is dropped.
- Oxygen support uses the method (a closed enum) in the code and the flow in L/min as the value.

**Medication doses are extension columns, not `numeric_value`.** One code covers every drug, so a
`numeric_value` would mix milligrams of one drug with puffs of another under the same code. Mapping
the drug names to RxNorm would let the drug enter the code. The MNE paper does such a mapping for its
evaluation, but it is not released.

**A separate spec.** MNE needs its own PhysioNet approval and has no demo, and MEDS-Extract fails on
missing tables. It is therefore an [extension spec](index.md#why-extensions-are-separate-specs), not
part of `spec=MIMIC-IV`.

## 6. Caveats and limitations

- **During a stay, MNE is visible only through earlier admissions.** Because everything is timed at
    discharge, a prediction made during a stay sees MNE only from *previous* admissions. 45.9% of
    admissions and 40.0% of ICU stays have one. An opt-in variant timing admission-type content
    (history, home medications, allergies, the ADMISSION exam) at `admittime` was discussed in
    PR #82 as possible follow-up work. It would trade some leakage for value within the stay.

- **Coverage is uneven.** MNE reaches 300,663 of MIMIC-IV 3.1's 546,028 admissions (55.1%) and 64.6%
    of ICU stays:

    | MIMIC-IV 3.1 group                                      | Covered by MNE                                                    |
    | ------------------------------------------------------- | ----------------------------------------------------------------- |
    | All hospital admissions                                 | **55.1%**                                                         |
    | Admissions ~2008–2019                                   | 61–68%                                                            |
    | Admissions ~2020–2022 (added in MIMIC-IV 3.x; no notes) | **7%**                                                            |
    | `EW EMER.` / `DIRECT EMER.` admissions                  | ~74%                                                              |
    | `EU OBSERVATION` admissions                             | 25%                                                               |
    | Admissions with / without an ICU stay                   | 66% / 53%                                                         |
    | ICU stays                                               | **64.6%**; ~80–82% for 2008–2016, 72% for 2017–19, 6% for 2020–22 |
    | Subjects: all / ever admitted / ever in ICU             | 37.7% / 61.5% / 75.2%                                             |

    Years are approximate (from each patient's `anchor_year_group`). The *presence* of MNE events is
    therefore partly a proxy for calendar period. Most sections reach 91–100% of MNE admissions;
    `major_procedures` (53%) is the exception.

- **LLM extraction errors.** Item-level accuracy ranges from 92.2% (pertinent results, not extracted
    here) to 99.8% (allergies). The paper's error audit counts 14 fabrications, 42 changes of meaning,
    231 omissions and 157 wrong attributes across 13,744 judged items. The authors warn that a missing
    entry is not evidence of absence. Physiologically implausible exam vitals are rare (under 0.1% per
    vital).

- **Unknown selection.** MNE filtered 9.4% of the source summaries (331,793 to 300,720) by criteria
    the paper does not spell out, so any selection bias is unknown.

- **Admissions missing from MIMIC-IV 3.1.** 57 MNE admissions no longer exist in 3.1. Their join
    leaves `dischtime` null, so their 3,522 rows are dropped. For the remaining admissions, `subject_id`
    agrees with MIMIC-IV on 100%.

- **Free text is unnormalized.** `text_value` holds MNE's strings as extracted, with no vocabulary
    mapping. Abnormal exam findings are kept verbatim as Postgres array literals (`{a,"b c"}`).

- **Shared column name.** MNE's medication `route` uses the same extension column as
    `hosp/pharmacy`'s `route`. Tell them apart by code (`NOTE//MEDICATION//…` vs `MEDICATION//START//…`).

- **Not a first-party dataset.** MNE is a v1.0 release by external contributors, not MIT-LCP.

## 7. Not extracted, and why

| MNE content                                                           | Reason                                                                                                                                                                                   |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `gender`, `services`, `discharge_disposition`                         | Duplicate MIMIC-IV's own timestamped columns. MNE's gender disagrees with `hosp/patients` for 0.04% of admissions; its `EXPIRED` agrees with `hospital_expire_flag` ~97–98% of the time. |
| `patient_died`, `patient_left_ama`                                    | Already in the base ETL, with times: `MEDS_DEATH` and `HOSPITAL_DISCHARGE//AGAINST ADVICE`.                                                                                              |
| `pertinent_results`                                                   | For a quarter of its rows it repeats structured labs and microbiology. The imaging, pathology and electrophysiology rest needs Postgres-array parsing first. A candidate for follow-up.  |
| Medication `strength` and `frequency`                                 | JSON in at least five different shapes.                                                                                                                                                  |
| `discharge_sections`                                                  | Verbatim note text (845 MB of the ~1.2 GB release).                                                                                                                                      |
| `admissions`                                                          | Only the hub that keys the other tables.                                                                                                                                                 |
| `temperature_f`                                                       | Always derivable from `temperature_c`.                                                                                                                                                   |
| Sparse columns (`warnings`, `duration_value`, `presentation_form`, …) | Nearly empty: `warnings` is non-empty in 0.08% of rows, `duration_value` is filled in 0.45%, `presentation_form` is 82% null.                                                            |

The download's `include` list skips the files for all unextracted tables.

## 8. Validation

**Full data (run locally against MNE 1.0 and MIMIC-IV 3.1; aggregate results only):** these
figures come from a build with MEDS-Extract 0.7, before the spec moved to 0.8.1 and gained the
suMEDS post-stage. The code-metadata row describes the ETL's own metadata, before enrichment.

| Check                                      | Result                                                                 |
| ------------------------------------------ | ---------------------------------------------------------------------- |
| Events / subjects / admissions             | 17.55 M / 137,362 / 300,663                                            |
| Events with a null time                    | 0                                                                      |
| Rows dropped for admissions missing in 3.1 | 3,522 (57 admissions), as expected                                     |
| `NOTE//ALLERGY//NONE_KNOWN`                | 126,120                                                                |
| `NOTE//ALLERGY//UNK`                       | 2,012                                                                  |
| `NOT_ASSESSED` codes emitted               | 0                                                                      |
| Exam-vital medians                         | SBP 128 mmHg, HR 78, SpO₂ 98%, temperature 36.7 °C, weight 79.6 kg     |
| Code metadata                              | all 3,244 codes described; the 28 exam-vital codes carry LOINC parents |
| Cost of the MNE tables                     | ~90 s, 1.4 GB peak memory                                              |

**CI ([`tests/e2e_mne_synthetic_test.py`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/tests/e2e_mne_synthetic_test.py)).**
MNE has no public subset, so CI runs `spec=MIMIC-IV-MNE` over the real public MIMIC-IV demo plus a few
fabricated MNE rows. The rows are keyed to real demo admissions and written to hit every routing
rule: the no-known-allergies row, an unstandardized allergen, a history row repeated under a new
`seq_num`, a redacted temperature, a weight in pounds and one without a unit, a `NOT_ASSESSED` exam
system, empty discharge-condition fields, and an admission that does not exist in MIMIC. The test
compares the full list of resulting `NOTE//` events exactly and checks that every one is at its
admission's `dischtime`. It also checks the extension columns and the code metadata, and, because
the demo tables are real, that the generated spec still runs the whole base ETL.
[`tests/extension_specs_test.py`](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/blob/main/tests/extension_specs_test.py)
checks that the generated spec is current and exactly base plus fragment.

The scoping analysis behind these decisions is in
[#81](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/issues/81) and the implementation
in [PR #82](https://github.com/Medical-Event-Data-Standard/MIMIC_IV_MEDS/pull/82).
