# MIMIC-CXR-DB

!!! warning "Open issue: report-derived events are timed before the report existed"

    CheXpert labels (`CXR_NLP//CHEXPERT`) and radiology-report concepts (`CXR_NLP//RADIOLOGY`) are
    timed at the study's **last image**. The radiologist's report was signed later, by an amount
    MIMIC-CXR does not distribute. A model predicting at or shortly after a chest X-ray can
    therefore see that study's findings before a clinician could have read them. This is
    **leakage** for any time-sensitive task.

    The timing rule is not settled. The options under consideration are:

    - **A fixed reporting delay**: shift the report-derived events by a constant lag after the last
        image. MESSY can express this in the spec (`$study_end_datetime::"..." + N::hours`), but
        any constant is a guess, and too short a lag still leaks.
    - **The report's stored time from MIMIC-IV-Note**: the `radiology` table of MIMIC-IV-Note is
        the likely source of a report-stored timestamp (`storetime`). It has no MIMIC-CXR
        `study_id`, though, and whether its notes can be linked to CXR studies reliably is
        **unverified**. It would also need a third PhysioNet approval.
    - **Keep the current timing, with this warning** (today's behavior).

    Until it is settled, drop `CXR_NLP//CHEXPERT` and `CXR_NLP//RADIOLOGY` for leakage-sensitive
    tasks, or shift them yourself. The `CXR//` image and study events, and the discharge concepts
    (timed at `dischtime`), are not affected.

## Summary

The opt-in spec **`MIMIC-IV-CXRDB`** is the full `MIMIC-IV` ETL plus three tables of
[MIMIC-CXR-DB](https://physionet.org/content/mimic-cxr-ext-mimic-cxr-db/1.0/). It adds what MIMIC-IV
has no table for:

- **Chest radiographs.** One event per image at its acquisition time, with its view position and
    patient orientation, and one per study with its procedure. Both carry `study_id` and `dicom_id`,
    so a MEDS event can be traced to its MIMIC-CXR-JPG image. This covers 357,507 images and 215,990
    studies of 61,868 MIMIC-IV subjects, including the 61% of images taken outside any inpatient
    admission.
- **CheXpert labels.** The 14 report-derived findings of MIMIC-CXR-JPG, as
    `POSITIVE`/`NEGATIVE`/`UNCERTAIN`.
- **Ontology concepts from radiology reports and discharge summaries.** These are named-entity mentions
    linked to SNOMED CT, LOINC, OCHV, MeSH and other vocabularies, keyed by the note section they
    came from.

It adds 1.96 M events. 71% of them come from text (`CXR_NLP//`), and all of those can be dropped by
prefix.

Everything else in MIMIC-CXR-DB re-derives MIMIC-IV and is not extracted (see
[Not extracted](#not-extracted-and-why)).

## Source

|            |                                                                                                                                                                                                                                                                    |
| ---------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Dataset    | MIMIC-CXR-Ext MIMIC-CXR-DB: Linking MIMIC-IV Chest Radiographs with Rich and Harmonized Clinical Context                                                                                                                                                           |
| Version    | 1.0 (published 2026-10-01)                                                                                                                                                                                                                                         |
| URL        | <https://physionet.org/content/mimic-cxr-ext-mimic-cxr-db/1.0/>                                                                                                                                                                                                    |
| DOI        | [10.13026/gy0s-e371](https://doi.org/10.13026/gy0s-e371)                                                                                                                                                                                                           |
| Access     | PhysioNet credentialed access: a credentialed account, CITI "Data or Specimens Only Research" training, and the signed PhysioNet Credentialed Health Data Use Agreement 1.5.0 *for this project*. This is separate from your MIMIC-IV approval, and you need both. |
| License    | PhysioNet Credentialed Health Data License 1.5.0                                                                                                                                                                                                                   |
| Built from | MIMIC-IV 3.1, MIMIC-IV-Note 2.2 (discharge summaries), MIMIC-CXR 2.1.0 (radiology reports), MIMIC-CXR-JPG 2.1.0 (DICOM metadata, CheXpert labels)                                                                                                                  |

Please cite the dataset and its paper:

> Turki, H., Ismaila, L., Ben Salem, A., Nebli, A., Kchaou, M., Dere, A., Alzahrani, A., & Koubaa,
> M. (2026). MIMIC-CXR-Ext-MIMIC-CXR-DB: Linking MIMIC-IV Chest Radiographs with Rich and Harmonized
> Clinical Context (version 1.0). PhysioNet. RRID:SCR_007345. https://doi.org/10.13026/gy0s-e371

> Turki, H., Ismaila, L. E., Ben Salem, A., Nebli, A., Kchaou, M., Dere, A. A., Alzahrani, A., &
> Koubaa, M. (2026). MIMIC-CXR-DB: An Admission-Linked, Ontology-Grounded Multimodal Resource Built on
> MIMIC-CXR and MIMIC-IV. In *Medical Image Computing and Computer Assisted Intervention – MICCAI 2026
> Workshops and Challenges* (MultiTab), LNCS 17263. Springer.
> [Open access](https://papers.miccai.org/miccai-2026-sat/MultiTab_015.html).

PhysioNet asks that the parent projects be cited too. The works the extracted content builds on are:

- **MIMIC-CXR** (radiographs and reports): Johnson, A. E. W., et al. (2019). MIMIC-CXR, a
    de-identified publicly available database of chest radiographs with free-text reports.
    *Scientific Data* 6, 317. [PhysioNet](https://physionet.org/content/mimic-cxr/2.1.0/).
- **MIMIC-CXR-JPG** (DICOM metadata and the CheXpert labels): Johnson, A. E. W., et al. (2019).
    MIMIC-CXR-JPG, a large publicly available database of labeled chest radiographs.
    arXiv:1901.07042. [PhysioNet](https://physionet.org/content/mimic-cxr-jpg/2.1.0/).
- **CheXpert labeler** (the 14 labels): Irvin, J., et al. (2019). CheXpert: A large chest
    radiograph dataset with uncertainty labels and expert comparison. *AAAI* 33, 590–597.
- **MIMIC-IV** and **MIMIC-IV-Note**: Johnson, A. E. W., et al. (2023). MIMIC-IV, a freely
    accessible electronic health record dataset. *Scientific Data* 10, 1.
- **The concept pipeline**, as described on the PhysioNet page and in the paper:
    - Notes are split into sections by header patterns.
    - **scispaCy** `en_core_sci_lg` NER runs on each section (Neumann, M., et al. (2019). ScispaCy:
        Fast and robust models for biomedical NLP. *BioNLP*).
    - **NegEx** (negspacy) removes negated mentions.
    - Each entity is linked by **RapidFuzz** fuzzy matching (≥ 90% similarity), against twelve
        vocabularies in a fixed order. SNOMED CT comes first, built locally with ROBOT and rdflib,
        then OCHV, MeSH, PathLex, RadLex, SOHO, GALEN, LOINC, UPHENO, DCM, SYMP and IOBC from
        BioPortal.
    - Physicians reviewed the schema, the section-title mapping and the concept classes, but not
        individual triples.
    - The `*_jev` tables come from a further automated plausibility screen, **TypeSafe's Jev**, for
        which no reference is given.
- **PhysioNet**: Pollard, T., et al. (2026). PhysioNet as a global platform for biomedical research.
    *Nature Health*. https://doi.org/10.1038/s44360-026-00096-z

## How to run

MIMIC-CXR-DB ships **semicolon-delimited** `.csv` files whose fields contain commas. MEDS-Extract
0.8.1 reads every `.csv` with a comma separator and has no option to change it. So one conversion
step runs between the download and the extraction. It is the package's module `prepare_cxr_db`, which
needs only polars. (A CSV separator option for MEDS-Extract would remove the format conversion; it
is not released yet.)

```bash
# Needs a PhysioNet account approved for BOTH MIMIC-IV and MIMIC-CXR-DB.
export DATASET_DOWNLOAD_USERNAME=... DATASET_DOWNLOAD_PASSWORD=...
meds-extract-download spec=MIMIC-IV-CXRDB output_dir=$RAW # MIMIC-IV + 3 CXR-DB tables (~140 MB)
python -m MIMIC_IV_MEDS.prepare_cxr_db $RAW               # writes $RAW/cxr_db/*.parquet (~1 s)
meds-extract-run spec=MIMIC-IV-CXRDB do_download=false input_dir=$RAW output_dir=$MEDS_OUTPUT_DIR
```

!!! warning "`do_download=true` does not work for this spec"

    A one-step `meds-extract-run spec=MIMIC-IV-CXRDB` downloads the data but then fails, because
    the `cxr_db/*.parquet` files it reads do not exist until `prepare_cxr_db` has run. Use the three
    commands above.

If you already have the release, put `radiographs.csv`, `radiology_jev.csv` and `discharge_jev.csv`
at the root of `input_dir`, next to `hosp/` and `icu/`, then run the last two commands. The converter
writes to `cxr_db/` and leaves the `.csv` files alone, so a later download still verifies them.
MEDS-Extract refuses a table that resolves to both a `.csv` and a `.parquet`, which is why the
output goes to a subdirectory.

There is no public demo of MIMIC-CXR-DB, so `dataset_key=demo` is not available for this spec. CI
checks it with fabricated rows keyed to MIMIC-IV demo subjects (`tests/e2e_cxr_db_synthetic_test.py`).

## Tables → events

| Source table                      | Code                                                                  | Time                         | Values and columns                                                  |
| --------------------------------- | --------------------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------- |
| `radiographs` (one row per image) | `CXR//IMAGE//{ViewPosition}//{PatientOrientation}` (`UNK` if missing) | the image's `study_datetime` | `study_id`, `dicom_id`, `hadm_id` (null if not admission-linked)    |
| `radiographs`                     | `CXR//STUDY//{ProcedureCodeSequence_CodeMeaning}`                     | the study's last image       | `study_id`, `hadm_id`                                               |
| `radiographs` (14 label columns)  | `CXR_NLP//CHEXPERT//{FINDING}//{POSITIVE,NEGATIVE,UNCERTAIN}`         | the study's last image       | `study_id`, `hadm_id`; a blank label emits nothing                  |
| `radiology_jev`                   | `CXR_NLP//RADIOLOGY//{report section}//{ontology id}`                 | the study's last image       | `study_id`, `hadm_id`; `concept_class` and parents in code metadata |
| `discharge_jev`                   | `CXR_NLP//DISCHARGE//{summary section}//{ontology id}`                | `hosp/admissions.dischtime`  | `hadm_id`; `concept_class` and parents in code metadata             |

Examples:

- `CXR//IMAGE//AP//Erect`
- `CXR//STUDY//CHEST (PORTABLE AP)`
- `CXR_NLP//CHEXPERT//PLEURAL_EFFUSION//UNCERTAIN`
- `CXR_NLP//RADIOLOGY//IMPRESSION//snomed:<id>`
- `CXR_NLP//DISCHARGE//Past Medical History//ochv:<id>`

Every code is described in `metadata/codes.parquet`:

- An image or study code names its DICOM fields.
- A CheXpert code names the finding and the labeler.
- A concept code gives the concept's label, its `d_concepts` class, its ontology id, its section and
    its provenance.

Concept codes also carry:

- a `concept_class` metadata column, for filtering;
- `parent_codes` of `SNOMED/<id>` or `LOINC/<id>` where the concept is a SNOMED CT or LOINC concept
    (890 codes).

## ETL decisions

**Only what MIMIC-IV lacks.** MIMIC-CXR-DB is a convenience join, and most of it is MIMIC-IV.
Re-emitting it would double-count events (see [Not extracted](#not-extracted-and-why)). What
remains is the CXR content: images, studies, labels and report concepts. It also includes the
discharge-summary concepts, because MIMIC-IV has no structured view of note text.

**Images and studies are separate events.** A study is one exam with 1–11 images (median 2):

- The image event records each view (AP, PA, lateral, ...), with its own acquisition time and the
    `dicom_id` that names the image file.
- The study event records the exam once, with its procedure.
- The CheXpert labels are constant within every study (0 of 227,827 studies vary). They are
    study-level facts, so they hang off the study, not the image.

DICOM fields become code components only when they are small, standardized enums. `ViewPosition`
and `PatientOrientation` (Erect/Recumbent) form the image code, and the procedure forms the study
code. Other fields are dropped:

- `Rows`/`Columns` are pixel dimensions of a file this dataset does not contain, and are reachable
    through `dicom_id`.
- `ViewCodeSequence` restates `ViewPosition`.
- `PerformedProcedureStepDescription` is a noisier copy of the procedure (9% null, plus a
    placeholder value on another 9%).
- `StudyDate`/`StudyTime` are `study_datetime`.

**Study-level events are timed at the study's last image.** Images of one study are acquired minutes
apart: median 278 s and maximum 12 h, in the 4,588 studies (2%) where they differ. The report, and
therefore every label and concept, cannot predate the last image, so that is the earliest honest
time for them (but still before the report was readable; see the warning at the top). The study
event goes there too, so a study's events sit together. `radiology_jev` gets this time from an
aggregated join, `study_datetime: max` over the study's images. `radiographs` cannot do the same
for its own study-level events: MEDS-Extract 0.8.1 rejects a table joining itself (every joined
column would collide with the left side, and joined columns cannot be renamed), and dftly has no
window aggregation. So `prepare_cxr_db` adds this one column to `radiographs`,
`study_end_datetime`. It is the only derived value in the conversion.

**Unlinked studies are kept.** 61% of images fall outside every inpatient admission and have a null
`hadm_id`. Their subject and time are still known, so they are emitted without a `hadm_id`. 67% of
them fall inside a MIMIC-IV-ED stay; none falls inside an admission window, so the release's
linkage is complete for its rule.

**Subjects absent from MIMIC-IV 3.1 are dropped.** These are 3,511 subjects (19,588 images, 5.2%).
They would have no birth, demographics or other events. Worse, MEDS-Extract draws the
train/tuning/held-out split from every table's subjects, so adding them would reshuffle the splits
of the base cohort. `subject_id` is therefore gated on `hosp/patients`, and enabling this extension
leaves the subject set and splits of `MIMIC-IV` unchanged.

**CheXpert labels go in the code, not `numeric_value`.** The label values 1.0 / 0.0 / −1.0 are
categories: −1 means "uncertain", not "less than negative". Averaging them would be meaningless,
so the label status is a code component. A blank label means the report does not mention the
finding, and emits nothing. `No_Finding` is only ever 1.0 or blank.

**The Jev-screened triples, with the (section, concept) pair as the code.**

- `radiology_jev` and `discharge_jev` are *exactly* the full `radiology`/`discharge` tables
    restricted to the (section, concept) pairs that passed TypeSafe Jev's automated plausibility
    screen. Every Jev triple is in the full table, and every full triple of a passing pair is in
    Jev.
- Jev keeps 31% of radiology triples and 8.7% of discharge triples. The full tables have 14,174 +
    151,885 (section, concept) pairs, dominated by generic and stem-artifact concepts: 88% of rows
    are OCHV `Domain_Specific` matches of words such as "patient" or "left", per the paper's audit.
    The Jev tables have 3,387 + 12,472.
- That validated pair is the unit the code uses. `relation_type` is the note section a concept was
    found in, not a predicate. It changes meaning materially: `pneumonia` under `INDICATION` is a
    question, under `IMPRESSION` a finding, and under `Family History` it describes a relative. So
    it stays in the code.
- The 15,859 concept codes are a vocabulary on the scale of ICD. The full tables would add about
    166k, far past usefulness (cf. MNE's rejection of 234k free-text history concepts).

**No `class` filter.** Filtering out the generic `Domain_Specific` class looks tempting, but that is
where the CheXpert findings live. Of the radiology Jev rows whose concept label matches a CheXpert
finding stem, 99.3% are `Domain_Specific` (OCHV), and only 631 rows are
`Clinical_Findings_and_Diseases`. The class is exposed as the `concept_class` metadata column, so a
user can filter by it deliberately.

**A repeated concept collapses.** A concept mentioned twice in one section of one note becomes one
event: the merge stage's `unique_by: "*"` drops identical rows. This removes 2,522 radiology and
14,747 discharge duplicate rows. The events mean "this concept appears in this section", not a
mention count.

**Discharge concepts are timed at `hosp/admissions.dischtime`.** The summary is written at
discharge. This is the rule MNE, `DIAGNOSIS` and `DRG` already follow.

**`CXR//` vs `CXR_NLP//`.** `CXR//` codes are DICOM header fields. Everything derived from report or
note text by a labeler or NER pipeline starts with `CXR_NLP//`, so a structured-data-only cohort
can drop it with one prefix filter.

## Caveats and limitations

**Leakage.** Report-derived events are timed before the report existed. This is an open issue;
see the warning at the top of this page.

**Coverage.**

- Images and studies cover the MIMIC-CXR population of 2011–2016 (in real years), not all of
    MIMIC-IV.
- Only 39% of images are admission-linked.
- Radiology concepts exist only for admission-linked studies: 78,073 studies, about 70% of the
    linked ones. Unlinked studies, mostly ED and outpatient films, have labels but no concepts.
- Discharge concepts cover 30,532 admissions after Jev (36,304 before). That is 76% of the
    admissions with a linked radiograph. Admissions without one have none.

**Selection.** The paper finds that the linked subset is enriched for admitted, sicker patients
(69.8% AP films linked vs 19.5% unlinked). Do not treat it as representative of MIMIC-CXR.

**NLP-derived quality.**

- The CheXpert labels are automated, not radiologist ground truth.
- The triples are the unreviewed output of an automated pipeline. No precision estimate exists,
    and Jev is itself automated.
- Known error modes per the paper:
    - stem artifacts ("atelectasi");
    - abbreviations linked to the wrong expansion ("PLT" → "primed lymphocyte test", 0.47% of
        discharge triples);
    - many generic concepts.
- NegEx removed negated mentions, so a missing concept is *not* a negative finding. CheXpert
    `NEGATIVE`, by contrast, is an explicit negation.
- Treat the triples as a recall-oriented index of the notes.

**Overlap with MIMIC-Note-Extract.** Both extensions read the same MIMIC-IV-Note discharge
summaries, and 86.4% of `discharge_jev`'s admissions are also in MNE. A user enabling both would get
two NLP views of one note:

- MNE: LLM-structured sections with free text;
- CXR-DB: ontology concepts per section.

Both are timed at `dischtime`, and both are separable by prefix (`NOTE//` vs `CXR_NLP//DISCHARGE`).
Each registered spec today is the base plus one fragment, so no spec contains both.

**Overlap with base MIMIC-IV.** The discharge concepts' sections parallel structured data the base
already has:

- `Diagnoses.*` parallels `DIAGNOSIS//ICD`;
- `Laboratory and Results.*` parallels `LAB`;
- `Discharge and Transition.Medications` parallels the prescriptions.

These are different representations of related facts, not duplicates, but a model sees both. The
ICU also charts chest X-rays as `PROCEDURE//START//225459` and `PROCEDURE//START//229581`. Of the
52,245 studies taken during an ICU stay, 37% have such a charted event within 2 h. Both are kept:
one is the nurse's record of the procedure, the other the image itself.

**Smaller issues.**

- One study's images disagree on `hadm_id`, so its label events are emitted twice, once per
    admission.
- 205 studies name different procedures on different images, so they get two study events.
- 34 images fall after the subject's date of death (6 by more than a day). This is inherited from
    the source; MIMIC-CXR and MIMIC-IV share date shifts.

## Not extracted, and why

| Table or column                                                                                                  | Why                                                                                                                                                                                                                                                                                                                                                                                             |
| ---------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `admissions`                                                                                                     | `hosp/admissions` + `hosp/patients`, verbatim. Times, type, locations, marital status, race, gender and `anchor_age` agree on 100% of its 40,172 admissions (129 rows are duplicates). `Primary Diagnosis` is the `seq_num` 1 `diagnoses_icd` title for 99.85%, 75% of them ICD-9 despite the README's "ICD-10". 97.6% of `Other Diagnoses` items are titles of the same admission's diagnoses. |
| `labs`, `d_labs`                                                                                                 | The earliest in-admission `labevents` value: it matches for 99.97% of rows, and every value occurs in that admission's `labevents`. It has no timestamp. The release holds one row per admission (38,264 rows, 38,264 admissions), not the documented 36-analyte panel. The base already emits every lab with its time.                                                                         |
| `d_concepts`                                                                                                     | The Jev tables carry each concept's `label` and `class` (identical to `d_concepts` for 100% of rows).                                                                                                                                                                                                                                                                                           |
| `radiology`, `discharge` (unscreened)                                                                            | 4.96 M rows and about 166k (section, concept) codes, dominated by generic concepts; see [ETL decisions](#etl-decisions). To use them instead, change the two `*_jev` table names and the `include` list in `extensions/cxr_db.yaml`, add them to `prepare_cxr_db.TABLES`, and join `d_concepts` for the label and class.                                                                        |
| `Rows`, `Columns`, `ViewCodeSequence_CodeMeaning`, `PerformedProcedureStepDescription`, `StudyDate`, `StudyTime` | Redundant or file-level; see [ETL decisions](#etl-decisions).                                                                                                                                                                                                                                                                                                                                   |

## Validation

These numbers come from running the extension's three tables, with their `hosp/patients` and
`hosp/admissions` joins, on the real MIMIC-CXR-DB 1.0 and MIMIC-IV 3.1, with MEDS-Extract 0.8.1.
They are unchanged from the earlier run on 0.7.0. Only aggregates were inspected. The composed spec as a whole runs in CI over the MIMIC-IV demo with synthetic CXR-DB
rows.

| Family               | Events        | Distinct codes | Subjects   | Studies     | Admissions                               |
| -------------------- | ------------- | -------------- | ---------- | ----------- | ---------------------------------------- |
| `CXR//IMAGE`         | 357,507       | 23             | 61,868     | 215,990     | 40,172 (59% of events have no `hadm_id`) |
| `CXR//STUDY`         | 216,197       | 27             | 61,868     | 215,990     | 40,172                                   |
| `CXR_NLP//CHEXPERT`  | 622,822       | 40             | 61,174     | 213,701     | 39,995                                   |
| `CXR_NLP//RADIOLOGY` | 483,999       | 3,387          | 21,938     | 78,073      | 31,382                                   |
| `CXR_NLP//DISCHARGE` | 279,982       | 12,472         | 20,849     | —           | 30,532                                   |
| **Total**            | **1,960,507** | **15,949**     | **61,868** | **215,990** | **40,173**                               |

The CheXpert events split into 401,098 `POSITIVE`, 150,639 `NEGATIVE` and 71,085 `UNCERTAIN`.

**Join rates.**

- 94.6% of CXR-DB subjects (94.8% of images) are in 3.1 `hosp/patients`.
- Every linked `hadm_id` is in `hosp/admissions`, with the same subject.
- 100% of linked images fall inside their admission's `[admittime, dischtime]`.
- Every `discharge_jev` admission is in 3.1.
- 99.995% of `radiology_jev` study ids are in `radiographs`.

**Rows dropped, and why.**

- 19,588 images (11,837 studies, 3,511 subjects) belong to subjects not in MIMIC-IV.
- 10 `radiology_jev` rows have 4 study ids with no image.
- 2,522 radiology and 14,747 discharge rows are exact duplicates and collapse.
- No discharge rows are dropped.

**Checks.**

- No event has a null time.
- Image events equal images of MIMIC-IV subjects exactly.
- CheXpert events equal the non-blank labels over studies, plus 5 from the one study with two
    `hadm_id`s.
- Every study-level event sits at its study's last image, both those from `radiographs` (via the
    converter's `study_end_datetime`) and those from `radiology_jev` (via the aggregated join).
- Every discharge concept sits at `dischtime`.
- All 15,949 codes are described, on one line each; 890 carry SNOMED/LOINC parents.

**Cost** (on a laptop, wall time and peak resident memory from `/usr/bin/time -l`):

- `prepare_cxr_db`: about 2 s and 0.5 GB, including interpreter start-up.
- The extension's tables through `meds-extract-run` on 0.8.1: about 26 s and 1.35 GB, including
    each stage's process start-up and converting `hosp/patients` and `hosp/admissions`. This is
    negligible next to the base ETL.
