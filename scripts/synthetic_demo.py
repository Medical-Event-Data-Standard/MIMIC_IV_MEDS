"""Simulate the MIMIC-IV modules that have no public demo, keyed to the public MIMIC-IV demo.

The public MIMIC-IV demo (v2.2, ODbL) has 100 subjects with their hospital admissions and ICU stays,
and MIMIC-IV-ED has a public demo too. MIMIC-IV-Note, MIMIC-CXR, MIMIC-CXR-JPG and MIMIC-IV-ECHO
are credentialed and have none, so a demo build of any spec that reads them has nothing to read.
This script fabricates them, in each release's exact file layout (names, columns, delimiters,
compression), for the demo's real `subject_id`s, `hadm_id`s and times, so that one simulated world
is consistent across modules:

- `mimic-iv-note` 2.2: a discharge summary for every demo admission, charted at its `dischtime`
  and stored later, and radiology reports during admissions, ED visits, ICU stays and outside any
  stay; the `_detail` tables with authors, exam names, CPT codes and addendum links.
- `mimic-cxr` 2.1.0: a chest radiograph study for every chest X-ray report in the simulated
  radiology notes, with the same report text as `files/pNN/pXXXXXXXX/sZZZZZZZZ.txt`, and the
  record, study and provider lists. No DICOM files (see "Images" below).
- `mimic-cxr-jpg` 2.1.0: the metadata, CheXpert, NegBio, split and test-set label tables for those
  studies, and `IMAGE_FILENAMES`. The labels follow from the findings written into the report.
  No JPG files.
- `mimic-iv-echo` 0.1: echocardiography studies near stays, with the record and study lists.
  No DICOM files.

Modules with a public release are not simulated: MIMIC-IV hosp/icu and MIMIC-IV-ED have demos,
and MIMIC-IV-ECG (with its own demo) and the MIMIC-IV Waveform Database are open access.

**Provenance.** Nothing here is read, sampled or fitted from any credentialed dataset: no values,
no distributions, no category frequencies, not even file headers. The inputs are the public
demo's `hosp/admissions`, `hosp/patients`, `hosp/provider` and `icu/icustays`, the schemas in the
public documentation (cited per table in `data/synthetic_demo/README.md`), public vocabularies
(SNOMED CT and LOINC ids, checked against public terminology servers; CheXpert's label names;
common generic drug names), and templates for all free text ("Synthetic history of present
illness sentence 2."). Every probability below is an arbitrary round number chosen for coverage,
not an estimate. The output is for testing pipelines and for tutorials: it describes no one and
must not be used for research.

**Images.** No DICOM or JPG files are written, only the lists and metadata that describe them.
No spec here reads pixels; a placeholder image would be a few hundred bytes but would add hundreds
of files that no image model can use, and a consumer that does open images has to handle missing
files anyway (MIMIC-CXR-JPG's own `IMAGE_FILENAMES` exists for partial downloads).

**Determinism.** Every random draw comes from a `random.Random` seeded with the global seed and a
fixed key (an admission id, a subject id or a module name), so the output depends only on the
seed, this script and the pinned demo release, and adding one module does not reshuffle another.
Gzip is written with a zero mtime and no file name; the bytes can still differ across zlib builds,
so `check` compares decompressed contents.

**Delivery.** The files are committed under `data/synthetic_demo/<physionet-slug>/` in the
release's layout. `pin --ref <commit>` writes `data/synthetic_demo/sources.yaml`: for each module,
an `http` source listing every file at raw.githubusercontent.com at that commit with its SHA-256,
ready to be a spec's `demo` bucket. The commit must already contain the files, so changing the
data takes two commits: commit the regenerated files, push, then pin to that commit.

    uv run python scripts/synthetic_demo.py generate --demo-dir $DEMO   # rewrite data/synthetic_demo/
    uv run python scripts/synthetic_demo.py check --demo-dir $DEMO      # exit 1 if it is stale
    uv run python scripts/synthetic_demo.py pin --ref <commit>          # repoint sources.yaml
    uv run python scripts/synthetic_demo.py pin --check                 # exit 1 if a pin is stale

`$DEMO` is a download of the public MIMIC-IV demo (`meds-extract-download spec=MIMIC-IV key=demo
output_dir=$DEMO`); it defaults to `$MEDS_DEMO_DOWNLOAD_DIR`.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import random
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path

import polars as pl

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data" / "synthetic_demo"
SOURCES = DATA / "sources.yaml"

GENERATOR = "scripts/synthetic_demo.py"
GENERATOR_VERSION = "0.1"
SEED = 0

#: The MIMIC-IV demo release the synthetic rows are keyed to.
DEMO_RELEASE = "2.2"
GITHUB_REPO = "Medical-Event-Data-Standard/MIMIC_IV_MEDS"
RAW_URL = "https://raw.githubusercontent.com/{repo}/{ref}/{path}"

BANNER = "SYNTHETIC {kind} - NOT REAL PATIENT DATA - FOR PIPELINE TESTING ONLY"
TIME_FMT = "%Y-%m-%d %H:%M:%S"

Table = tuple[list[str], list[dict]]


# --------------------------------------------------------------------------------------------------
# Writing


def _fmt(v) -> str:
    """One CSV field as the releases write it: empty for null, `true`/`false` for booleans.

    Examples:
        >>> [_fmt(v) for v in (None, True, False, 3, 37.0, "x", datetime(2150, 1, 2, 3, 4, 5))]
        ['', 'true', 'false', '3', '37.0', 'x', '2150-01-02 03:04:05']
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, datetime):
        return v.strftime(TIME_FMT)
    return str(v)


def csv_text(table: Table, delimiter: str = ",") -> str:
    """Render a table as CSV text: a header, `\\n` line ends, minimal quoting.

    Examples:
        >>> print(csv_text((["a", "b"], [{"a": 1, "b": 'say "x, y" now'}, {"a": None}])), end="")
        a,b
        1,"say ""x, y"" now"
        ,
    """
    columns, rows = table
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=delimiter, lineterminator="\n")
    w.writerow(columns)
    for row in rows:
        unknown = set(row) - set(columns)
        if unknown:
            raise ValueError(f"unknown columns {sorted(unknown)}")
        w.writerow([_fmt(row.get(c)) for c in columns])
    return buf.getvalue()


def gzip_bytes(text: str) -> bytes:
    """Gzip `text` reproducibly: no file name and a zero mtime in the header."""
    buf = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=buf, mtime=0) as f:
        f.write(text.encode())
    return buf.getvalue()


def rng_for(seed: int, *key) -> random.Random:
    """A generator for one fixed key, so each entity's draws do not depend on any other's."""
    return random.Random("/".join(map(str, (seed, *key))))


def _pg_array(items: list[str]) -> str:
    """A Postgres array literal.

    Examples:
        >>> _pg_array([]), _pg_array(["Synthetic finding 1", "x"])
        ('{}', '{"Synthetic finding 1",x}')
    """
    return "{" + ",".join(f'"{s}"' if re.search(r'[\s,{}"]', s) else s for s in items) + "}"


def _json(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


# --------------------------------------------------------------------------------------------------
# Vocabularies (all public)

#: Discharge-summary section contents follow the closed enums of MIMIC-Note-Extract's public data
#: dictionary (https://physionet.org/content/mimic-iv-ext-note-extract/1.0/), so the simulated
#: summaries can be the source of a simulated MNE as well.
ENUMS = {
    "chief_complaint.category": ("FINDING", "CONTEXT"),
    "history.subject": ("PATIENT", "FAMILY"),
    "history.category": ("CONDITION", "INTERVENTION", "SUBSTANCE", "FUNCTIONAL_STATUS"),
    "history.temporal_status": ("CURRENT", "PAST"),
    "presentation": (
        "TABLET",
        "CAPSULE",
        "INHALER",
        "PATCH",
        "NEBULIZATION",
        "APPLICATION",
        "PUFF",
        "DROP",
        "DISK",
        "SPRAY",
        "OINTMENT",
        "CREAM",
        "GEL",
        "LOZENGE",
        "INJECTION",
        "SOLUTION",
        "[OTHER]",
    ),
    "measurement": ("MG", "MCG", "G", "ML", "MEQ", "PERCENT", "UNIT", "[OTHER]"),
    "route": (
        "ORAL",
        "SUBCUTANEOUS",
        "INHALATION",
        "TOPICAL",
        "OPHTHALMIC",
        "SUBLINGUAL",
        "NASAL",
        "INJECTION",
        "RECTAL",
        "TRANSDERMAL",
        "NEBULIZATION",
        "MUCOSAL",
        "[OTHER]",
    ),
    "period": ("MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "[OTHER]"),
    "time_of_day": (
        "BEDTIME",
        "BREAKFAST",
        "LUNCH",
        "DINNER",
        "MORNING",
        "AFTERNOON",
        "EVENING",
        "NIGHT",
        "[OTHER]",
        "[REDACTED]",
    ),
    "day_of_week": ("MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"),
    "timepoint": ("ADMISSION", "DISCHARGE", "INTERIM", "UNKNOWN"),
    "o2_support_method": (
        "ROOM_AIR",
        "NASAL_CANNULA",
        "HIGH_FLOW_NASAL_CANNULA",
        "SIMPLE_MASK",
        "NON_REBREATHER",
        "CPAP_BIPAP",
        "VENTILATOR",
        "AMBU_BAG",
    ),
    "system": ("GENERAL", "HEENT", "CV", "RESP", "GI", "EXTREM", "NEURO", "SKIN", "PSYCH", "GU", "ACCESS"),
    "status": ("NORMAL", "ABNORMAL", "NOT_ASSESSED"),
    "mental_status": ("CLEAR", "CONFUSED_SOMETIMES", "CONFUSED_ALWAYS"),
    "level_of_consciousness": ("ALERT", "LETHARGIC_AROUSABLE", "LETHARGIC_NOT_AROUSABLE"),
    "activity_status": ("INDEPENDENT", "WITH_ASSISTANCE", "CHAIR_ASSISTANCE", "BEDBOUND"),
    "general_condition": ("STABLE", "IMPROVED", "WORSENED", "GOOD", "POOR", "CRITICAL"),
    "pain_status": ("NO_PAIN", "MODERATE_PAIN", "SEVERE_PAIN"),
    "cardiovascular_status": ("STABLE", "UNSTABLE"),
    "vital_signs_status": ("STABLE", "UNSTABLE"),
    "fever_status": ("NO_FEVER", "FEVER_PRESENT"),
    "nutrition_status": ("ORAL_DIET", "TUBE_FEEDING", "PARENTERAL_NUTRITION", "NPO"),
    "wound_presence": ("NO_WOUNDS", "WOUNDS_PRESENT"),
    "wound_infection_status": ("NOT_INFECTED", "INFECTED"),
    "respiratory_status": ("ROOM_AIR", "OXYGEN_LOW_FLOW", "OXYGEN_HIGH_FLOW", "MECHANICAL_VENTILATION"),
    "orientation_status": ("ORIENTED_X3", "ORIENTED_X2", "ORIENTED_X1", "DISORIENTED"),
    "neurological_status": ("FOCAL", "NO_FOCAL"),
}

#: The discharge-condition aspects, in MNE's column order.
CONDITION_ASPECTS = (
    "mental_status",
    "level_of_consciousness",
    "activity_status",
    "general_condition",
    "pain_status",
    "cardiovascular_status",
    "vital_signs_status",
    "fever_status",
    "nutrition_status",
    "wound_presence",
    "wound_infection_status",
    "respiratory_status",
    "orientation_status",
    "neurological_status",
)

#: Statements of no known allergy.
NO_KNOWN_ALLERGY = ("No Known Allergies / Adverse Drug Reactions", "NKDA", "nka", "No allergies on file")
#: Common drug-allergy classes as (as written, standardized label): textbook pharmacology.
ALLERGENS = (
    ("Penicillins", "Penicillins"),
    ("Sulfa (Sulfonamide Antibiotics)", "Sulfonamides"),
    ("Codeine", "Codeine"),
    ("aspirin", "Aspirin"),
    ("Latex", "Latex"),
    ("Iodine", "Iodine"),
    ("Morphine", "Morphine"),
    ("Cephalosporins", "Cephalosporins"),
    ("ibuprofen", "Ibuprofen"),
)
#: Common generic drug names (WHO Model List of Essential Medicines).
DRUGS = (
    "Acetaminophen",
    "Amlodipine",
    "Atorvastatin",
    "Albuterol",
    "Aspirin",
    "Furosemide",
    "Heparin",
    "Insulin",
    "Levothyroxine",
    "Lisinopril",
    "Metformin",
    "Metoprolol",
    "Omeprazole",
    "Senna",
)
FAMILY = ("mother", "father", "sister", "brother", "son", "daughter", "grandmother", "grandfather")
WEIGHT_KG_PER_LB = 0.45359237

#: History concepts that carry an ontology id: SNOMED CT preferred terms, each id checked against
#: the public HL7 terminology server (tx.fhir.org).
HISTORY_CONCEPTS = (
    ("snomed:38341003", "Hypertensive disorder"),
    ("snomed:44054006", "Type 2 diabetes mellitus"),
    ("snomed:49436004", "Atrial fibrillation"),
    ("snomed:195967001", "Asthma"),
    ("snomed:13645005", "Chronic obstructive pulmonary disease"),
    ("snomed:84114007", "Heart failure"),
    ("snomed:709044004", "Chronic kidney disease"),
    ("snomed:55822004", "Hyperlipidemia"),
    ("snomed:53741008", "Coronary arteriosclerosis"),
    ("snomed:35489007", "Depressive disorder"),
    ("snomed:40930008", "Hypothyroidism"),
    ("snomed:235595009", "Gastroesophageal reflux disease"),
)
#: Discharge labs: LOINC codes (checked against the NLM LOINC API), unit and a plausible range.
DISCHARGE_LABS = (
    ("loinc:2160-0", "Creatinine", "mg/dL", 0.5, 2.5),
    ("loinc:2345-7", "Glucose", "mg/dL", 70, 220),
    ("loinc:718-7", "Hemoglobin", "g/dL", 8.0, 16.0),
    ("loinc:2823-3", "Potassium", "mEq/L", 3.2, 5.4),
    ("loinc:2951-2", "Sodium", "mEq/L", 130, 146),
)

#: The 14 CheXpert labels, as MIMIC-CXR-JPG's label columns are named (public CXR-JPG page), each
#: with the SNOMED CT concept a report mention of it is linked to, where there is one.
CHEXPERT = (
    ("Atelectasis", "snomed:46621007"),
    ("Cardiomegaly", "snomed:8186001"),
    ("Consolidation", "snomed:95436008"),
    ("Edema", "snomed:19242006"),
    ("Enlarged Cardiomediastinum", None),
    ("Fracture", "snomed:125605004"),
    ("Lung Lesion", None),
    ("Lung Opacity", None),
    ("Pleural Effusion", "snomed:60046008"),
    ("Pneumonia", "snomed:233604007"),
    ("Pneumothorax", "snomed:36118008"),
    ("Pleural Other", None),
    ("Support Devices", None),
    ("No Finding", None),
)
CHEXPERT_LABELS = tuple(label for label, _ in CHEXPERT)

#: Radiology exams: (exam name, CPT code, modality). The chest radiograph names are the two the
#: MIMIC-CXR-JPG page quotes; the CPT codes are the public procedure codes for each exam.
CHEST_PA_LAT = ("CHEST (PA AND LAT)", "71046", "CXR")
CHEST_PORTABLE = ("CHEST (PORTABLE AP)", "71045", "CXR")
OTHER_EXAMS = (
    ("CT HEAD W/O CONTRAST", "70450", "CT"),
    ("CT ABD & PELVIS WITH CONTRAST", "74177", "CT"),
    ("LIVER OR GALLBLADDER US (SINGLE ORGAN)", "76705", "US"),
    ("MR HEAD W & W/O CONTRAST", "70553", "MR"),
)
#: The hospital's internal exam codes are not documented publicly; these are made up, `SYN` + number.
EXAM_CODES = {
    name: f"SYN{i:02d}" for i, (name, _, _) in enumerate((CHEST_PA_LAT, CHEST_PORTABLE, *OTHER_EXAMS), 1)
}
#: View positions and their DICOM code meanings, as the CXR-JPG page describes them.
VIEWS = {
    "PA": "postero-anterior",
    "AP": "antero-posterior",
    "LATERAL": "lateral",
    "LL": "left lateral",
}


# --------------------------------------------------------------------------------------------------
# The public demo


@dataclass
class Demo:
    """The public MIMIC-IV demo tables the simulation is keyed to."""

    admissions: list[dict]
    patients: dict[int, dict]
    icustays: dict[int, list[dict]]  #: by hadm_id
    providers: list[str]

    @classmethod
    def load(cls, demo_dir: Path) -> Demo:
        def times(df: pl.DataFrame, *cols: str) -> pl.DataFrame:
            return df.with_columns(pl.col(c).str.strptime(pl.Datetime, TIME_FMT) for c in cols)

        adm = times(
            pl.read_csv(demo_dir / "hosp" / "admissions.csv.gz", infer_schema_length=None),
            "admittime",
            "dischtime",
            "edregtime",
            "edouttime",
        )
        icu = times(pl.read_csv(demo_dir / "icu" / "icustays.csv.gz"), "intime", "outtime")
        pts = pl.read_csv(demo_dir / "hosp" / "patients.csv.gz")
        providers = pl.read_csv(demo_dir / "hosp" / "provider.csv.gz")["provider_id"].sort().to_list()
        stays: dict[int, list[dict]] = {}
        for s in icu.sort("intime").to_dicts():
            stays.setdefault(s["hadm_id"], []).append(s)
        return cls(
            admissions=adm.filter(pl.col("dischtime").is_not_null()).sort("hadm_id").to_dicts(),
            patients={p["subject_id"]: p for p in pts.to_dicts()},
            icustays=stays,
            providers=providers,
        )

    @cached_property
    def by_subject(self) -> dict[int, list[dict]]:
        out: dict[int, list[dict]] = {}
        for a in sorted(self.admissions, key=lambda a: a["admittime"]):
            out.setdefault(a["subject_id"], []).append(a)
        return out


# --------------------------------------------------------------------------------------------------
# The simulated world: one discharge summary per admission, radiology exams, CXR studies, echoes


def _medication(rng: random.Random, seq_num: int) -> dict:
    e = ENUMS
    redacted = rng.random() < 0.08
    med = {
        "seq_num": seq_num,
        "name": rng.choice(DRUGS),
        "presentation_form": rng.choice(e["presentation"]) if rng.random() < 0.9 else None,
        "dose_value": None if redacted else float(rng.choice((0.5, 1, 2, 5, 10, 20, 25, 40, 81, 100, 500))),
        "dose_unit": rng.choice(e["measurement"] + e["presentation"]),
        "dose_redacted": redacted,
        "route": rng.choice(e["route"]) if rng.random() < 0.9 else None,
        "duration_value": None,
        "duration_unit": None,
        "prn": rng.random() < 0.2,
        "prn_condition": None,
        "hold": rng.random() < 0.1,
        "hold_condition": None,
        "strength": None,
        "frequency": None,
    }
    if rng.random() < 0.2:
        med |= {"duration_value": float(rng.randint(1, 14)), "duration_unit": rng.choice(e["period"])}
    if med["prn"] and rng.random() < 0.8:
        med["prn_condition"] = f"Synthetic PRN indication {rng.randint(1, 5)}"
    if med["hold"] and rng.random() < 0.8:
        med["hold_condition"] = f"Synthetic hold condition {rng.randint(1, 5)}"
    kind = rng.choice(("SINGLE", "RANGE", "COMBINATION", None))
    if kind == "SINGLE":
        med["strength"] = {
            "type": "SINGLE",
            "numerator_values": [float(rng.choice((5, 10, 20, 50)))],
            "numerator_units": ["MG"],
            "denominator_value": 1.0,
            "denominator_unit": "TABLET",
        }
    elif kind == "RANGE":
        lo = float(rng.choice((1, 2, 5)))
        med["strength"] = {
            "type": "RANGE",
            "numerator_values": [lo, lo * 2],
            "numerator_units": ["MG", "MG"],
            "denominator_value": None,
            "denominator_unit": None,
        }
    elif kind == "COMBINATION":
        med["strength"] = {
            "type": "COMBINATION",
            "numerator_values": [5.0, "[REDACTED]"],
            "numerator_units": ["MG", "MG"],
            "denominator_value": 1.0,
            "denominator_unit": rng.choice(e["presentation"]),
        }
    freq = rng.choice(("times_per_period", "every_period", "specific_time", "specific_day", "[OTHER]", None))
    if freq == "times_per_period":
        med["frequency"] = {"type": freq, "times": rng.randint(1, 4), "period_unit": rng.choice(e["period"])}
    elif freq == "every_period":
        med["frequency"] = {"type": freq, "value": rng.choice((4, 6, 8, 12)), "unit": "HOUR"}
    elif freq == "specific_time":
        med["frequency"] = {"type": freq, "time": rng.choice(e["time_of_day"])}
    elif freq == "specific_day":
        med["frequency"] = {"type": freq, "days": sorted(rng.sample(e["day_of_week"], 2))}
    elif freq == "[OTHER]":
        med["frequency"] = {"type": freq}
    return med


def _vitals(rng: random.Random, timepoint: str) -> dict:
    """One physical-exam vitals block; a value is either present, redacted (`___`) or absent."""
    v: dict = {"timepoint": timepoint, "warnings": []}
    for vital in ("temperature", "heart_rate", "bp_systolic", "bp_diastolic"):
        v[f"{vital}_redacted"] = False
    r = rng.random()
    if r < 0.1:
        v["temperature_redacted"] = True
    elif r < 0.9:
        c = round(rng.uniform(36.0, 38.8), 1)
        v |= {"temperature_c": c, "temperature_f": round(c * 9 / 5 + 32, 1)}
    r = rng.random()
    if r < 0.05:
        v["heart_rate_redacted"] = True
    elif r < 0.95:
        v["heart_rate"] = rng.randint(50, 125)
    for vital, lo, hi in (("bp_systolic", 90, 175), ("bp_diastolic", 45, 105)):
        r = rng.random()
        if r < 0.05:
            v[f"{vital}_redacted"] = True
        elif r < 0.92:
            v[vital] = rng.randint(lo, hi)
    if rng.random() < 0.9:
        v["respiratory_rate"] = rng.randint(10, 28)
    if rng.random() < 0.9:
        v["o2_saturation"] = rng.randint(88, 100)
    if rng.random() < 0.6:
        kg = round(rng.uniform(45.0, 130.0), 1)
        unit = rng.choice(("kg", "kg", "lbs", None))
        v |= {"weight_value": round(kg / WEIGHT_KG_PER_LB, 1) if unit == "lbs" else kg, "weight_unit": unit}
    if rng.random() < 0.7:
        method = rng.choice(ENUMS["o2_support_method"])
        v["o2_support_method"] = method
        lpm = {
            "NASAL_CANNULA": (1, 6),
            "HIGH_FLOW_NASAL_CANNULA": (20, 60),
            "SIMPLE_MASK": (5, 10),
            "NON_REBREATHER": (10, 15),
        }.get(method)
        if lpm and rng.random() < 0.8:
            v["o2_support_lpm"] = float(rng.randint(*lpm))
    if rng.random() < 0.03:
        v["warnings"] = ["Synthetic outlier warning"]
    return v


def discharge_summary(seed: int, adm: dict, first: bool) -> dict:
    """The structured content of one admission's discharge summary; `render_discharge` writes it out.

    `first` marks the first admission (by `hadm_id`), which carries the edge cases a random draw
    might miss: a standardized allergen whose text reads like a no-known-allergies statement, and
    one history entry written twice.
    """
    e = ENUMS
    rng = rng_for(seed, "discharge", adm["hadm_id"])
    s: dict = {}

    if first:
        s["allergies"] = [("Penicillins", "Penicillins"), ("Codeine (no known reaction)", "Codeine")]
    elif rng.random() < 0.35:
        s["allergies"] = [(rng.choice(NO_KNOWN_ALLERGY), None)]
    else:
        s["allergies"] = [
            (f"Synthetic allergen {rng.randint(1, 9)}", None) if rng.random() < 0.15 else pair
            for pair in rng.sample(ALLERGENS, rng.randint(1, 3))
        ]

    s["chief_complaint"] = [
        ("FINDING", f"Synthetic chief complaint {rng.randint(1, 20)}") for _ in range(rng.randint(1, 2))
    ]
    if rng.random() < 0.4:
        s["chief_complaint"].append(("CONTEXT", f"Synthetic presentation context {rng.randint(1, 5)}"))

    history = []
    for _ in range(rng.randint(1, 6)):
        family = rng.random() < 0.15
        concept = rng.choice(HISTORY_CONCEPTS) if rng.random() < 0.6 else None
        history.append(
            {
                "subject": "FAMILY" if family else "PATIENT",
                "family_relation": rng.choice(FAMILY) if family else None,
                "concept_id": concept[0] if concept else None,
                "concept_name": concept[1] if concept else f"Synthetic history item {rng.randint(1, 30)}",
                "category": "CONDITION" if concept else rng.choice(e["history.category"]),
                "temporal_status": rng.choice((*e["history.temporal_status"], None)),
            }
        )
    if first:
        history.insert(1, history[0])
    s["history"] = history

    s["medications_on_admission"] = [_medication(rng, i) for i in range(1, rng.randint(0, 6) + 1)]
    s["discharge_medications"] = [_medication(rng, i) for i in range(1, rng.randint(0, 6) + 1)]
    s["diagnoses"] = [
        (i == 1 or (i == 2 and rng.random() < 0.2), f"Synthetic discharge diagnosis {rng.randint(1, 40)}")
        for i in range(1, rng.randint(1, 5) + 1)
    ]
    s["procedures"] = [
        (rng.random() < 0.3, f"Synthetic procedure {rng.randint(1, 15)}")
        for _ in range(rng.choice((0, 0, 1, 1, 2)))
    ]

    p = {"ADMISSION": 0.7, "INTERIM": 0.1, "DISCHARGE": 0.8, "UNKNOWN": 0.1}
    s["exam"] = []
    for tp in [tp for tp in p if rng.random() < p[tp]] or ["UNKNOWN"]:
        systems = []
        for system in e["system"]:
            status = rng.choice(e["status"])
            findings = (
                [f"Synthetic finding {rng.randint(1, 12)}" for _ in range(rng.randint(1, 2))]
                if status == "ABNORMAL"
                else []
            )
            systems.append((system, status, findings))
        s["exam"].append((_vitals(rng, tp), systems))

    s["labs"] = [
        (cid, name, unit, round(rng.uniform(lo, hi), 1))
        for cid, name, unit, lo, hi in rng.sample(DISCHARGE_LABS, rng.randint(0, 3))
    ]
    s["condition"] = {k: rng.choice(e[k]) if rng.random() < 0.7 else None for k in CONDITION_ASPECTS}
    s["hpi_sentences"] = rng.randint(1, 3)
    s["course_sentences"] = rng.randint(1, 4)
    return s


def _words(enum_value: str) -> str:
    return enum_value.replace("_", " ").lower()


def _med_line(m: dict) -> str:
    parts = [m["name"]]
    if m["dose_redacted"]:
        parts.append("___")
    elif m["dose_value"] is not None:
        parts.append(f"{m['dose_value']:g}")
    parts.append(m["dose_unit"])
    if m["route"]:
        parts.append(m["route"])
    if m["presentation_form"]:
        parts.append(f"({m['presentation_form']})")
    if m["frequency"]:
        parts.append(_json(m["frequency"]))
    if m["duration_value"] is not None:
        parts.append(f"for {m['duration_value']:g} {m['duration_unit']}")
    if m["prn"]:
        parts.append(f"PRN {m['prn_condition'] or ''}".rstrip())
    if m["hold"]:
        parts.append(f"HOLD {m['hold_condition'] or ''}".rstrip())
    return " ".join(parts)


def _vitals_line(v: dict) -> str:
    def val(key: str, fmt=str) -> str:
        if v.get(f"{key}_redacted"):
            return "___"
        return fmt(v[key]) if v.get(key) is not None else "not recorded"

    if v["temperature_redacted"]:
        temp = "___"
    elif v.get("temperature_c") is not None:
        temp = f"{v['temperature_f']}F ({v['temperature_c']}C)"
    else:
        temp = "not recorded"
    o2 = "not recorded"
    if v.get("o2_support_method"):
        o2 = _words(v["o2_support_method"])
        if v.get("o2_support_lpm"):
            o2 += f" {v['o2_support_lpm']:g} L/min"
    weight = "not recorded"
    if v.get("weight_value") is not None:
        weight = f"{v['weight_value']:g} {v.get('weight_unit') or '(no unit)'}"
    return (
        f"VS: T {temp}, HR {val('heart_rate')}, BP {val('bp_systolic')}/{val('bp_diastolic')}, "
        f"RR {v.get('respiratory_rate') or 'not recorded'}, "
        f"O2 sat {v.get('o2_saturation') or 'not recorded'}% "
        f"on {o2}, weight {weight}"
    )


def render_discharge(s: dict, sex: str) -> str:
    """The discharge summary text: section headers with templated, unmistakably synthetic content."""
    out = [BANNER.format(kind="DISCHARGE SUMMARY"), "", f"Sex: {sex}", ""]

    def section(header: str, lines: list[str]) -> None:
        out.extend([f"{header}:", *(lines or ["None"]), ""])

    section("Allergies", [title for title, _ in s["allergies"]])
    section("Chief Complaint", [text for _, text in s["chief_complaint"]])
    section(
        "Major Surgical or Invasive Procedure",
        [f"{title}{' (previous admission)' if prev else ''}" for prev, title in s["procedures"]],
    )
    section(
        "History of Present Illness",
        [f"Synthetic history of present illness sentence {i}." for i in range(1, s["hpi_sentences"] + 1)],
    )
    history = []
    for h in s["history"]:
        status = f" ({h['temporal_status'].lower()})" if h["temporal_status"] else ""
        who = f"Family history, {h['family_relation']}: " if h["subject"] == "FAMILY" else ""
        history.append(f"- {who}{h['concept_name']}{status}")
    section("Past Medical History", history)
    exam = []
    for vitals, systems in s["exam"]:
        exam += [f"{vitals['timepoint']} EXAM:", _vitals_line(vitals)]
        for system, status, findings in systems:
            if status == "ABNORMAL":
                exam.append(f"{system}: {', '.join(findings)}")
            elif status == "NORMAL":
                exam.append(f"{system}: Synthetic normal examination.")
        if vitals["warnings"]:
            exam.append(f"(warnings: {', '.join(vitals['warnings'])})")
    section("Physical Exam", exam)
    section(
        "Pertinent Results",
        [f"DISCHARGE LABS: {name} {value:g} {unit}" for _, name, unit, value in s["labs"]],
    )
    section(
        "Brief Hospital Course",
        [f"Synthetic hospital course sentence {i}." for i in range(1, s["course_sentences"] + 1)],
    )
    section(
        "Medications on Admission", [f"{m['seq_num']}. {_med_line(m)}" for m in s["medications_on_admission"]]
    )
    section("Discharge Medications", [f"{m['seq_num']}. {_med_line(m)}" for m in s["discharge_medications"]])
    section(
        "Discharge Diagnosis",
        [f"{'Primary' if primary else 'Secondary'}: {title}" for primary, title in s["diagnoses"]],
    )
    section(
        "Discharge Condition",
        [f"{_words(k).capitalize()}: {_words(v)}." for k, v in s["condition"].items() if v],
    )
    section("Discharge Instructions", ["Synthetic discharge instructions."])
    out.append("Followup Instructions:\n___")
    return "\n".join(out) + "\n"


@dataclass
class Exam:
    """One radiology exam: a MIMIC-IV-Note radiology report, and for a chest X-ray a MIMIC-CXR study."""

    subject_id: int
    hadm_id: int | None
    time: datetime  #: when the exam was performed (and the report charted)
    exam: tuple[str, str, str]  #: name, CPT code, modality
    setting: str  #: ED, ICU, WARD or OUTSIDE (no stay)
    labels: dict[str, float | None] = field(default_factory=dict)  #: CheXpert labels (CXR only)
    sections: bool = True  #: whether the report has FINDINGS/IMPRESSION sections
    addendum: bool = False
    note_id: str = ""
    study_id: int | None = None
    images: list[dict] = field(default_factory=list)

    @property
    def is_cxr(self) -> bool:
        return self.exam[2] == "CXR"


def _chexpert_labels(rng: random.Random) -> dict[str, float | None]:
    """CheXpert-style labels: 1.0 present, 0.0 absent, -1.0 uncertain, None not mentioned."""
    labels: dict[str, float | None] = {}
    for label in CHEXPERT_LABELS[:-1]:
        r = rng.random()
        labels[label] = 1.0 if r < 0.15 else 0.0 if r < 0.3 else -1.0 if r < 0.37 else None
    descriptive = [labels[x] for x in CHEXPERT_LABELS[:-2]]  # Support Devices does not count
    labels["No Finding"] = 1.0 if all(v in (None, 0.0) for v in descriptive) else None
    return labels


def render_radiology(exam: Exam) -> str:
    """The radiology report text; for a chest X-ray, its findings are its CheXpert labels."""
    name = exam.exam[0]
    out = [
        BANNER.format(kind="RADIOLOGY REPORT"),
        "",
        f"EXAMINATION: {name}",
        "",
        f"INDICATION: ___ Synthetic indication for {exam.setting.lower()} imaging.",
        "",
        "TECHNIQUE: Synthetic technique description.",
        "",
        "COMPARISON: ___",
        "",
    ]
    if not exam.sections:
        out.append("Synthetic report without findings or impression sections.")
        return "\n".join(out) + "\n"
    findings, impression = [], []
    if exam.is_cxr:
        for label in CHEXPERT_LABELS[:-1]:
            v = exam.labels[label]
            if v == 1.0:
                findings.append(f"Synthetic finding: {label.lower()} is present.")
                impression.append(label.lower())
            elif v == 0.0:
                findings.append(f"No {label.lower()}.")
            elif v == -1.0:
                findings.append(f"Possible {label.lower()}, cannot be excluded.")
        if exam.labels["No Finding"] == 1.0:
            impression.append("no acute cardiopulmonary process")
    else:
        findings.append(f"Synthetic {exam.exam[2]} findings.")
    out += [
        "FINDINGS:",
        *(findings or ["Synthetic unremarkable study."]),
        "",
        "IMPRESSION:",
        f"Synthetic impression: {', '.join(impression) or 'see findings'}.",
    ]
    return "\n".join(out) + "\n"


def _new_id(rng: random.Random, used: set, lo: int, hi: int) -> int:
    while (i := rng.randint(lo, hi)) in used:
        pass
    used.add(i)
    return i


def _dicom_id(rng: random.Random) -> str:
    """A 40-hex-character id in five dash-separated groups of eight, as MIMIC-CXR's dicom_id."""
    return "-".join(f"{rng.getrandbits(32):08x}" for _ in range(5))


def radiology_exams(seed: int, demo: Demo) -> list[Exam]:
    """Radiology exams during ED visits, ward and ICU time, and outside any stay, per subject."""
    exams: list[Exam] = []
    for subject_id, adms in sorted(demo.by_subject.items()):
        rng = rng_for(seed, "radiology", subject_id)
        for adm in adms:
            if adm["edregtime"] is not None and adm["edouttime"] is not None and rng.random() < 0.4:
                t = adm["edregtime"] + (adm["edouttime"] - adm["edregtime"]) * rng.uniform(0.1, 0.9)
                exams.append(Exam(subject_id, None, t, rng.choice((CHEST_PA_LAT, CHEST_PORTABLE)), "ED"))
            for stay in demo.icustays.get(adm["hadm_id"], []):
                for _ in range(rng.choice((0, 1, 1, 2))):
                    t = stay["intime"] + (stay["outtime"] - stay["intime"]) * rng.uniform(0.05, 0.95)
                    exams.append(Exam(subject_id, adm["hadm_id"], t, CHEST_PORTABLE, "ICU"))
            for _ in range(rng.choice((0, 1, 1, 2))):
                t = adm["admittime"] + (adm["dischtime"] - adm["admittime"]) * rng.uniform(0.02, 0.98)
                kind = rng.choice((CHEST_PA_LAT, CHEST_PA_LAT, *OTHER_EXAMS))
                exams.append(Exam(subject_id, adm["hadm_id"], t, kind, "WARD"))
        if rng.random() < 0.3:  # an outpatient exam, weeks before the first admission
            t = adms[0]["admittime"] - timedelta(days=rng.randint(14, 120), minutes=rng.randint(0, 1440))
            exams.append(Exam(subject_id, None, t, CHEST_PA_LAT, "OUTSIDE"))
    exams.sort(key=lambda x: (x.subject_id, x.time))

    # Report ids, CXR studies and images; ids are drawn per subject in time order.
    used_studies: set[int] = set()
    for subject_id in sorted(demo.by_subject):
        rng = rng_for(seed, "radiology-ids", subject_id)
        mine = [x for x in exams if x.subject_id == subject_id]
        for seq, x in enumerate(mine, start=1):
            x.time = x.time.replace(microsecond=0)
            x.note_id = f"{subject_id}-RR-{seq}"
            x.addendum = rng.random() < 0.05
            if not x.is_cxr:
                continue
            x.labels = _chexpert_labels(rng)
            x.sections = rng.random() > 0.02
            x.study_id = _new_id(rng, used_studies, 50_000_000, 59_999_999)
            views = ("PA", "LATERAL") if x.exam is CHEST_PA_LAT else ("AP",)
            if x.exam is CHEST_PA_LAT and rng.random() < 0.15:
                views = ("PA", "LATERAL", "LL")
            for i, view in enumerate(views):
                t = x.time + timedelta(
                    minutes=2 * i, seconds=rng.randint(0, 59), milliseconds=rng.randint(0, 999)
                )
                missing_view = rng.random() < 0.05
                x.images.append(
                    {
                        "dicom_id": _dicom_id(rng),
                        "time": t,
                        "ViewPosition": None if missing_view else view,
                        "ViewCodeSequence_CodeMeaning": None if missing_view else VIEWS[view],
                        "PatientOrientationCodeSequence_CodeMeaning": rng.choice(
                            ("Erect", "Erect", "Recumbent", None)
                            if x.exam is CHEST_PA_LAT
                            else ("Recumbent", "Erect", None)
                        ),
                        "PerformedProcedureStepDescription": x.exam[0] if rng.random() < 0.9 else None,
                        "Rows": rng.choice((2544, 3056, 2022, 2320)),
                        "Columns": rng.choice((3056, 2544, 2022, 2828)),
                    }
                )
    return exams


def echo_studies(seed: int, demo: Demo) -> list[dict]:
    """Echocardiography studies: during some admissions, and some outside any stay."""
    studies, used = [], set()
    for subject_id, adms in sorted(demo.by_subject.items()):
        rng = rng_for(seed, "echo", subject_id)
        times = [
            adm["admittime"] + (adm["dischtime"] - adm["admittime"]) * rng.uniform(0.1, 0.9)
            for adm in adms
            if rng.random() < 0.15
        ]
        if rng.random() < 0.1:
            times.append(
                adms[-1]["dischtime"] + timedelta(days=rng.randint(20, 200), hours=rng.randint(0, 23))
            )
        for t in sorted(times):
            study_id = _new_id(rng, used, 90_000_000, 99_999_999)
            t = t.replace(microsecond=0)
            views, view = [], 0
            for _ in range(rng.randint(5, 12)):
                view += rng.randint(1, 4)
                views.append((view, t + timedelta(seconds=40 * len(views) + rng.randint(0, 30))))
            studies.append({"subject_id": subject_id, "study_id": study_id, "time": t, "views": views})
    return studies


# --------------------------------------------------------------------------------------------------
# Writers: one function per module, returning {path in the release: bytes}


def _discharge_notes(seed: int, demo: Demo) -> list[dict]:
    """One DS note per admission (plus the odd AD addendum), with its structured content."""
    notes = []
    first = demo.admissions[0]["hadm_id"]
    for subject_id, adms in sorted(demo.by_subject.items()):
        seq = {"DS": 0, "AD": 0}
        for adm in adms:  # in time order, so note_seq sorts chronologically
            rng = rng_for(seed, "discharge-times", adm["hadm_id"])
            content = discharge_summary(seed, adm, adm["hadm_id"] == first)
            sex = demo.patients[subject_id]["gender"]
            seq["DS"] += 1
            charttime = adm["dischtime"]
            note = {
                "note_id": f"{subject_id}-DS-{seq['DS']}",
                "subject_id": subject_id,
                "hadm_id": adm["hadm_id"],
                "note_type": "DS",
                "note_seq": seq["DS"],
                "charttime": charttime,
                "storetime": charttime + timedelta(minutes=rng.randint(30, 48 * 60)),
                "text": render_discharge(content, sex),
                "content": content,
                "author": rng.random() < 0.9,
            }
            notes.append(note)
            if rng.random() < 0.05:
                seq["AD"] += 1
                notes.append(
                    {
                        **note,
                        "note_id": f"{subject_id}-AD-{seq['AD']}",
                        "note_type": "AD",
                        "note_seq": seq["AD"],
                        "storetime": note["storetime"] + timedelta(hours=rng.randint(2, 72)),
                        "text": BANNER.format(kind="DISCHARGE SUMMARY ADDENDUM")
                        + "\n\nSynthetic addendum text.\n",
                        "content": None,
                    }
                )
    return notes


NOTE_COLUMNS = ["note_id", "subject_id", "hadm_id", "note_type", "note_seq", "charttime", "storetime", "text"]
DETAIL_COLUMNS = ["note_id", "subject_id", "field_name", "field_value", "field_ordinal"]


def note_files(seed: int, demo: Demo) -> dict[str, bytes]:
    """MIMIC-IV-Note 2.2: `note/{discharge,discharge_detail,radiology,radiology_detail}.csv.gz`.

    Columns and types: mimic-code's `mimic-iv-note/buildmimic/postgres/create.sql` and the
    mimic.mit.edu Note module pages; comma-separated, gzipped, header row (its `load_gz.sql`).
    """
    discharge, discharge_detail = [], []
    for n in _discharge_notes(seed, demo):
        discharge.append({c: n[c] for c in NOTE_COLUMNS})
        if n["note_type"] == "DS" and n["author"]:
            discharge_detail.append(
                {
                    "note_id": n["note_id"],
                    "subject_id": n["subject_id"],
                    "field_name": "author",
                    "field_value": "___",
                    "field_ordinal": 1,
                }
            )

    radiology, radiology_detail = [], []
    exams = radiology_exams(seed, demo)
    ar_seq: dict[int, int] = {}
    for x in exams:
        rng = rng_for(seed, "radiology-note", x.note_id)
        storetime = x.time + timedelta(minutes=rng.randint(20, 6 * 60))
        radiology.append(
            {
                "note_id": x.note_id,
                "subject_id": x.subject_id,
                "hadm_id": x.hadm_id,
                "note_type": "RR",
                "note_seq": int(x.note_id.rsplit("-", 1)[1]),
                "charttime": x.time,
                "storetime": storetime,
                "text": render_radiology(x),
            }
        )

        def detail(
            note_id: str, name: str, value: str, ordinal: int = 1, subject_id: int = x.subject_id
        ) -> None:
            radiology_detail.append(
                {
                    "note_id": note_id,
                    "subject_id": subject_id,
                    "field_name": name,
                    "field_value": value,
                    "field_ordinal": ordinal,
                }
            )

        detail(x.note_id, "exam_code", EXAM_CODES[x.exam[0]])
        detail(x.note_id, "exam_name", x.exam[0])
        detail(x.note_id, "cpt_code", x.exam[1])
        if x.addendum:
            ar_seq[x.subject_id] = ar_seq.get(x.subject_id, 0) + 1
            ad_id = f"{x.subject_id}-AR-{ar_seq[x.subject_id]}"
            radiology.append(
                {
                    "note_id": ad_id,
                    "subject_id": x.subject_id,
                    "hadm_id": x.hadm_id,
                    "note_type": "AR",
                    "note_seq": ar_seq[x.subject_id],
                    "charttime": x.time,
                    "storetime": storetime + timedelta(hours=rng.randint(1, 24)),
                    "text": f"{BANNER.format(kind='RADIOLOGY REPORT ADDENDUM')}\n\n"
                    f"Synthetic addendum to {x.exam[0]}.\n",
                }
            )
            detail(x.note_id, "addendum_note_id", ad_id)
            detail(ad_id, "parent_note_id", x.note_id)
            detail(ad_id, "exam_name", x.exam[0])
    radiology.sort(key=lambda r: (r["subject_id"], r["note_type"], r["note_seq"]))
    return {
        "note/discharge.csv.gz": gzip_bytes(csv_text((NOTE_COLUMNS, discharge))),
        "note/discharge_detail.csv.gz": gzip_bytes(csv_text((DETAIL_COLUMNS, discharge_detail))),
        "note/radiology.csv.gz": gzip_bytes(csv_text((NOTE_COLUMNS, radiology))),
        "note/radiology_detail.csv.gz": gzip_bytes(csv_text((DETAIL_COLUMNS, radiology_detail))),
    }


def _cxr_studies(seed: int, demo: Demo) -> list[Exam]:
    return [x for x in radiology_exams(seed, demo) if x.is_cxr]


def _study_dir(subject_id: int, study_id: int) -> str:
    return f"files/p{str(subject_id)[:2]}/p{subject_id}/s{study_id}"


def cxr_files(seed: int, demo: Demo) -> dict[str, bytes]:
    """MIMIC-CXR 2.1.0: the record, study and provider lists and one report `.txt` per study.

    Layout and columns: the MIMIC-CXR 2.1.0 PhysioNet page and the mimic.mit.edu CXR module pages
    (`cxr-record-list`: subject_id, study_id, dicom_id; `cxr-study-list`: subject_id, study_id;
    `cxr-provider-list`: study_id, ordering, attending, provider). The report text is the
    simulated MIMIC-IV-Note radiology report of the same exam.
    """
    studies = _cxr_studies(seed, demo)
    records, study_list, providers, files = [], [], [], {}
    for x in sorted(studies, key=lambda x: (x.subject_id, x.study_id)):
        rng = rng_for(seed, "cxr-provider", x.study_id)
        study_list.append({"subject_id": x.subject_id, "study_id": x.study_id})
        records += [
            {"subject_id": x.subject_id, "study_id": x.study_id, "dicom_id": im["dicom_id"]}
            for im in x.images
        ]
        providers.append(
            {
                "study_id": x.study_id,
                "ordering": rng.choice(demo.providers),
                "attending": rng.choice(demo.providers),
                "provider": rng.choice(demo.providers) if rng.random() < 0.4 else None,
            }
        )
        files[f"{_study_dir(x.subject_id, x.study_id)}.txt"] = render_radiology(x).encode()
    return {
        "cxr-record-list.csv.gz": gzip_bytes(csv_text((["subject_id", "study_id", "dicom_id"], records))),
        "cxr-study-list.csv.gz": gzip_bytes(csv_text((["subject_id", "study_id"], study_list))),
        "cxr-provider-list.csv.gz": gzip_bytes(
            csv_text((["study_id", "ordering", "attending", "provider"], providers))
        ),
        **files,
    }


#: `mimic-cxr-2.0.0-metadata.csv.gz` columns: `dicom_id`, `subject_id`, `study_id` (the order the
#: public notebook `dcm/create-mimic-cxr-jpg-metadata.ipynb` in MIT-LCP/mimic-cxr writes), then the
#: DICOM-derived columns the CXR-JPG page documents, in its order.
CXR_METADATA_COLUMNS = [
    "dicom_id",
    "subject_id",
    "study_id",
    "PerformedProcedureStepDescription",
    "ViewPosition",
    "Rows",
    "Columns",
    "StudyDate",
    "StudyTime",
    "ProcedureCodeSequence_CodeMeaning",
    "ViewCodeSequence_CodeMeaning",
    "PatientOrientationCodeSequence_CodeMeaning",
]


def _label(v: float | None) -> str | None:
    return None if v is None else f"{v:.1f}"


def _split(seed: int, subject_id: int) -> str:
    r = rng_for(seed, "cxr-split", subject_id).random()
    return "test" if r < 0.1 else "validate" if r < 0.2 else "train"


def cxr_jpg_files(seed: int, demo: Demo) -> dict[str, bytes]:
    """MIMIC-CXR-JPG 2.1.0: metadata, labels, split, test-set labels and `IMAGE_FILENAMES`.

    Columns as the MIMIC-CXR-JPG 2.1.0 PhysioNet page documents them (label columns in its order;
    the metadata's `subject_id`/`study_id` as the public metadata notebook writes them). No images.
    """
    studies = sorted(_cxr_studies(seed, demo), key=lambda x: (x.subject_id, x.study_id))
    metadata, split, chexpert, test_set, filenames = [], [], [], [], []
    for x in studies:
        for im in x.images:
            metadata.append(
                {
                    "dicom_id": im["dicom_id"],
                    "subject_id": x.subject_id,
                    "study_id": x.study_id,
                    "PerformedProcedureStepDescription": im["PerformedProcedureStepDescription"],
                    "ViewPosition": im["ViewPosition"],
                    "Rows": im["Rows"],
                    "Columns": im["Columns"],
                    # All images of a study share its date and time (the CXR-JPG page).
                    "StudyDate": x.images[0]["time"].strftime("%Y%m%d"),
                    "StudyTime": float(x.images[0]["time"].strftime("%H%M%S.%f")[:-3]),
                    "ProcedureCodeSequence_CodeMeaning": x.exam[0],
                    "ViewCodeSequence_CodeMeaning": im["ViewCodeSequence_CodeMeaning"],
                    "PatientOrientationCodeSequence_CodeMeaning": im[
                        "PatientOrientationCodeSequence_CodeMeaning"
                    ],
                }
            )
            split.append(
                {
                    "dicom_id": im["dicom_id"],
                    "study_id": x.study_id,
                    "subject_id": x.subject_id,
                    "split": _split(seed, x.subject_id),
                }
            )
            filenames.append(f"{_study_dir(x.subject_id, x.study_id)}/{im['dicom_id']}.jpg")
        if not x.sections:  # an unlabelable report (no findings or impression): in no label file
            continue
        labels = {k: _label(v) for k, v in x.labels.items()}
        chexpert.append({"subject_id": x.subject_id, "study_id": x.study_id, **labels})
        if _split(seed, x.subject_id) == "test":
            test_set.append({"study_id": x.study_id, **labels})
    label_cols = ["subject_id", "study_id", *CHEXPERT_LABELS]
    return {
        "mimic-cxr-2.0.0-metadata.csv.gz": gzip_bytes(csv_text((CXR_METADATA_COLUMNS, metadata))),
        "mimic-cxr-2.0.0-split.csv.gz": gzip_bytes(
            csv_text((["dicom_id", "study_id", "subject_id", "split"], split))
        ),
        "mimic-cxr-2.0.0-chexpert.csv.gz": gzip_bytes(csv_text((label_cols, chexpert))),
        # NegBio labels every study CheXpert labels; here both read the same simulated report.
        "mimic-cxr-2.0.0-negbio.csv.gz": gzip_bytes(csv_text((label_cols, chexpert))),
        "mimic-cxr-2.1.0-test-set-labeled.csv": csv_text((["study_id", *CHEXPERT_LABELS], test_set)).encode(),
        "IMAGE_FILENAMES": ("\n".join(filenames) + "\n").encode(),
    }


def echo_files(seed: int, demo: Demo) -> dict[str, bytes]:
    """MIMIC-IV-ECHO 0.1: `echo-record-list.csv` and `echo-study-list.csv` (no DICOMs).

    Columns as the MIMIC-IV-ECHO 0.1 PhysioNet page names them; it does not give their order. The cardiologist
    reports they link to are not in MIMIC-IV-Note 2.2, so the note columns are empty. The machine measurements
    are not in 0.1 ("will be released in an upcoming version").
    """
    records, study_list = [], []
    for s in echo_studies(seed, demo):
        sid = s["study_id"]
        study_list.append({"subject_id": s["subject_id"], "study_id": sid, "study_datetime": s["time"]})
        records += [
            {
                "subject_id": s["subject_id"],
                "study_id": sid,
                "acquisition_datetime": t,
                "dicom_filepath": f"{_study_dir(s['subject_id'], sid)}/{sid}_{view:04d}.dcm",
            }
            for view, t in s["views"]
        ]
    return {
        "echo-record-list.csv": csv_text(
            (["subject_id", "study_id", "acquisition_datetime", "dicom_filepath"], records)
        ).encode(),
        "echo-study-list.csv": csv_text(
            (
                ["subject_id", "study_id", "study_datetime", "note_id", "note_seq", "note_charttime"],
                study_list,
            )
        ).encode(),
    }


# --------------------------------------------------------------------------------------------------
# Registry, provenance and pinning


@dataclass(frozen=True)
class Module:
    """One simulated dataset: where it lives and how to make it."""

    slug: str  #: the PhysioNet project slug; also the directory under data/synthetic_demo/
    title: str
    version: str  #: the release whose layout is simulated
    files: Callable[[int, Demo], dict[str, bytes]]

    @property
    def data_dir(self) -> Path:
        return DATA / self.slug

    @property
    def dataset_version(self) -> str:
        """How a build from these files identifies its raw data."""
        return f"{self.version}+synthetic-{GENERATOR_VERSION}"

    @property
    def provenance_name(self) -> str:
        return f"SYNTHETIC_{self.slug}.json"


MODULES = {
    m.slug: m
    for m in (
        Module("mimic-iv-note", "MIMIC-IV-Note: Deidentified free-text clinical notes", "2.2", note_files),
        Module("mimic-cxr", "MIMIC-CXR Database", "2.1.0", cxr_files),
        Module(
            "mimic-cxr-jpg",
            "MIMIC-CXR-JPG - chest radiographs with structured labels",
            "2.1.0",
            cxr_jpg_files,
        ),
        Module("mimic-iv-echo", "MIMIC-IV-ECHO: Echocardiogram Matched Subset", "0.1", echo_files),
    )
}


def provenance(m: Module, files: dict[str, bytes], seed: int) -> bytes:
    """The machine-readable record that travels with the files (and into a demo run's raw dir)."""

    def n_rows(path: str, data: bytes) -> int | None:
        if path.endswith(".csv.gz"):
            return gzip.decompress(data).decode().count("\n") - 1  # approximate for multi-line text
        if path.endswith(".csv"):
            return data.decode().count("\n") - 1
        return None

    record = {
        "synthetic": True,
        "warning": (
            "SYNTHETIC DATA for pipeline testing and tutorials only. Not real patient data and not derived "
            "from any; do not use for research."
        ),
        "simulates": f"{m.title} {m.version} (https://physionet.org/content/{m.slug}/{m.version}/)",
        "dataset_version": m.dataset_version,
        "keyed_to": f"MIMIC-IV Clinical Database Demo {DEMO_RELEASE} (ODbL 1.0): subject_id, hadm_id, times",
        "generator": GENERATOR,
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "license": "ODbL-1.0",
        "tables": {p: n_rows(p, d) for p, d in sorted(files.items()) if not p.endswith(".txt")},
        "report_files": sum(p.endswith(".txt") for p in files),
    }
    return (json.dumps(record, indent=2) + "\n").encode()


def build(m: Module, demo: Demo, seed: int) -> dict[str, bytes]:
    files = m.files(seed, demo)
    return {**files, m.provenance_name: provenance(m, files, seed)}


def _comparable(path: str, data: bytes):
    """File content compared by `check`: decompressed, since gzip bytes vary across zlib builds."""
    return gzip.decompress(data) if path.endswith(".gz") else data


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def committed_files(m: Module) -> dict[str, bytes]:
    """The files of a module as committed, keyed by their path in the release."""
    return {
        p.relative_to(m.data_dir).as_posix(): p.read_bytes()
        for p in sorted(m.data_dir.rglob("*"))
        if p.is_file()
    }


def source_entries(m: Module, ref: str, rel_prefix: str = "") -> list[str]:
    """YAML lines (at list-item depth 0) for one `http` source fetching a module's files at `ref`."""
    lines = ["- type: http", "  urls:"]
    for rel, data in committed_files(m).items():
        path = (m.data_dir / rel).relative_to(REPO).as_posix()
        lines += [
            f"    - url: {RAW_URL.format(repo=GITHUB_REPO, ref=ref, path=path)}",
            f"      rel_path: {rel_prefix}{rel}",
            f"      sha256: {sha256(data)}",
        ]
    return lines


def sources_yaml(ref: str) -> str:
    """`data/synthetic_demo/sources.yaml`: per module, a ready-made `demo` bucket entry."""
    out = [
        "# GENERATED by `scripts/synthetic_demo.py pin`; do not edit. Per simulated module, an http source",
        "# that fetches its SYNTHETIC files from this repository at a fixed commit, checked against their",
        "# SHA-256, ready to be (part of) a spec's `demo` bucket. Files land at their release paths.",
        f"ref: {ref}",
        "modules:",
    ]
    for m in MODULES.values():
        out += [f"  {m.slug}:", f'    dataset_version: "{m.dataset_version}"', "    sources:"]
        out += [f"      {line}" for line in source_entries(m, ref)]
    return "\n".join(out) + "\n"


def pinned_ref() -> str | None:
    if not SOURCES.exists():
        return None
    m = re.search(r"^ref: (\S+)$", SOURCES.read_text(), re.MULTILINE)
    return m.group(1) if m else None


# --------------------------------------------------------------------------------------------------
# CLI


def _demo(arg: str | None) -> Demo:
    path = arg or os.environ.get("MEDS_DEMO_DOWNLOAD_DIR")
    if not path:
        raise SystemExit(
            "pass --demo-dir (a download of the public MIMIC-IV demo) or set MEDS_DEMO_DOWNLOAD_DIR"
        )
    path = Path(path)
    if not (path / "hosp" / "admissions.csv.gz").is_file():
        raise SystemExit(f"{path} is not a MIMIC-IV demo download (no hosp/admissions.csv.gz)")
    return Demo.load(path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name, help_ in (
        ("generate", "write the simulated files under data/synthetic_demo/"),
        ("check", "exit 1 if the committed files differ from a fresh generation"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("modules", nargs="*", help=f"any of {', '.join(MODULES)} (default: all)")
        p.add_argument("--demo-dir", help="the public MIMIC-IV demo (default: $MEDS_DEMO_DOWNLOAD_DIR)")
        p.add_argument("--seed", type=int, default=SEED)
    p = sub.add_parser("pin", help="point sources.yaml at the committed files at a commit")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--ref", help="a pushed commit of this repository that contains the files")
    group.add_argument("--check", action="store_true", help="exit 1 if the pinned hashes are stale")
    args = parser.parse_args(argv)

    if args.command == "pin":
        ref = args.ref or pinned_ref()
        if ref is None:
            print(f"no pin in {SOURCES.relative_to(REPO)}", file=sys.stderr)
            return 1
        text = sources_yaml(ref)
        if args.check:
            if not SOURCES.exists() or SOURCES.read_text() != text:
                print(f"{SOURCES.relative_to(REPO)} is stale: re-run `pin --ref <commit>`", file=sys.stderr)
                return 1
            return 0
        SOURCES.write_text(text)
        print(f"pinned {SOURCES.relative_to(REPO)} to {ref}")
        return 0

    if unknown := sorted(set(args.modules) - set(MODULES)):
        parser.error(f"unknown modules {unknown}; choose from {sorted(MODULES)}")
    demo = _demo(args.demo_dir)
    failed = []
    for m in [MODULES[n] for n in (args.modules or MODULES)]:
        files = build(m, demo, args.seed)
        if args.command == "generate":
            for old in m.data_dir.rglob("*"):
                if old.is_file() and old.relative_to(m.data_dir).as_posix() not in files:
                    old.unlink()
            for rel, data in files.items():
                dest = m.data_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if not dest.exists() or _comparable(rel, dest.read_bytes()) != _comparable(rel, data):
                    dest.write_bytes(data)
            total = sum(p.stat().st_size for p in m.data_dir.rglob("*") if p.is_file())
            print(f"{m.data_dir.relative_to(REPO)}: {len(files)} files, {total:,} bytes")
        else:
            committed = committed_files(m)
            if set(committed) != set(files):
                failed.append(f"{m.slug}: file sets differ: {sorted(set(committed) ^ set(files))[:5]}")
            failed += [
                f"{m.slug}/{rel}: content differs"
                for rel in sorted(set(committed) & set(files))
                if _comparable(rel, committed[rel]) != _comparable(rel, files[rel])
            ]
    if failed:
        print("stale (re-run `scripts/synthetic_demo.py generate`):", *failed, sep="\n  ", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
