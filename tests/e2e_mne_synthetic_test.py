"""`spec=MIMIC-IV-MNE` end to end: the public MIMIC-IV demo plus synthetic MIMIC-Note-Extract rows.

MIMIC-Note-Extract (MNE) has no public subset, so its tables here are fabricated: a few rows per
table, keyed to real demo admissions and written to hit every routing rule in
`extensions/mne.yaml` — the no-known-allergies row, an unstandardized allergen, a history row
repeated under a new `seq_num`, a redacted temperature, a weight in pounds and one without a unit,
a NOT_ASSESSED exam system, empty discharge-condition fields, and an admission that does not exist
in MIMIC (MNE has 57 of those against 3.1). The demo tables are the real demo download, so this
also proves the generated spec still runs the whole base ETL.

Every MNE event must land at its admission's `dischtime`; the expected events are spelled out in
full below and compared exactly.
"""

import csv
import gzip
import os
import subprocess
import sysconfig
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import CodeMetadataSchema

SPEC = "MIMIC-IV-MNE"

#: Shared with e2e_demo_test.py: CI caches the raw demo download here between runs.
DOWNLOAD_DIR_ENV_VAR = "MEDS_DEMO_DOWNLOAD_DIR"

#: An admission id that is not in the demo, like the 57 MNE admissions missing from MIMIC-IV 3.1.
ORPHAN_HADM = 29_999_999


#: MNE's column order per table (the release's CSV headers). Rows below name only the columns
#: they fill; the rest are written empty, as MNE writes nulls.
COLUMNS = {
    "allergies": "seq_num,title,title_std",
    "chief_complaint": "seq_num,category,text",
    "past_medical_history": "seq_num,subject,family_relation,concept_name,category,temporal_status",
    "medications_on_admission": (
        "seq_num,name,presentation_form,dose_value,dose_unit,dose_redacted,route,duration_value,"
        "duration_unit,prn,prn_condition,hold,hold_condition,strength,frequency"
    ),
    "discharge_diagnosis": "seq_num,title,is_primary",
    "major_procedures": "seq_num,title,is_previous",
    "physical_exam_vitals": (
        "timepoint,temperature_f,temperature_redacted,temperature_c,heart_rate,heart_rate_redacted,"
        "bp_systolic,bp_systolic_redacted,bp_diastolic,bp_diastolic_redacted,respiratory_rate,"
        "o2_saturation,weight_value,weight_unit,o2_support_method,o2_support_lpm,warnings"
    ),
    "physical_exam_systems": "timepoint,system,status,abnormal_findings",
    "discharge_condition": (
        "mental_status,level_of_consciousness,activity_status,general_condition,pain_status,"
        "cardiovascular_status,vital_signs_status,fever_status,nutrition_status,wound_presence,"
        "wound_infection_status,respiratory_status,orientation_status,neurological_status,"
        "patient_died,patient_left_ama"
    ),
}
COLUMNS["discharge_medications"] = COLUMNS["medications_on_admission"]


def _write(raw: Path, table: str, rows: list[tuple[tuple[int, int], dict]]) -> None:
    """Write `table` as MNE ships it; each row is `((subject_id, hadm_id), {column: value})`."""
    columns = ["subject_id", "hadm_id", *COLUMNS[table].split(",")]
    with gzip.open(raw / f"{table}.csv.gz", "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for (subject_id, hadm_id), values in rows:
            assert set(values) <= set(columns), f"{table}: unknown columns {set(values) - set(columns)}"
            w.writerow([subject_id, hadm_id, *(values.get(c, "") for c in columns[2:])])


def _write_mne_tables(raw: Path, a: tuple[int, int], b: tuple[int, int], c: tuple[int, int]) -> None:
    """Synthetic MNE tables over admissions `a`, `b`, `c` (each `(subject_id, hadm_id)`)."""
    orphan = (a[0], ORPHAN_HADM)
    _write(raw, "allergies", [
        (a, {"seq_num": 1, "title": "Penicillins", "title_std": "Penicillins"}),
        (a, {"seq_num": 2, "title": "Sulfa (Sulfonamide Antibiotics)", "title_std": "Sulfonamides"}),
        (b, {"seq_num": 1, "title": "No Known Allergies / Adverse Drug Reactions"}),
        (c, {"seq_num": 1, "title": "synthetic unmapped allergen"}),
        (orphan, {"seq_num": 1, "title": "Penicillins", "title_std": "Penicillins"}),
    ])  # fmt: skip
    _write(raw, "chief_complaint", [
        (a, {"seq_num": 1, "category": "FINDING", "text": "chest pain"}),
        (a, {"seq_num": 2, "category": "CONTEXT", "text": "transfer from outside hospital"}),
    ])  # fmt: skip
    htn = {
        "subject": "PATIENT",
        "concept_name": "hypertension",
        "category": "CONDITION",
        "temporal_status": "CURRENT",
    }
    _write(raw, "past_medical_history", [
        (a, {"seq_num": 1, **htn}),
        (a, {"seq_num": 2, **htn}),  # the same entry again: kept apart by seq_num
        (a, {"seq_num": 3, "subject": "FAMILY", "family_relation": "mother", "concept_name": "breast cancer",
             "category": "CONDITION", "temporal_status": "PAST"}),
        (b, {"seq_num": 1, "subject": "PATIENT", "concept_name": "appendectomy", "category": "INTERVENTION"}),
    ])  # fmt: skip
    med = {"dose_redacted": "false", "prn": "false", "hold": "false"}
    _write(raw, "medications_on_admission", [
        (a, {**med, "seq_num": 1, "name": "Lisinopril", "presentation_form": "TABLET", "dose_value": 10,
             "dose_unit": "MG", "route": "ORAL"}),
        (a, {**med, "seq_num": 2, "name": "Albuterol", "presentation_form": "INHALER", "dose_value": 2,
             "dose_unit": "PUFF", "route": "INHALATION", "prn": "true", "prn_condition": "wheezing"}),
    ])  # fmt: skip
    _write(raw, "discharge_medications", [
        (a, {**med, "seq_num": 1, "name": "Lisinopril", "presentation_form": "TABLET", "dose_value": 20,
             "dose_unit": "MG", "route": "ORAL", "hold": "true", "hold_condition": "low BP"}),
    ])  # fmt: skip
    _write(raw, "discharge_diagnosis", [
        (a, {"seq_num": 1, "title": "NSTEMI", "is_primary": "true"}),
        (a, {"seq_num": 2, "title": "Hypertension", "is_primary": "false"}),
    ])  # fmt: skip
    _write(raw, "major_procedures", [
        (a, {"seq_num": 1, "title": "Cardiac catheterization", "is_previous": "false"}),
        (a, {"seq_num": 2, "title": "CABG", "is_previous": "true"}),
    ])  # fmt: skip
    vitals = ("temperature", "heart_rate", "bp_systolic", "bp_diastolic")
    unredacted = {f"{v}_redacted": "false" for v in vitals}
    _write(raw, "physical_exam_vitals", [
        (a, {**unredacted, "timepoint": "ADMISSION", "temperature_f": 98.6, "temperature_c": 37.0,
             "heart_rate": 88, "bp_systolic": 140, "bp_diastolic": 90, "respiratory_rate": 18,
             "o2_saturation": 97, "weight_value": 220, "weight_unit": "lbs",
             "o2_support_method": "NASAL_CANNULA", "o2_support_lpm": 2, "warnings": "{}"}),
        # A redacted temperature (no value) and room air (no flow).
        (a, {**unredacted, "timepoint": "DISCHARGE", "temperature_redacted": "true", "heart_rate": 72,
             "bp_systolic": 120, "bp_diastolic": 80, "respiratory_rate": 16, "o2_saturation": 99,
             "weight_value": 80, "weight_unit": "kg", "o2_support_method": "ROOM_AIR", "warnings": "{}"}),
        # A weight without a unit is not emitted.
        (b, {**unredacted, "timepoint": "UNKNOWN", "heart_rate": 100, "weight_value": 70, "warnings": "{}"}),
    ])  # fmt: skip
    _write(raw, "physical_exam_systems", [
        (a, {"timepoint": "ADMISSION", "system": "CV", "status": "ABNORMAL",
             "abnormal_findings": '{"irregular rhythm",murmur}'}),
        (a, {"timepoint": "ADMISSION", "system": "RESP", "status": "NORMAL", "abnormal_findings": "{}"}),
        (a, {"timepoint": "ADMISSION", "system": "NEURO", "status": "NOT_ASSESSED",
             "abnormal_findings": "{}"}),
    ])  # fmt: skip
    _write(raw, "discharge_condition", [
        (a, {"mental_status": "CLEAR", "level_of_consciousness": "ALERT", "activity_status": "INDEPENDENT",
             "patient_died": "false", "patient_left_ama": "false"}),
    ])  # fmt: skip


def _expected(a: int, b: int, c: int) -> list[tuple[int, str, float | None, str | None]]:
    """`(hadm_id, code, numeric_value, text_value)` for every MNE event the rows above must produce."""
    p = "NOTE//PHYSICAL_EXAM//"
    return [
        (a, "NOTE//ALLERGY//Penicillins", None, "Penicillins"),
        (a, "NOTE//ALLERGY//Sulfonamides", None, "Sulfa (Sulfonamide Antibiotics)"),
        (b, "NOTE//ALLERGY//NONE_KNOWN", None, None),
        (c, "NOTE//ALLERGY//UNK", None, "synthetic unmapped allergen"),
        (a, "NOTE//CHIEF_COMPLAINT//FINDING", None, "chest pain"),
        (a, "NOTE//CHIEF_COMPLAINT//CONTEXT", None, "transfer from outside hospital"),
        (a, "NOTE//PAST_MEDICAL_HISTORY//PATIENT//CONDITION//CURRENT", None, "hypertension"),
        (a, "NOTE//PAST_MEDICAL_HISTORY//PATIENT//CONDITION//CURRENT", None, "hypertension"),
        (a, "NOTE//PAST_MEDICAL_HISTORY//FAMILY//CONDITION//PAST", None, "breast cancer"),
        (b, "NOTE//PAST_MEDICAL_HISTORY//PATIENT//INTERVENTION//UNK", None, "appendectomy"),
        (a, "NOTE//MEDICATION//ON_ADMISSION", None, "Lisinopril"),
        (a, "NOTE//MEDICATION//ON_ADMISSION", None, "Albuterol"),
        (a, "NOTE//MEDICATION//AT_DISCHARGE", None, "Lisinopril"),
        (a, "NOTE//DISCHARGE_DIAGNOSIS//PRIMARY", None, "NSTEMI"),
        (a, "NOTE//DISCHARGE_DIAGNOSIS//SECONDARY", None, "Hypertension"),
        (a, "NOTE//MAJOR_PROCEDURE//THIS_ADMISSION", None, "Cardiac catheterization"),
        (a, "NOTE//MAJOR_PROCEDURE//PREVIOUS", None, "CABG"),
        (a, f"{p}ADMISSION//TEMPERATURE_C", 37.0, None),
        (a, f"{p}ADMISSION//HEART_RATE", 88.0, None),
        (a, f"{p}ADMISSION//BP_SYSTOLIC", 140.0, None),
        (a, f"{p}ADMISSION//BP_DIASTOLIC", 90.0, None),
        (a, f"{p}ADMISSION//RESPIRATORY_RATE", 18.0, None),
        (a, f"{p}ADMISSION//O2_SATURATION", 97.0, None),
        (a, f"{p}ADMISSION//WEIGHT_KG", round(220 * 0.45359237, 3), None),
        (a, f"{p}ADMISSION//O2_SUPPORT//NASAL_CANNULA", 2.0, None),
        (a, f"{p}DISCHARGE//HEART_RATE", 72.0, None),
        (a, f"{p}DISCHARGE//BP_SYSTOLIC", 120.0, None),
        (a, f"{p}DISCHARGE//BP_DIASTOLIC", 80.0, None),
        (a, f"{p}DISCHARGE//RESPIRATORY_RATE", 16.0, None),
        (a, f"{p}DISCHARGE//O2_SATURATION", 99.0, None),
        (a, f"{p}DISCHARGE//WEIGHT_KG", 80.0, None),
        (a, f"{p}DISCHARGE//O2_SUPPORT//ROOM_AIR", None, None),
        (b, f"{p}UNKNOWN//HEART_RATE", 100.0, None),
        (a, f"{p}ADMISSION//CV//ABNORMAL", None, '{"irregular rhythm",murmur}'),
        (a, f"{p}ADMISSION//RESP//NORMAL", None, None),
        (a, "NOTE//DISCHARGE_CONDITION//MENTAL_STATUS//CLEAR", None, None),
        (a, "NOTE//DISCHARGE_CONDITION//LEVEL_OF_CONSCIOUSNESS//ALERT", None, None),
        (a, "NOTE//DISCHARGE_CONDITION//ACTIVITY_STATUS//INDEPENDENT", None, None),
    ]


def _run(args: list[str], env: dict) -> None:
    result = subprocess.run(args, capture_output=True, check=False, env=env)
    assert result.returncode == 0, (
        f"{' '.join(args)} failed with return code {result.returncode}.\n"
        f"stdout:\n{result.stdout.decode()}\nstderr:\n{result.stderr.decode()}"
    )


def test_e2e_mne_synthetic():
    scripts = Path(sysconfig.get_path("scripts"))
    env = os.environ | {"PATH": f"{scripts}{os.pathsep}{os.environ.get('PATH', '')}"}

    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        demo = Path(os.environ.get(DOWNLOAD_DIR_ENV_VAR) or tmp / "demo_download").resolve()
        # Skips every file that verifies against PhysioNet's manifest, so a warm cache (or the
        # base e2e test's download) costs only the manifest fetch.
        _run([str(scripts / "meds-extract-download"), "spec=MIMIC-IV", f"output_dir={demo}", "key=demo"], env)

        # The raw input the MNE spec reads: the demo tree, plus the MNE tables at its root, where
        # the PhysioNet download of MNE lands them.
        raw = tmp / "raw"
        raw.mkdir()
        for entry in demo.iterdir():
            (raw / entry.name).symlink_to(entry)

        adm = pl.read_csv(demo / "hosp" / "admissions.csv.gz", columns=["subject_id", "hadm_id", "dischtime"])
        adm = adm.sort("hadm_id")
        picks = [tuple(r) for r in adm.select("subject_id", "hadm_id").head(3).iter_rows()]
        disch = {
            h: datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
            for h, t in zip(adm["hadm_id"], adm["dischtime"], strict=True)
        }
        _write_mne_tables(raw, *picks)

        out = tmp / "MEDS_cohort"
        _run(
            [str(scripts / "meds-extract-run"), f"spec={SPEC}", f"output_dir={out}", "do_download=false",
             f"input_dir={raw}"],
            env,
        )  # fmt: skip

        data = pl.concat(
            [pl.read_parquet(f) for f in sorted((out / "data").glob("**/*.parquet"))], how="diagonal_relaxed"
        )
        note = data.filter(pl.col("code").str.starts_with("NOTE//"))

        # The base ETL is unaffected: the demo's own codes are all still there.
        assert data.filter(~pl.col("code").str.starts_with("NOTE//")).height > 100_000

        # Exactly the expected MNE events, nothing from the orphan admission. numeric_value is
        # float32, hence the rounding; the key orders None before any number.
        def key(r):
            return (r[0], r[1], -1.0 if r[2] is None else r[2], r[3] or "")

        got = sorted(
            (
                (h, c, None if v is None else round(v, 3), t)
                for h, c, v, t in note.select("hadm_id", "code", "numeric_value", "text_value").iter_rows()
            ),
            key=key,
        )
        expected = sorted(_expected(*(h for _, h in picks)), key=key)
        assert got == expected, f"MNE events differ.\n got: {got}\n expected: {expected}"
        assert ORPHAN_HADM not in note["hadm_id"].to_list()

        # Every MNE event sits at its admission's discharge time.
        wrong_time = [
            (h, c) for h, c, t in note.select("hadm_id", "code", "time").iter_rows() if t != disch[h]
        ]
        assert not wrong_time, f"MNE events not at dischtime: {wrong_time}"

        # Extension columns ride along: seq_num keeps the repeated history row, PRN/hold/dose survive.
        pmh = note.filter(pl.col("code") == "NOTE//PAST_MEDICAL_HISTORY//PATIENT//CONDITION//CURRENT")
        assert sorted(pmh["seq_num"].to_list()) == [1, 2]
        fam = note.filter(pl.col("code") == "NOTE//PAST_MEDICAL_HISTORY//FAMILY//CONDITION//PAST")
        assert fam["family_relation"].to_list() == ["mother"]
        meds = note.filter(pl.col("code") == "NOTE//MEDICATION//ON_ADMISSION").sort("seq_num")
        assert meds.select("text_value", "dose_value", "dose_unit", "route", "prn").rows() == [
            ("Lisinopril", 10.0, "MG", "ORAL", False),
            ("Albuterol", 2.0, "PUFF", "INHALATION", True),
        ]
        assert note.filter(pl.col("code") == "NOTE//MEDICATION//AT_DISCHARGE")["hold"].to_list() == [True]

        # Every MNE code is described; the exam vitals carry their LOINC parents.
        codes = pq.read_table(out / "metadata" / "codes.parquet")
        CodeMetadataSchema.validate(codes)
        meta = pl.from_arrow(codes).filter(pl.col("code").str.starts_with("NOTE//"))
        assert set(meta["code"]) == set(note["code"])
        assert meta.filter(pl.col("description").is_null()).is_empty()
        assert not meta.filter(pl.col("description").str.contains("\n")).height
        parents = dict(meta.select("code", "parent_codes").iter_rows())
        assert parents["NOTE//PHYSICAL_EXAM//ADMISSION//HEART_RATE"] == ["LOINC/8867-4"]
        assert parents["NOTE//PHYSICAL_EXAM//ADMISSION//WEIGHT_KG"] == ["LOINC/29463-7"]
        assert parents["NOTE//ALLERGY//NONE_KNOWN"] in (None, [])


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v", "-s"])
