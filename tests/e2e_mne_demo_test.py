"""`spec=MIMIC-IV-MNE dataset_key=demo` end to end: the public MIMIC-IV demo plus SIMULATED MNE tables.

MIMIC-Note-Extract has no public demo, so the spec's `demo` bucket adds the simulated MNE tables of
`data/synthetic_demo/mimic-iv-ext-note-extract/` (extracted from the simulated discharge summaries;
see `scripts/synthetic_demo.py`), fetched from GitHub at a pinned commit, to the public demo. This test
runs exactly that one command. The download dir is seeded with the cached demo and the committed MNE
files, which the downloader verifies by SHA-256 and keeps, so the test does not depend on GitHub
serving them (`synthetic_demo_test.py` checks that it does).

The expected MNE events are computed here, independently of the spec, from the simulated rows by the
rules `extensions/mne.yaml` documents, and compared exactly: codes, values, text, `seq_num`, and the
time, which must be the admission's `dischtime`. The simulated tables cover every routing rule (the
generator refuses to write them otherwise), so this checks all of them. Everything else must equal
the plain `MIMIC-IV` demo build.
"""

import json
import re
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from conftest import run_script
from meds import CodeMetadataSchema

REPO = Path(__file__).resolve().parent.parent
MNE = REPO / "data" / "synthetic_demo" / "mimic-iv-ext-note-extract"
SPEC = "MIMIC-IV-MNE"
DEMO_STAMP = "2.2+mne-1.0-synthetic-0.1"
ORPHAN_HADM = 29_999_999
NONE_KNOWN = re.compile(r"(?i)no known|nkda|\bnka\b|no allerg")
VITALS = {
    "temperature_c": "TEMPERATURE_C",
    "heart_rate": "HEART_RATE",
    "bp_systolic": "BP_SYSTOLIC",
    "bp_diastolic": "BP_DIASTOLIC",
    "respiratory_rate": "RESPIRATORY_RATE",
    "o2_saturation": "O2_SATURATION",
}
CONDITION = (
    "mental_status", "level_of_consciousness", "activity_status", "general_condition", "pain_status",
    "cardiovascular_status", "vital_signs_status", "fever_status", "nutrition_status", "wound_presence",
    "wound_infection_status", "respiratory_status", "orientation_status", "neurological_status",
)  # fmt: skip


def _rows(table: str) -> list[dict]:
    return pl.read_csv(MNE / f"{table}.csv.gz", infer_schema_length=None).to_dicts()


def expected_events() -> set[tuple]:
    """`(hadm_id, code, numeric_value, text_value, seq_num)` for every MNE event, by the fragment's rules."""
    out = []

    def add(r: dict, code: str, value=None, text=None, seq=True) -> None:
        # As the output stores it: float32, then rounded like the output values below.
        value = None if value is None else round(float(np.float32(value)), 3)
        out.append((r["hadm_id"], code, value, text,
                    r["seq_num"] if seq else None))  # fmt: skip

    for r in _rows("allergies"):
        none_known = not r["title_std"] and bool(NONE_KNOWN.search(r["title"] or ""))
        label = "NONE_KNOWN" if none_known else r["title_std"] or "UNK"
        add(r, f"NOTE//ALLERGY//{label}", text=None if none_known else r["title"])
    for r in _rows("chief_complaint"):
        add(r, f"NOTE//CHIEF_COMPLAINT//{r['category']}", text=r["text"])
    for r in _rows("past_medical_history"):
        code = f"NOTE//PAST_MEDICAL_HISTORY//{r['subject']}//{r['category']}//{r['temporal_status'] or 'UNK'}"
        add(r, code, text=r["concept_name"])
    for table, which in (
        ("medications_on_admission", "ON_ADMISSION"),
        ("discharge_medications", "AT_DISCHARGE"),
    ):
        for r in _rows(table):
            add(r, f"NOTE//MEDICATION//{which}", text=r["name"])
    for r in _rows("discharge_diagnosis"):
        add(r, f"NOTE//DISCHARGE_DIAGNOSIS//{'PRIMARY' if r['is_primary'] else 'SECONDARY'}", text=r["title"])
    for r in _rows("major_procedures"):
        add(
            r,
            f"NOTE//MAJOR_PROCEDURE//{'PREVIOUS' if r['is_previous'] else 'THIS_ADMISSION'}",
            text=r["title"],
        )
    for r in _rows("physical_exam_vitals"):
        p = f"NOTE//PHYSICAL_EXAM//{r['timepoint']}//"
        for col, name in VITALS.items():
            if r[col] is not None:
                add(r, p + name, value=r[col], seq=False)
        if r["weight_value"] is not None and r["weight_unit"] in ("kg", "lbs"):
            kg = r["weight_value"] * (0.45359237 if r["weight_unit"] == "lbs" else 1)
            add(r, p + "WEIGHT_KG", value=kg, seq=False)
        if r["o2_support_method"]:
            add(r, f"{p}O2_SUPPORT//{r['o2_support_method']}", value=r["o2_support_lpm"], seq=False)
    for r in _rows("physical_exam_systems"):
        if r["status"] != "NOT_ASSESSED":
            text = r["abnormal_findings"] if r["status"] == "ABNORMAL" else None
            add(
                r,
                f"NOTE//PHYSICAL_EXAM//{r['timepoint']}//{r['system']}//{r['status']}",
                text=text,
                seq=False,
            )
    for r in _rows("discharge_condition"):
        for aspect in CONDITION:
            if r[aspect]:
                add(r, f"NOTE//DISCHARGE_CONDITION//{aspect.upper()}//{r[aspect]}", seq=False)
    return {e for e in out if e[0] != ORPHAN_HADM}


def _data(cohort: Path) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(f) for f in sorted((cohort / "data").glob("**/*.parquet"))], how="diagonal"
    )


def test_e2e_mne_demo(demo_cohort: Path, demo_download: Path):
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # The download dir: a copy of the (cached) public demo plus the committed simulated MNE files,
        # all of which the downloader verifies and keeps rather than fetching again.
        raw = tmp / "raw"
        shutil.copytree(demo_download, raw)
        for f in MNE.iterdir():
            shutil.copy2(f, raw / f.name)

        out = tmp / "MEDS_cohort"
        run_script(
            "meds-extract-run",
            f"spec={SPEC}",
            f"output_dir={out}",
            "dataset_key=demo",
            f"download_dest_dir={raw}",
            "skip_stages=[sumeds_enrich]",
        )

        dataset = json.loads((out / "metadata" / "dataset.json").read_text())
        assert dataset["dataset_name"] == SPEC
        assert dataset["dataset_version"].startswith(f"{DEMO_STAMP}:"), dataset["dataset_version"]

        data = _data(out)
        note = data.filter(pl.col("code").str.starts_with("NOTE//"))

        # Exactly the expected events (numeric_value is float32, hence the rounding).
        got = [
            (h, c, None if v is None else round(v, 3), t, s)
            for h, c, v, t, s in note.select(
                "hadm_id", "code", "numeric_value", "text_value", "seq_num"
            ).iter_rows()
        ]
        assert len(got) == len(set(got)), "duplicate MNE events"
        expected = expected_events()
        missing = sorted(expected - set(got), key=str)[:10]
        extra = sorted(set(got) - expected, key=str)[:10]
        assert set(got) == expected, f"missing: {missing}\nextra: {extra}"

        # Every MNE event sits at its admission's discharge time; the orphan admission is gone.
        adm = pl.read_csv(demo_download / "hosp" / "admissions.csv.gz").select(
            "hadm_id", pl.col("dischtime").str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S")
        )
        timed = note.select("hadm_id", "time").join(adm, on="hadm_id", how="left")
        assert timed["dischtime"].null_count() == 0
        assert (timed["time"] == timed["dischtime"]).all()

        # Every routing case shows up in the output.
        codes = set(note["code"])
        for code in ("NOTE//ALLERGY//NONE_KNOWN", "NOTE//ALLERGY//UNK", "NOTE//ALLERGY//Penicillins"):
            assert code in codes
        for aspect in CONDITION:
            assert any(c.startswith(f"NOTE//DISCHARGE_CONDITION//{aspect.upper()}//") for c in codes), aspect
        assert {c.rsplit("//", 1)[1] for c in codes if "//O2_SUPPORT//" in c} == {
            "ROOM_AIR", "NASAL_CANNULA", "HIGH_FLOW_NASAL_CANNULA", "SIMPLE_MASK", "NON_REBREATHER",
            "CPAP_BIPAP", "VENTILATOR", "AMBU_BAG",
        }  # fmt: skip
        assert not any(c.endswith("//NOT_ASSESSED") for c in codes)
        assert any(c.endswith("//WEIGHT_KG") for c in codes)

        # Extension columns ride along.
        meds = note.filter(pl.col("code").str.starts_with("NOTE//MEDICATION//"))
        assert set(meds["prn"].drop_nulls()) == {True, False}
        assert set(meds["hold"].drop_nulls()) == {True, False}
        assert meds["dose_value"].null_count() < meds.height
        assert note["family_relation"].drop_nulls().len() > 0

        # The base part is the plain MIMIC-IV demo build, and the subject splits are unchanged.
        cols = ["subject_id", "time", "code", "numeric_value", "text_value"]
        base = _data(demo_cohort).select(cols).sort(cols, nulls_last=True)
        rest = data.filter(~pl.col("code").str.starts_with("NOTE//")).select(cols).sort(cols, nulls_last=True)
        assert rest.equals(base)
        splits = "metadata/subject_splits.parquet"
        assert (
            pl.read_parquet(out / splits)
            .sort("subject_id")
            .equals(pl.read_parquet(demo_cohort / splits).sort("subject_id"))
        )

        # Every MNE code is described; the exam vitals carry their LOINC parents.
        meta_table = pq.read_table(out / "metadata" / "codes.parquet")
        CodeMetadataSchema.validate(meta_table)
        meta = pl.from_arrow(meta_table).filter(pl.col("code").str.starts_with("NOTE//"))
        assert set(meta["code"]) == codes
        assert meta.filter(pl.col("description").is_null()).is_empty()
        assert not meta.filter(pl.col("description").str.contains("\n")).height
        parents = dict(meta.select("code", "parent_codes").iter_rows())
        assert parents["NOTE//PHYSICAL_EXAM//ADMISSION//HEART_RATE"] == ["LOINC/8867-4"]
        assert parents["NOTE//PHYSICAL_EXAM//ADMISSION//WEIGHT_KG"] == ["LOINC/29463-7"]
