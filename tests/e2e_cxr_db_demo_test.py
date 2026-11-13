"""`spec=MIMIC-IV-CXRDB dataset_key=demo` end to end: the public MIMIC-IV demo plus SIMULATED CXR-DB.

MIMIC-CXR-DB has no public demo, so the spec's `demo` bucket adds the simulated tables of
`data/synthetic_demo/mimic-cxr-ext-mimic-cxr-db/`, fetched from GitHub at a pinned commit, to the public
demo. They are derived from the simulated MIMIC-CXR(-JPG) studies and MIMIC-IV-Note notes (see
`scripts/synthetic_demo.py`), and shipped both as the release's semicolon-delimited `.csv` and as the
`cxr_db/*.parquet` that `prepare_cxr_db` makes of them, so the demo is one command. This test:

- checks that `prepare_cxr_db` turns the shipped `.csv` into exactly the shipped parquet;
- runs that one command. The download dir is seeded with the cached demo and the committed files, which
  the downloader verifies by SHA-256 and keeps, so the test does not depend on GitHub serving them
  (`synthetic_demo_test.py` checks that it does);
- recomputes every expected CXR-DB event from the simulated rows by the rules `extensions/cxr_db.yaml`
  documents and compares exactly. The rows cover every rule (the generator refuses to write them
  otherwise): CheXpert 1/0/-1/blank, images with and without an admission, multi-image studies,
  missing views, repeated triples, SNOMED/LOINC/MeSH ids, and a subject, a study and an admission
  that exist nowhere else;
- checks that everything else equals the plain `MIMIC-IV` demo build.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pyarrow.parquet as pq
from conftest import run_script
from meds import CodeMetadataSchema

from MIMIC_IV_MEDS import prepare_cxr_db

REPO = Path(__file__).resolve().parent.parent
CXR_DB = REPO / "data" / "synthetic_demo" / "mimic-cxr-ext-mimic-cxr-db"
SPEC = "MIMIC-IV-CXRDB"
DEMO_STAMP = "2.2+cxr_db-1.0-synthetic-0.1"
ORPHAN_SUBJECT, ORPHAN_STUDY, ORPHAN_HADM = 19_999_999, 59_999_999, 29_999_999
LABELS = (
    "Atelectasis",
    "Cardiomegaly",
    "Consolidation",
    "Edema",
    "Enlarged_Cardiomediastinum",
    "Fracture",
    "Lung_Lesion",
    "Lung_Opacity",
    "No_Finding",
    "Pleural_Effusion",
    "Pleural_Other",
    "Pneumonia",
    "Pneumothorax",
    "Support_Devices",
)
STATUS = {1.0: "POSITIVE", 0.0: "NEGATIVE", -1.0: "UNCERTAIN"}
MS = "%Y-%m-%d %H:%M:%S.%f"


def _csv(table: str) -> pl.DataFrame:
    return pl.read_csv(CXR_DB / f"{table}.csv", separator=";", infer_schema_length=None)


def expected_events(demo: Path) -> set[tuple]:
    """`(subject_id, code, time, hadm_id, study_id, dicom_id)` of every CXR-DB event, by the fragment's
    rules."""
    subjects = set(pl.read_csv(demo / "hosp" / "patients.csv.gz")["subject_id"])
    adm = pl.read_csv(demo / "hosp" / "admissions.csv.gz")
    disch = {
        h: (s, datetime.strptime(t, "%Y-%m-%d %H:%M:%S"))
        for s, h, t in adm.select("subject_id", "hadm_id", "dischtime").rows()
    }
    out = set()
    studies: dict[int, dict] = {}
    for r in _csv("radiographs").iter_rows(named=True):
        t = datetime.strptime(r["study_datetime"], MS)
        hadm = None if r["hadm_id"] is None else int(r["hadm_id"])
        s = studies.setdefault(r["study_id"], {"subject": r["subject_id"], "end": t, "hadm": hadm, "row": r})
        s["end"] = max(s["end"], t)
        if r["subject_id"] not in subjects:
            continue
        orientation = r["PatientOrientationCodeSequence_CodeMeaning"] or "UNK"
        code = f"CXR//IMAGE//{r['ViewPosition'] or 'UNK'}//{orientation}"
        out.add((r["subject_id"], code, t, hadm, r["study_id"], r["dicom_id"]))
    for study_id, s in studies.items():
        if s["subject"] not in subjects:
            continue
        r = s["row"]
        at = (s["subject"], s["end"], s["hadm"], study_id, None)

        def add(code: str, at: tuple = at) -> None:
            out.add((at[0], code, *at[1:]))

        add(f"CXR//STUDY//{r['ProcedureCodeSequence_CodeMeaning']}")
        for label in LABELS:
            if r[label] is not None:
                add(f"CXR_NLP//CHEXPERT//{label.upper()}//{STATUS[r[label]]}")
    for r in _csv("radiology_jev").iter_rows(named=True):
        s = studies.get(r["study_id"])
        if s is not None and s["subject"] in subjects:
            out.add(
                (
                    s["subject"],
                    f"CXR_NLP//RADIOLOGY//{r['relation_type']}//{r['object_uri']}",
                    s["end"],
                    s["hadm"],
                    r["study_id"],
                    None,
                )
            )
    for r in _csv("discharge_jev").iter_rows(named=True):
        if r["hadm_id"] in disch:
            subject, t = disch[r["hadm_id"]]
            out.add(
                (
                    subject,
                    f"CXR_NLP//DISCHARGE//{r['relation_type']}//{r['object_uri']}",
                    t,
                    r["hadm_id"],
                    None,
                    None,
                )
            )
    return out


def _data(cohort: Path) -> pl.DataFrame:
    files = sorted((cohort / "data").glob("**/*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def test_shipped_parquet_is_the_converters_output(tmp_path: Path):
    for table in prepare_cxr_db.TABLES:
        got = prepare_cxr_db.convert_table(CXR_DB / f"{table}.csv", tmp_path / f"{table}.parquet")
        assert got.equals(pl.read_parquet(CXR_DB / prepare_cxr_db.OUT_SUBDIR / f"{table}.parquet")), table


def test_e2e_cxr_db_demo(demo_cohort: Path, demo_download: Path):
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        raw = tmp / "raw"
        shutil.copytree(demo_download, raw)
        shutil.copytree(CXR_DB, raw, dirs_exist_ok=True)

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
        is_cxr = pl.col("code").str.starts_with("CXR//") | pl.col("code").str.starts_with("CXR_NLP//")
        cxr = data.filter(is_cxr)
        got = list(
            cxr.select("subject_id", "code", "time", "hadm_id", "cxr_study_id", "cxr_dicom_id").iter_rows()
        )
        assert len(got) == len(set(got)), "duplicate CXR-DB events"
        expected = expected_events(demo_download)
        missing = sorted(expected - set(got), key=str)[:10]
        extra = sorted(set(got) - expected, key=str)[:10]
        assert set(got) == expected, f"missing: {missing}\nextra: {extra}"
        assert ORPHAN_SUBJECT not in set(data["subject_id"])
        assert ORPHAN_STUDY not in set(cxr["cxr_study_id"].drop_nulls())
        assert ORPHAN_HADM not in set(cxr["hadm_id"].drop_nulls())

        # Every routing case shows up in the output.
        codes = set(cxr["code"])
        for status in STATUS.values():
            assert any(c.startswith("CXR_NLP//CHEXPERT//") and c.endswith(status) for c in codes), status
        assert any("//UNK" in c for c in codes if c.startswith("CXR//IMAGE//"))
        assert cxr.filter(pl.col("code").str.starts_with("CXR//IMAGE//"))["hadm_id"].null_count() > 0
        for prefix in ("CXR//STUDY//", "CXR_NLP//RADIOLOGY//", "CXR_NLP//DISCHARGE//"):
            assert any(c.startswith(prefix) for c in codes), prefix

        # The base part is the plain MIMIC-IV demo build, and the subject splits are unchanged.
        base = _data(demo_cohort)
        ours = data.filter(~is_cxr).select(base.columns)
        assert ours.sort(base.columns, nulls_last=True).equals(base.sort(base.columns, nulls_last=True))
        splits = "metadata/subject_splits.parquet"
        assert (
            pl.read_parquet(out / splits)
            .sort("subject_id")
            .equals(pl.read_parquet(demo_cohort / splits).sort("subject_id"))
        )

        # Every CXR-DB code is described on one line; SNOMED and LOINC concepts carry parents, MeSH none.
        table = pq.read_table(out / "metadata" / "codes.parquet")
        CodeMetadataSchema.validate(table)
        meta = pl.from_arrow(table).filter(is_cxr)
        assert set(meta["code"]) == codes
        assert meta.filter(pl.col("description").is_null()).is_empty()
        assert not meta.filter(pl.col("description").str.contains("\n")).height
        parents = dict(meta.select("code", "parent_codes").iter_rows())
        for code, parent in parents.items():
            uri = code.rsplit("//", 1)[1]
            if uri.startswith("snomed:"):
                assert parent == [f"SNOMED/{uri[7:]}"], code
            elif uri.startswith("loinc:"):
                assert parent == [f"LOINC/{uri[6:]}"], code
            elif uri.startswith("mesh:"):
                assert parent in (None, []), code
