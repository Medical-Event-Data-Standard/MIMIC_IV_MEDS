"""`spec=MIMIC-IV-CXRDB` end to end: the public MIMIC-IV demo plus synthetic MIMIC-CXR-DB rows.

MIMIC-CXR-DB has no public subset, so its three tables here are fabricated, a few rows each, keyed to
real demo subjects and admissions and written the way the release writes them: semicolon-delimited
`.csv`, a comma inside a concept label, `hadm_id` as a float, CheXpert labels as 1.0/0.0/-1.0/blank,
millisecond `study_datetime`s. They go through `MIMIC_IV_MEDS.prepare_cxr_db` first, as a real run
does. The demo tables are the raw files the shared `demo_cohort` build of `conftest.py` staged, and the
run skips the `sumeds_enrich` post-stage as that build does, so the base part of the output can be
compared with that build exactly. The rows hit every routing rule in `extensions/cxr_db.yaml`:

- a two-image study whose images are five minutes apart: two image events, but one study event and
  one event per CheXpert label, all at the second image;
- an unlinked study (no `hadm_id`) with a missing view position and a `No_Finding` label;
- a study of a subject who is not in MIMIC-IV, which must not reach the cohort at all;
- report and discharge concepts: a concept repeated in one section (collapses), the same concept in
  two sections (two codes), SNOMED and LOINC ids (parent codes), and a study id and an admission
  that exist nowhere else (dropped).

The expected events are spelled out in full below and compared exactly. Everything else (the base
events and the subject splits) must equal the plain `MIMIC-IV` demo build.
"""

from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pyarrow.parquet as pq
import pytest
from conftest import run_script
from meds import CodeMetadataSchema

from MIMIC_IV_MEDS import prepare_cxr_db

SPEC = "MIMIC-IV-CXRDB"

#: A MIMIC-CXR subject that is not in MIMIC-IV (3,511 such subjects in the real release).
NON_MIMIC_SUBJECT = 99_999_999
#: A study id with triples but no images (4 in the real release), and an admission not in MIMIC.
ORPHAN_STUDY = 59_999_999
ORPHAN_HADM = 29_999_999

S1, S2, S3 = 50_000_001, 50_000_002, 50_000_003

CHEXPERT = [
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
]
COLUMNS = {
    "radiographs": [
        "subject_id",
        "study_id",
        *CHEXPERT,
        "dicom_id",
        "study_datetime",
        "StudyDate",
        "StudyTime",
        "PerformedProcedureStepDescription",
        "ViewPosition",
        "Rows",
        "Columns",
        "ProcedureCodeSequence_CodeMeaning",
        "ViewCodeSequence_CodeMeaning",
        "PatientOrientationCodeSequence_CodeMeaning",
        "hadm_id",
    ],
    "radiology_jev": ["study_id", "relation_type", "object_uri", "id", "label", "class"],
    "discharge_jev": ["hadm_id", "relation_type", "object_uri", "id", "label", "class"],
}


def _ms(t: datetime) -> str:
    return t.strftime("%Y-%m-%d %H:%M:%S.") + f"{t.microsecond // 1000:03d}"


def _write(raw: Path, table: str, rows: list[dict]) -> None:
    """Write `table` as the release does: `;`-delimited, nulls as empty fields."""
    columns = COLUMNS[table]
    lines = [";".join(columns)]
    for values in rows:
        assert set(values) <= set(columns), f"{table}: unknown columns {set(values) - set(columns)}"
        lines.append(";".join(str(values.get(c, "")) for c in columns))
    (raw / f"{table}.csv").write_text("\n".join(lines) + "\n")


def _image(subject_id: int, study_id: int, dicom_id: str, t: datetime, hadm_id: int | None, **kw) -> dict:
    return {
        "subject_id": subject_id,
        "study_id": study_id,
        "dicom_id": dicom_id,
        "study_datetime": _ms(t),
        "StudyDate": t.strftime("%Y%m%d"),
        "StudyTime": t.strftime("%H%M%S.") + f"{t.microsecond // 1000:03d}",
        "Rows": 3050,
        "Columns": 2539,
        "hadm_id": "" if hadm_id is None else f"{hadm_id}.0",
        **kw,
    }


def _write_cxr_db(raw: Path, a: tuple[int, int, datetime], b: tuple[int, int, datetime]) -> dict:
    """Synthetic CXR-DB tables over demo admissions `a`, `b` (each `(subject_id, hadm_id, admittime)`).

    Returns the image times the expectations need.
    """
    t1 = a[2] + timedelta(hours=1, milliseconds=123)
    t1b = t1 + timedelta(minutes=5, milliseconds=500)
    t2 = b[2] - timedelta(days=30)
    s1 = {"Atelectasis": "1.0", "Cardiomegaly": "0.0", "Edema": "-1.0", "Support_Devices": "1.0",
          "ProcedureCodeSequence_CodeMeaning": "CHEST (PA AND LAT)",
          "PerformedProcedureStepDescription": "CHEST (PA AND LAT)"}  # fmt: skip
    _write(raw, "radiographs", [
        _image(a[0], S1, "img-1a", t1, a[1], ViewPosition="AP",
               ViewCodeSequence_CodeMeaning="antero-posterior",
               PatientOrientationCodeSequence_CodeMeaning="Erect", **s1),
        _image(a[0], S1, "img-1b", t1b, a[1], ViewPosition="LATERAL", ViewCodeSequence_CodeMeaning="lateral",
               **s1),
        _image(b[0], S2, "img-2", t2, None, PatientOrientationCodeSequence_CodeMeaning="Recumbent",
               No_Finding="1.0", ProcedureCodeSequence_CodeMeaning="CHEST (PORTABLE AP)"),
        _image(NON_MIMIC_SUBJECT, S3, "img-3", t2, None, ViewPosition="PA", Pneumonia="1.0",
               ProcedureCodeSequence_CodeMeaning="CHEST (PA AND LAT)"),
    ])  # fmt: skip
    atel = {"object_uri": "ochv:C0004144", "id": "ochv:C0004144", "label": "atelectasis, left",
            "class": "Domain_Specific"}  # fmt: skip
    _write(raw, "radiology_jev", [
        {"study_id": S1, "relation_type": "IMPRESSION", "object_uri": "snomed:46621007",
         "id": "snomed:46621007", "label": "Atelectasis", "class": "Clinical_Findings_and_Diseases"},
        {"study_id": S1, "relation_type": "IMPRESSION", **atel},
        {"study_id": S1, "relation_type": "IMPRESSION", **atel},  # repeated in the section: collapses
        {"study_id": S1, "relation_type": "INDICATION", **atel},  # same concept, other section
        {"study_id": ORPHAN_STUDY, "relation_type": "FINDINGS", **atel},
    ])  # fmt: skip
    htn = {"relation_type": "Past Medical History", "object_uri": "snomed:38341003", "id": "snomed:38341003",
           "label": "Hypertensive disorder", "class": "Clinical_Findings_and_Diseases"}  # fmt: skip
    _write(raw, "discharge_jev", [
        {"hadm_id": a[1], **htn},
        {"hadm_id": a[1], **htn},
        {"hadm_id": a[1], "relation_type": "Laboratory and Results.Discharge Labs",
         "object_uri": "loinc:2160-0", "id": "loinc:2160-0", "label": "Creatinine",
         "class": "Measurements_and_Concepts"},
        {"hadm_id": b[1], "relation_type": "Combined Medical/Surgical History", "object_uri": "mesh:D006973",
         "id": "mesh:D006973", "label": "Hypertension", "class": "Domain_Specific"},
        {"hadm_id": ORPHAN_HADM, **htn},
    ])  # fmt: skip
    return {"t1": t1.replace(microsecond=123_000), "t1b": t1b, "t2": t2}


def _expected(a, b, disch: dict[int, datetime], t: dict) -> list[tuple]:
    """`(subject_id, code, time, hadm_id, study_id, dicom_id)` for every CXR-DB event."""
    (sa, ha, _), (sb, hb, _) = a, b
    t1, t1b, t2 = t["t1"], t["t1b"], t["t2"]
    c = "CXR_NLP//CHEXPERT//"
    return [
        (sa, "CXR//IMAGE//AP//Erect", t1, ha, S1, "img-1a"),
        (sa, "CXR//IMAGE//LATERAL//UNK", t1b, ha, S1, "img-1b"),
        # Once per study, at its last image, though both image rows emit it.
        (sa, "CXR//STUDY//CHEST (PA AND LAT)", t1b, ha, S1, None),
        (sa, f"{c}ATELECTASIS//POSITIVE", t1b, ha, S1, None),
        (sa, f"{c}CARDIOMEGALY//NEGATIVE", t1b, ha, S1, None),
        (sa, f"{c}EDEMA//UNCERTAIN", t1b, ha, S1, None),
        (sa, f"{c}SUPPORT_DEVICES//POSITIVE", t1b, ha, S1, None),
        (sa, "CXR_NLP//RADIOLOGY//IMPRESSION//snomed:46621007", t1b, ha, S1, None),
        (sa, "CXR_NLP//RADIOLOGY//IMPRESSION//ochv:C0004144", t1b, ha, S1, None),
        (sa, "CXR_NLP//RADIOLOGY//INDICATION//ochv:C0004144", t1b, ha, S1, None),
        (sb, "CXR//IMAGE//UNK//Recumbent", t2, None, S2, "img-2"),
        (sb, "CXR//STUDY//CHEST (PORTABLE AP)", t2, None, S2, None),
        (sb, f"{c}NO_FINDING//POSITIVE", t2, None, S2, None),
        (sa, "CXR_NLP//DISCHARGE//Past Medical History//snomed:38341003", disch[ha], ha, None, None),
        (sa, "CXR_NLP//DISCHARGE//Laboratory and Results.Discharge Labs//loinc:2160-0", disch[ha], ha, None,
         None),
        (sb, "CXR_NLP//DISCHARGE//Combined Medical/Surgical History//mesh:D006973", disch[hb], hb, None,
         None),
    ]  # fmt: skip


def _read_data(cohort: Path) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(f) for f in sorted((cohort / "data").glob("**/*.parquet"))], how="diagonal_relaxed"
    )


def test_e2e_cxr_db_synthetic(demo_cohort: Path, demo_raw_dir: Path):
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)

        # The raw input: the demo tree plus the CXR-DB `.csv` files at its root, where the PhysioNet
        # download lands them. Symlinked, so the (possibly cached) demo download is not written to.
        raw = tmp / "raw"
        raw.mkdir()
        for entry in demo_raw_dir.iterdir():
            (raw / entry.name).symlink_to(entry)

        ts = "%Y-%m-%d %H:%M:%S"
        cols = ["subject_id", "hadm_id", "admittime", "dischtime"]
        adm = pl.read_csv(demo_raw_dir / "hosp" / "admissions.csv.gz", columns=cols).sort("hadm_id")
        rows = adm.head(2).iter_rows()
        a, b = ((s, h, datetime.strptime(t, ts)) for s, h, t, _ in rows)
        disch = {h: datetime.strptime(t, ts) for h, t in zip(adm["hadm_id"], adm["dischtime"], strict=True)}
        assert a[0] != b[0] or a[1] != b[1]
        times = _write_cxr_db(raw, a, b)

        assert prepare_cxr_db.main([str(raw)]) == 0
        assert sorted(p.name for p in (raw / "cxr_db").iterdir()) == [
            "discharge_jev.parquet",
            "radiographs.parquet",
            "radiology_jev.parquet",
        ]

        out = tmp / "MEDS_cohort"
        run_script(
            "meds-extract-run", f"spec={SPEC}", f"output_dir={out}", "do_download=false", f"input_dir={raw}",
            "skip_stages=[sumeds_enrich]",
        )  # fmt: skip

        data = _read_data(out)
        is_cxr = pl.col("code").str.starts_with("CXR//") | pl.col("code").str.starts_with("CXR_NLP//")
        cxr = data.filter(is_cxr)

        # The base ETL is unaffected: every non-CXR event equals the plain MIMIC-IV build's, and the
        # subject splits are the same. The non-MIMIC subject's study must not add a subject, which
        # would reshuffle the base cohort's splits.
        base = _read_data(demo_cohort)
        ours = data.filter(~is_cxr).select(base.columns)
        assert ours.sort(base.columns, nulls_last=True).equals(base.sort(base.columns, nulls_last=True))
        splits = "metadata/subject_splits.parquet"
        assert (
            pl.read_parquet(out / splits)
            .sort("subject_id")
            .equals(pl.read_parquet(demo_cohort / splits).sort("subject_id"))
        )
        assert NON_MIMIC_SUBJECT not in set(data["subject_id"])

        def key(r):
            return tuple("" if v is None else str(v) for v in r)

        got = sorted(
            cxr.select("subject_id", "code", "time", "hadm_id", "study_id", "dicom_id").iter_rows(), key=key
        )
        expected = sorted(_expected(a, b, disch, times), key=key)
        assert got == expected, "CXR-DB events differ.\n got:\n{}\n expected:\n{}".format(
            "\n".join(map(str, got)), "\n".join(map(str, expected))
        )
        assert ORPHAN_HADM not in cxr["hadm_id"].to_list()
        assert ORPHAN_STUDY not in cxr["study_id"].to_list()

        # Every CXR-DB code is described on one line; SNOMED and LOINC concepts carry parents; the
        # concept class rides along for filtering.
        codes = pq.read_table(out / "metadata" / "codes.parquet")
        CodeMetadataSchema.validate(codes)
        meta = pl.from_arrow(codes).filter(is_cxr)
        assert set(meta["code"]) == set(cxr["code"])
        assert meta.filter(pl.col("description").is_null()).is_empty()
        assert not meta.filter(pl.col("description").str.contains("\n")).height
        parents = dict(meta.select("code", "parent_codes").iter_rows())
        assert parents["CXR_NLP//RADIOLOGY//IMPRESSION//snomed:46621007"] == ["SNOMED/46621007"]
        assert parents["CXR_NLP//DISCHARGE//Laboratory and Results.Discharge Labs//loinc:2160-0"] == [
            "LOINC/2160-0"
        ]
        assert parents["CXR_NLP//RADIOLOGY//IMPRESSION//ochv:C0004144"] in (None, [])
        descriptions = dict(meta.select("code", "description").iter_rows())
        assert "atelectasis, left" in descriptions["CXR_NLP//RADIOLOGY//IMPRESSION//ochv:C0004144"]
        assert "IMPRESSION section" in descriptions["CXR_NLP//RADIOLOGY//IMPRESSION//ochv:C0004144"]
        # Non-standard metadata columns are aggregated per code into a list.
        classes = dict(meta.select("code", "concept_class").iter_rows())
        assert classes["CXR_NLP//DISCHARGE//Combined Medical/Surgical History//mesh:D006973"] == [
            "Domain_Specific"
        ]
        assert classes["CXR//IMAGE//AP//Erect"] in (None, [])


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v", "-s"])
