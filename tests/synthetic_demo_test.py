"""The simulated demo datasets under `data/synthetic_demo/` are current, pinned, and self-consistent.

`scripts/synthetic_demo.py` simulates MIMIC-IV-Note, MIMIC-CXR, MIMIC-CXR-JPG and MIMIC-IV-ECHO for the
public MIMIC-IV demo's subjects and admissions (none of them has a public demo). These tests check that:

- the committed files are exactly what the generator writes from the pinned demo (so nobody edits them
  by hand, and a generator change comes with its data);
- `sources.yaml` pins every committed file by SHA-256, and the pinned URLs serve those bytes;
- each table has its release's documented columns, and the modules agree with each other and with
  the demo: ids, times inside or near the right stays, one report per CXR study with the same text,
  CheXpert labels that match the report's findings, image lists that match the metadata;
- every file is small, and every note says it is synthetic.
"""

import gzip
import hashlib
import importlib.util
import io
import re
import sys
import urllib.request
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "data" / "synthetic_demo"

_spec = importlib.util.spec_from_file_location("synthetic_demo", REPO / "scripts" / "synthetic_demo.py")
synthetic_demo = sys.modules["synthetic_demo"] = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(synthetic_demo)

NOTE = DATA / "mimic-iv-note" / "note"
CXR = DATA / "mimic-cxr"
JPG = DATA / "mimic-cxr-jpg"
ECHO = DATA / "mimic-iv-echo"
TIME = "%Y-%m-%d %H:%M:%S"


def _read(path: Path) -> pl.DataFrame:
    return pl.read_csv(path, infer_schema_length=None)


@pytest.fixture(scope="module")
def admissions(demo_download: Path) -> pl.DataFrame:
    return pl.read_csv(demo_download / "hosp" / "admissions.csv.gz").with_columns(
        pl.col("admittime", "dischtime", "edregtime", "edouttime").str.strptime(pl.Datetime, TIME)
    )


def test_committed_files_are_current(demo_download: Path):
    assert synthetic_demo.main(["check", "--demo-dir", str(demo_download)]) == 0


def test_sources_pin_every_committed_file():
    assert synthetic_demo.main(["pin", "--check"]) == 0
    pinned = yaml.safe_load((DATA / "sources.yaml").read_text())
    for slug, module in synthetic_demo.MODULES.items():
        [source] = pinned["modules"][slug]["sources"]
        assert source["type"] == "http"
        assert {u["rel_path"] for u in source["urls"]} == set(synthetic_demo.committed_files(module))


def test_pinned_urls_serve_the_committed_bytes():
    """One file per module, fetched from GitHub at the pinned commit (needs network, as the demo does)."""
    pinned = yaml.safe_load((DATA / "sources.yaml").read_text())
    for slug, module in pinned["modules"].items():
        url = module["sources"][0]["urls"][0]
        with urllib.request.urlopen(url["url"], timeout=60) as r:
            assert hashlib.sha256(r.read()).hexdigest() == url["sha256"], f"{slug}: {url['url']}"


def test_meds_extract_downloads_a_pinned_module(tmp_path: Path):
    """The pinned entries work as a spec's `demo` bucket: MEDS-Extract's own downloader fetches them, verifies
    each SHA-256 and lays the files out at their release paths."""
    from conftest import run_script

    module = yaml.safe_load((DATA / "sources.yaml").read_text())["modules"]["mimic-iv-note"]
    spec = tmp_path / "spec.yaml"
    spec.write_text(
        yaml.safe_dump(
            {"sources": {"dataset_version": {"demo": module["dataset_version"]}, "demo": module["sources"]}}
        )
    )
    out = tmp_path / "raw"
    run_script("meds-extract-download", f"spec={spec}", f"output_dir={out}", "key=demo")
    for rel in synthetic_demo.committed_files(synthetic_demo.MODULES["mimic-iv-note"]):
        assert (out / rel).read_bytes() == (DATA / "mimic-iv-note" / rel).read_bytes(), rel


def test_files_are_small_and_labelled_synthetic():
    files = [p for p in DATA.rglob("*") if p.is_file()]
    assert max(p.stat().st_size for p in files) < 500_000
    assert sum(p.stat().st_size for p in files) < 2_000_000
    for slug, module in synthetic_demo.MODULES.items():
        prov = (module.data_dir / module.provenance_name).read_text()
        assert '"synthetic": true' in prov, slug
    for table in ("discharge", "radiology"):
        text = _read(NOTE / f"{table}.csv.gz")["text"]
        assert text.str.starts_with("SYNTHETIC ").all()
    reports = list(CXR.glob("files/p*/p*/s*.txt"))
    assert reports and all(p.read_text().startswith("SYNTHETIC ") for p in reports)


def test_note_tables(admissions: pl.DataFrame):
    note_cols = [
        "note_id",
        "subject_id",
        "hadm_id",
        "note_type",
        "note_seq",
        "charttime",
        "storetime",
        "text",
    ]
    detail_cols = ["note_id", "subject_id", "field_name", "field_value", "field_ordinal"]
    ds, rr = _read(NOTE / "discharge.csv.gz"), _read(NOTE / "radiology.csv.gz")
    for df in (ds, rr):
        assert df.columns == note_cols
        assert df["note_id"].is_unique().all()
        ids = df.select(pl.format("{}-{}-{}", "subject_id", "note_type", "note_seq"))
        assert (ids.to_series() == df["note_id"]).all()
        times = df.select(pl.col("charttime", "storetime").str.strptime(pl.Datetime, TIME))
        assert (times["storetime"] > times["charttime"]).all()
    for name in ("discharge_detail", "radiology_detail"):
        assert _read(NOTE / f"{name}.csv.gz").columns == detail_cols

    # One discharge summary per demo admission, charted at its discharge time.
    adm = admissions.select("hadm_id", "subject_id", "admittime", "dischtime")
    summaries = ds.filter(pl.col("note_type") == "DS").with_columns(
        pl.col("charttime").str.strptime(pl.Datetime, TIME)
    )
    assert sorted(summaries["hadm_id"]) == sorted(adm["hadm_id"])
    joined = summaries.join(adm, on="hadm_id", suffix="_adm")
    assert (joined["charttime"] == joined["dischtime"]).all()
    assert (joined["subject_id"] == joined["subject_id_adm"]).all()
    assert set(ds["note_type"]) == {"DS", "AD"}

    # Radiology: linked reports fall inside their admission; some are outside any admission.
    rr = rr.with_columns(pl.col("charttime").str.strptime(pl.Datetime, TIME))
    linked = rr.filter(pl.col("hadm_id").is_not_null()).join(adm, on="hadm_id", suffix="_adm")
    assert linked.height == rr["hadm_id"].is_not_null().sum()
    assert linked.filter(~pl.col("charttime").is_between("admittime", "dischtime")).is_empty()
    assert rr["hadm_id"].null_count() > 0
    assert set(rr["note_type"]) == {"RR", "AR"}
    assert set(rr["subject_id"]) <= set(adm["subject_id"])

    # Addenda link both ways; every report has an exam name.
    detail = _read(NOTE / "radiology_detail.csv.gz")
    links = dict(
        detail.filter(pl.col("field_name") == "addendum_note_id").select("note_id", "field_value").rows()
    )
    back = dict(
        detail.filter(pl.col("field_name") == "parent_note_id").select("field_value", "note_id").rows()
    )
    assert links and links == back
    assert set(detail.filter(pl.col("field_name") == "exam_name")["note_id"]) == set(rr["note_id"])


def test_cxr_modules(admissions: pl.DataFrame):
    records = _read(CXR / "cxr-record-list.csv.gz")
    studies = _read(CXR / "cxr-study-list.csv.gz")
    providers = _read(CXR / "cxr-provider-list.csv.gz")
    meta = _read(JPG / "mimic-cxr-2.0.0-metadata.csv.gz")
    split = _read(JPG / "mimic-cxr-2.0.0-split.csv.gz")
    chexpert = _read(JPG / "mimic-cxr-2.0.0-chexpert.csv.gz")
    negbio = _read(JPG / "mimic-cxr-2.0.0-negbio.csv.gz")
    test_set = _read(JPG / "mimic-cxr-2.1.0-test-set-labeled.csv")

    assert records.columns == ["subject_id", "study_id", "dicom_id"]
    assert studies.columns == ["subject_id", "study_id"]
    assert providers.columns == ["study_id", "ordering", "attending", "provider"]
    assert meta.columns == synthetic_demo.CXR_METADATA_COLUMNS
    assert split.columns == ["dicom_id", "study_id", "subject_id", "split"]
    labels = list(synthetic_demo.CHEXPERT_LABELS)
    assert chexpert.columns == negbio.columns == ["subject_id", "study_id", *labels]
    assert test_set.columns == ["study_id", *labels]

    # Ids as MIMIC-CXR documents them; one record per image everywhere.
    assert studies["study_id"].cast(pl.String).str.contains(r"^5\d{7}$").all()
    assert records["dicom_id"].str.contains(r"^([0-9a-f]{8}-){4}[0-9a-f]{8}$").all()
    assert set(studies["subject_id"]) <= set(admissions["subject_id"])
    assert set(records["dicom_id"]) == set(meta["dicom_id"]) == set(split["dicom_id"])
    assert set(records["study_id"]) == set(studies["study_id"]) == set(providers["study_id"])
    image_files = (JPG / "IMAGE_FILENAMES").read_text().split()
    expected = records.select(
        pl.format(
            "files/p{}/p{}/s{}/{}.jpg",
            pl.col("subject_id").cast(pl.String).str.slice(0, 2),
            "subject_id",
            "study_id",
            "dicom_id",
        )
    ).to_series()
    assert sorted(image_files) == sorted(expected)
    assert set(split["split"]) == {"train", "validate", "test"}
    assert set(test_set["study_id"]) <= set(split.filter(pl.col("split") == "test")["study_id"])

    # One report per study, identical to a chest X-ray report in MIMIC-IV-Note, imaged at or just
    # after that report's charttime.
    reports = {int(p.stem[1:]): p.read_text() for p in CXR.glob("files/p*/p*/s*.txt")}
    assert set(reports) == set(studies["study_id"])
    rr = _read(NOTE / "radiology.csv.gz").filter(pl.col("note_type") == "RR")
    notes: dict[tuple[int, str], list[datetime]] = {}
    for subject, text, charttime in rr.select("subject_id", "text", "charttime").rows():
        notes.setdefault((subject, text), []).append(datetime.strptime(charttime, TIME))
    study_subject = dict(studies.select("study_id", "subject_id").rows())
    first_image = meta.group_by("study_id").agg(pl.col("StudyDate", "StudyTime").first())
    for study_id, date, time in first_image.rows():
        charttimes = notes[(study_subject[study_id], reports[study_id])]
        imaged = datetime.strptime(f"{date}{int(time):06d}", "%Y%m%d%H%M%S")
        assert any(0 <= (imaged - c).total_seconds() < 600 for c in charttimes), (
            study_id,
            imaged,
            charttimes,
        )

    # CheXpert labels are the report's findings; unlabelable reports are in no label file.
    assert chexpert.equals(negbio)
    allowed = {1.0, 0.0, -1.0, None}
    for row in chexpert.iter_rows(named=True):
        text = reports[row["study_id"]].lower()
        for label in labels[:-1]:
            v = row[label]
            assert v in allowed
            assert (v == 1.0) == (f"{label.lower()} is present" in text), (row["study_id"], label)
        assert (row["No Finding"] == 1.0) == ("no acute cardiopulmonary process" in text)
    unlabelled = set(studies["study_id"]) - set(chexpert["study_id"])
    assert unlabelled and all("FINDINGS:" not in reports[s] for s in unlabelled)
    values = set(chexpert.select(labels).unpivot()["value"])
    assert values == allowed


def test_echo_module(admissions: pl.DataFrame):
    records = _read(ECHO / "echo-record-list.csv")
    studies = _read(ECHO / "echo-study-list.csv")
    assert records.columns == ["subject_id", "study_id", "acquisition_datetime", "dicom_filepath"]
    assert studies.columns == [
        "subject_id",
        "study_id",
        "study_datetime",
        "note_id",
        "note_seq",
        "note_charttime",
    ]
    assert studies["study_id"].cast(pl.String).str.contains(r"^9\d{7}$").all()
    assert set(records["study_id"]) == set(studies["study_id"])
    path = re.compile(r"^files/p(\d{2})/p(\d{8})/s(\d{8})/(\d{8})_\d{4}\.dcm$")
    for subject, study, filepath in records.select("subject_id", "study_id", "dicom_filepath").rows():
        m = path.match(filepath)
        assert m and m[1] == str(subject)[:2] and int(m[2]) == subject and int(m[3]) == int(m[4]) == study

    # Some studies fall inside an admission, some outside any.
    t = studies.with_columns(pl.col("study_datetime").str.strptime(pl.Datetime, TIME))
    inside = t.join(admissions.select("subject_id", "admittime", "dischtime"), on="subject_id").filter(
        pl.col("study_datetime").is_between("admittime", "dischtime")
    )
    assert 0 < inside["study_id"].n_unique() < studies.height
    acq = records.with_columns(pl.col("acquisition_datetime").str.strptime(pl.Datetime, TIME)).join(
        t.select("study_id", "study_datetime"), on="study_id"
    )
    assert (acq["acquisition_datetime"] >= acq["study_datetime"]).all()


def test_gzip_is_reproducible():
    """Same text, same bytes: no timestamp or file name in the gzip header."""
    a = synthetic_demo.gzip_bytes("x,y\n1,2\n")
    assert a == synthetic_demo.gzip_bytes("x,y\n1,2\n")
    assert gzip.GzipFile(fileobj=io.BytesIO(a)).read() == b"x,y\n1,2\n"
