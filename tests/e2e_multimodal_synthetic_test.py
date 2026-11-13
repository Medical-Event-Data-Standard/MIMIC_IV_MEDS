"""`mimic-iv-meds-multimodal` end to end, on the public MIMIC-IV demo plus made-up module releases.

MIMIC-IV-Note, -ECHO and MIMIC-CXR-JPG are credentialed, so CI cannot use them. This test writes tiny
releases in their PhysioNet layouts instead (fake bytes for the objects, made-up text for the notes) for a
few real demo subjects, puts the public core demo next to them, and runs the CLI with every object module
and notes. Each case it covers is planted on purpose and checked exactly:

- a listed ECG whose `.dat` is missing and an echo listed for a subject outside the core get no event, and
  are counted in `metadata/sources.json`;
- two CXR images with identical bytes share one link (content addressing);
- a `StudyTime` written without its leading zeros (`4212.5`) and a missing `ViewPosition`;
- note times are `max(charttime, storetime)`; note text lands in `text_value`;
- every link resolves inside the MEDS root, and `objects.parquet` maps it back to its source file;
- `dataset.json` names the combination, its releases, its license and its link columns.
"""

import gzip
import hashlib
import json
import os
from pathlib import Path

import polars as pl
import pytest
from conftest import run_script

TS = "%Y-%m-%d %H:%M:%S"


def _csv(fp: Path, df: pl.DataFrame, gz: bool = False) -> None:
    fp.parent.mkdir(parents=True, exist_ok=True)
    if gz:
        with gzip.open(fp, "wb") as f:
            df.write_csv(f)
    else:
        df.write_csv(fp)


def _write(fp: Path, data: bytes) -> str:
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


@pytest.fixture(scope="module")
def synthetic_root(demo_raw_dir: Path, tmp_path_factory) -> tuple[Path, list[int]]:
    root = tmp_path_factory.mktemp("mm_root")
    core = root / "mimic-iv-demo" / "2.2"
    core.mkdir(parents=True)
    for sub in ("hosp", "icu"):
        (core / sub).symlink_to(demo_raw_dir / sub, target_is_directory=True)
    s = sorted(pl.read_csv(demo_raw_dir / "hosp" / "patients.csv.gz")["subject_id"].to_list())[:3]
    outsider = 99999999

    ecg = root / "mimic-iv-ecg-demo-diagnostic-electrocardiogram-matched-subset-demo-0.1"
    rows = []
    for i, (subj, study) in enumerate([(s[0], 40000001), (s[1], 40000002), (s[1], 40000003)]):
        path = f"files/p{str(subj)[:4]}/p{subj}/s{study}/{study}"
        _write(ecg / f"{path}.hea", f"{study} 12 500 5000\n{study}.dat 16 200 16 0 0 0 0 I\n".encode())
        if i != 2:  # the third record's signal file was "not downloaded"
            _write(ecg / f"{path}.dat", bytes([i]) * 64)
        rows.append(
            {
                "subject_id": subj,
                "study_id": study,
                "file_name": study,
                "ecg_time": f"2150-01-0{i + 1} 10:00:00",
                "path": path,
            }
        )
    _csv(ecg / "record_list.csv", pl.DataFrame(rows))

    echo = root / "mimic-iv-echo" / "1.0.1"
    rows = []
    for subj, study, n, t in [
        (s[0], 90000001, 1, "2150-02-01 08:30:00"),
        (s[2], 90000002, 2, "2150-02-02 09:00:00"),
        (outsider, 90000003, 1, "2150-02-03 09:00:00"),
    ]:
        rel = f"files/p{str(subj)[:2]}/p{subj}/s{study}/{study}_{n:04d}.dcm"
        _write(echo / rel, f"DICM{study}{n}".encode())
        rows.append({"subject_id": subj, "study_id": study, "acquisition_datetime": t, "dicom_filepath": rel})
    _csv(echo / "echo-record-list.csv", pl.DataFrame(rows))

    cxr = root / "mimic-cxr-jpg" / "2.1.0"
    rows = []
    for subj, study, dicom, data, date, time, view in [
        (s[0], 50000001, "aaaa-1", b"JPEG-A", "21500301", "4212.5", "PA"),
        (s[0], 50000001, "aaaa-2", b"JPEG-B", "21500301", "104212.0", ""),
        (s[1], 50000002, "bbbb-1", b"JPEG-A", "21500302", "235959", "AP"),  # same bytes as aaaa-1
    ]:
        _write(cxr / f"files/p{str(subj)[:2]}/p{subj}/s{study}/{dicom}.jpg", data)
        rows.append(
            {
                "dicom_id": dicom,
                "subject_id": subj,
                "study_id": study,
                "ViewPosition": view,
                "StudyDate": date,
                "StudyTime": time,
            }
        )
    _csv(cxr / "mimic-cxr-2.0.0-metadata.csv", pl.DataFrame(rows))

    note = root / "mimic-iv-note" / "2.2" / "note"
    _csv(
        note / "discharge.csv.gz",
        pl.DataFrame(
            {
                "note_id": [f"{s[0]}-DS-1"],
                "subject_id": [s[0]],
                "hadm_id": [None],
                "note_type": ["DS"],
                "note_seq": [1],
                "charttime": ["2150-04-01 00:00:00"],
                "storetime": ["2150-04-02 13:14:15"],
                "text": ["Synthetic discharge text.\nLine two."],
            }
        ),
        gz=True,
    )
    _csv(
        note / "radiology.csv.gz",
        pl.DataFrame(
            {
                "note_id": [f"{s[1]}-RR-1", f"{s[1]}-AR-2"],
                "subject_id": [s[1], s[1]],
                "hadm_id": [None, None],
                "note_type": ["RR", "AR"],
                "note_seq": [1, 2],
                "charttime": ["2150-05-01 10:00:00", "2150-05-01 10:00:00"],
                "storetime": [None, "2150-05-03 11:00:00"],
                "text": ["Synthetic report.", "Synthetic addendum."],
            }
        ),
        gz=True,
    )
    return root, s


@pytest.fixture(scope="module")
def built(synthetic_root, demo_raw_dir, tmp_path_factory) -> tuple[Path, list[int]]:
    root, s = synthetic_root
    out = tmp_path_factory.mktemp("mm_out") / "meds"
    run_script(
        "mimic-iv-meds-multimodal",
        "--root",
        str(root),
        "--output-dir",
        str(out),
        "--modules",
        "note,ecg,echo,cxr",
        "--common-dir",
        str(demo_raw_dir),
        "--no-enrich",
        "--cache-dir",
        str(out.parent / "cache"),
    )
    return out, s


def _events(out: Path, prefix: str) -> pl.DataFrame:
    return (
        pl.scan_parquet(out / "data" / "**" / "*.parquet")
        .filter(pl.col("code").str.starts_with(prefix))
        .collect()
        .sort("subject_id", "time", "code")
    )


def test_dataset_json(built):
    out, _ = built
    meta = json.loads((out / "metadata" / "dataset.json").read_text())
    assert meta["dataset_name"] == "MIMIC-IV+Note+ECG+ECHO+CXR"
    assert meta["dataset_version"].split(",") == [
        "mimic-iv-demo/2.2",
        "mimic-iv-note/2.2",
        "mimic-iv-ecg-demo/0.1",
        "mimic-iv-echo/1.0.1",
        "mimic-cxr-jpg/2.1.0",
    ]
    assert meta["license"] == "PhysioNet Credentialed Health Data License 1.5.0"
    assert meta["additional_value_modality_columns"] == ["ecg_link", "echo_link", "cxr_link"]
    assert {"note_id", "ecg_study_id", "echo_study_id", "cxr_study_id", "cxr_dicom_id"} <= set(
        meta["raw_source_id_columns"]
    )
    assert not {"ecg_link", "echo_link", "cxr_link"} & set(meta.get("other_extension_columns", []))


def test_ecg(built):
    out, s = built
    ev = _events(out, "ECG//")
    assert ev.select("subject_id", "ecg_study_id", pl.col("time").dt.strftime(TS)).rows() == [
        (s[0], 40000001, "2150-01-01 10:00:00"),
        (s[1], 40000002, "2150-01-02 10:00:00"),
    ]
    assert set(ev["code"]) == {"ECG//WAVEFORM"}
    for link, study in ev.select("ecg_link", "ecg_study_id").rows():
        # The record keeps its own name inside its content-addressed folder, so its header finds its signal.
        assert link.endswith(f"/{study}")
        assert (out / f"{link}.hea").read_text().split("\n")[1].startswith(f"{study}.dat")
        assert (out / f"{link}.dat").exists()


def test_echo(built):
    out, s = built
    ev = _events(out, "ECHO//")
    assert ev.select("subject_id", "echo_study_id", pl.col("time").dt.strftime(TS)).rows() == [
        (s[0], 90000001, "2150-02-01 08:30:00"),
        (s[2], 90000002, "2150-02-02 09:00:00"),
    ]
    for link, study in ev.select("echo_link", "echo_study_id").rows():
        assert link.startswith("echo/") and link.endswith(".dcm")
        assert (out / link).read_bytes().startswith(f"DICM{study}".encode())


def test_cxr(built):
    out, s = built
    ev = _events(out, "CXR//")
    assert ev.select("subject_id", "code", "cxr_dicom_id", pl.col("time").dt.strftime(TS)).rows() == [
        (s[0], "CXR//IMAGE//PA", "aaaa-1", "2150-03-01 00:42:12"),
        (s[0], "CXR//IMAGE//UNKNOWN", "aaaa-2", "2150-03-01 10:42:12"),
        (s[1], "CXR//IMAGE//AP", "bbbb-1", "2150-03-02 23:59:59"),
    ]
    links = dict(ev.select("cxr_dicom_id", "cxr_link").rows())
    assert links["aaaa-1"] == links["bbbb-1"] != links["aaaa-2"]
    assert (
        links["aaaa-1"]
        == f"cxr/{hashlib.sha256(b'JPEG-A').hexdigest()[:2]}/{hashlib.sha256(b'JPEG-A').hexdigest()}.jpg"
    )
    assert (out / links["aaaa-2"]).read_bytes() == b"JPEG-B"


def test_notes(built):
    out, s = built
    ev = _events(out, "NOTE//")
    assert ev.select(
        "subject_id", "code", "note_id", pl.col("time").dt.strftime(TS), "text_value"
    ).rows() == [
        (
            s[0],
            "NOTE//DISCHARGE_SUMMARY",
            f"{s[0]}-DS-1",
            "2150-04-02 13:14:15",
            "Synthetic discharge text.\nLine two.",
        ),
        (s[1], "NOTE//RADIOLOGY_REPORT", f"{s[1]}-RR-1", "2150-05-01 10:00:00", "Synthetic report."),
        (
            s[1],
            "NOTE//RADIOLOGY_REPORT//ADDENDUM",
            f"{s[1]}-AR-2",
            "2150-05-03 11:00:00",
            "Synthetic addendum.",
        ),
    ]


def test_objects_and_sources(built, synthetic_root):
    out, _ = built
    root, _ = synthetic_root
    objects = pl.read_parquet(out / "metadata" / "objects.parquet")
    assert objects.group_by("modality").len().sort("modality").rows() == [("cxr", 3), ("ecg", 2), ("echo", 2)]
    for row in objects.iter_rows(named=True):
        src_root = {
            "ecg": root / "mimic-iv-ecg-demo-diagnostic-electrocardiogram-matched-subset-demo-0.1",
            "echo": root / "mimic-iv-echo" / "1.0.1",
            "cxr": root / "mimic-cxr-jpg" / "2.1.0",
        }[row["modality"]]
        if row["modality"] != "ecg":
            assert hashlib.sha256((src_root / row["files"][0]).read_bytes()).hexdigest() == row["sha256"]
        assert sum((src_root / f).stat().st_size for f in row["files"]) == row["size_bytes"]

    sources = json.loads((out / "metadata" / "sources.json").read_text())
    assert sources["modules"] == ["core", "note", "ecg", "echo", "cxr"]
    by = {x["module"]: x for x in sources["sources"]}
    assert {"listed": 3, "missing_on_disk": 1, "emitted": 2}.items() <= by["ecg"]["staging"].items()
    assert by["echo"]["staging"]["outside_core_subjects"] == 1
    assert by["cxr"]["staging"]["distinct_objects"] == 2
    assert by["cxr"]["objects"] == {"objects": 2, "files_written": 2, "bytes": 12}
    assert by["note"]["staging"]["radiology"]["missing_storetime"] == 1
    assert (by["core"]["release"], by["note"]["release"], by["ecg"]["license"]) == (
        "demo",
        "full",
        "ODbL-1.0",
    )


@pytest.mark.skipif(os.name == "nt", reason="symlinks")
def test_rerun_needs_overwrite(built, synthetic_root, demo_raw_dir):
    out, _ = built
    root, _ = synthetic_root
    with pytest.raises(AssertionError, match="not empty"):
        run_script(
            "mimic-iv-meds-multimodal",
            "--root",
            str(root),
            "--output-dir",
            str(out),
            "--modules",
            "ecg",
            "--common-dir",
            str(demo_raw_dir),
            "--no-enrich",
        )
