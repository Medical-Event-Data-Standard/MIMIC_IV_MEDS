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
- every `external_value_link` resolves inside `external_values/`, which holds nothing else, and
  `external_value_encoding` says how to read it;
- the waveform database gives one linked window per (short) segment and its vital signs as events;
- `dataset.json` names the combination, its releases, its license and its link column.

The build also takes MIMIC-Note-Extract and MIMIC-CXR-DB from this repository's simulated
`data/synthetic_demo/` (a second `--root`): they are used because no real release of them is there,
flagged as synthetic, and CXR-DB leaves the image events to MIMIC-CXR-JPG.
"""

import gzip
import hashlib
import json
import os
from pathlib import Path

import polars as pl
import pytest
from conftest import run_script

from MIMIC_IV_MEDS.multimodal import finalize

TS = "%Y-%m-%d %H:%M:%S"
SIMULATED = Path(__file__).resolve().parents[1] / "data" / "synthetic_demo"
EXTENSIONS = "mimic-iv-note,mimic-iv-mne,mimic-iv-ecg,mimic-iv-echo,mimic-iv-cxr,mimic-iv-cxr-db,mimic-iv-wdb"


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
    # Like the real table, it holds patients outside the cohort; measurement 7001 belongs to study 90000001.
    _csv(
        echo / "echo-study-list.csv",
        pl.DataFrame(
            {
                "subject_id": [s[0]],
                "study_id": [90000001],
                "study_datetime": ["2150-02-01 08:00:00"],
                "measurement_id": [7001],
                "measurement_datetime": ["2150-02-01 09:00:00"],
                "note_id": [None],
                "note_seq": [None],
                "note_charttime": [None],
            }
        ),
    )
    _csv(
        echo / "structured-measurement.csv.gz",
        pl.DataFrame(
            {
                "subject_id": [s[0], s[0], s[2], s[2], outsider],
                "measurement_id": [7001, 7001, 7002, 7002, 7003],
                "measurement_datetime": ["2150-02-01 09:00:00"] * 2
                + ["2150-02-05 12:00:00"] * 2
                + ["2150-02-06 12:00:00"],
                "test_type": ["TTE", "TTE", "TEE", "TEE", "TTE"],
                "measurement": ["lvef", "lv_size", "lvef", "rvsp", "lvef"],
                "measurement_description": [
                    "LV ejection fraction",
                    "LV cavity size",
                    "LV ejection fraction",
                    "RV systolic pressure",
                    "LV ejection fraction",
                ],
                "result": ["55", "Normal (<5.6cm)", "60.5", "", "40"],
                "unit": ["%", "", "%", "mmHg", "%"],
            }
        ),
        gz=True,
    )

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

    # MIMIC-IV Waveform Database: RECORDS lists every patient's folder (one outside the cohort, one not
    # downloaded); one record of two segments with a gap, preceded by a zero-length layout segment.
    wdb = root / "mimic4wdb" / "0.1.0"
    pdir = f"waves/p{str(s[0])[:3]}/p{s[0]}"
    (wdb / "waves").mkdir(parents=True)
    (wdb / "RECORDS").write_text(f"{pdir}/\nwaves/p999/p{outsider}/\nwaves/p{str(s[2])[:3]}/p{s[2]}/\n")
    _write(wdb / pdir / "RECORDS", b"88000001/88000001\n")
    rec = wdb / pdir / "88000001"
    _write(
        rec / "88000001.hea",
        (
            "#wfdb 10.7\n88000001/4 2 62.5/999.9 1250 23:59:58.500 31/12/2150\n"
            "88000001_0000 0\n88000001_0001 625\n~ 125\n88000001_0002 500\n"
            f"# subject_id {s[0]}\n# hadm_id 21000001\n"
        ).encode(),
    )
    _write(
        rec / "88000001_0000.hea",
        b"88000001_0000 2 62.5 0\n~ 1x1 200/mV 16 0 0 0 0 II\n~ 1x1 200/mV 16 0 0 0 0 V\n",
    )
    for seg, n in (("0001", 625), ("0002", 500)):
        _write(
            rec / f"88000001_{seg}.hea",
            (
                f"88000001_{seg} 2 62.5 {n}\n88000001_{seg}e.dat 516 200/mV 16 0 0 0 0 II\n"
                f"88000001_{seg}e.dat 516 200/mV 16 0 0 0 0 V\n"
            ).encode(),
        )
        _write(rec / f"88000001_{seg}e.dat", seg.encode() * 8)
    _write(rec / "88000001n.csv.gz", gzip.compress(b'"time","HR [bpm]"\n0,80\n'))

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
        "--root",
        str(SIMULATED),
        "--output-dir",
        str(out),
        "--extensions",
        EXTENSIONS,
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
    assert meta["dataset_name"] == "MIMIC-IV+Note+MNE[synthetic]+ECG+ECHO+CXR+CXR-DB[synthetic]+WDB"
    assert meta["dataset_version"].split(",") == [
        "mimic-iv-demo/2.2",
        "mimic-iv-note/2.2",
        "mimic-iv-ext-note-extract/1.0+synthetic-0.1",
        "mimic-iv-ecg-demo/0.1",
        "mimic-iv-echo/1.0.1",
        "mimic-cxr-jpg/2.1.0",
        "mimic-cxr-ext-mimic-cxr-db/1.0+synthetic-0.1",
        "mimic4wdb/0.1.0",
    ]
    assert meta["license"] == "PhysioNet Credentialed Health Data License 1.5.0"
    assert meta["additional_value_modality_columns"] == ["external_value_link"]
    assert "external_value_encoding" in meta["other_extension_columns"]
    assert {"note_id", "ecg_study_id", "echo_study_id", "cxr_study_id", "cxr_dicom_id"} <= set(
        meta["raw_source_id_columns"]
    )
    # The same releases, structured: version, demo or full, license and the parts built, per release.
    src = {d["module"]: d for d in meta["source_datasets"]}
    assert [d["extension"] for d in meta["source_datasets"]] == ["mimic-iv", *EXTENSIONS.split(",")]
    assert [d["module"] for d in meta["source_datasets"] if d["release"] == "synthetic"] == ["mne", "cxr_db"]
    assert not any("synthetic" in d for d in meta["source_datasets"])
    assert src["mne"]["release"] == "synthetic" and src["mne"]["url"] is None
    assert (src["core"]["name"], src["core"]["version"], src["core"]["release"]) == (
        "mimic-iv-demo",
        "2.2",
        "demo",
    )
    assert (src["ecg"]["name"], src["ecg"]["version"], src["ecg"]["license"]) == (
        "mimic-iv-ecg-demo",
        "0.1",
        "ODbL-1.0",
    )
    assert (
        src["echo"]["parts"] == ["dicom", "measurements"]
        and src["echo"]["external_value_encoding"] == "dicom"
    )
    assert src["waveform"]["parts"] == ["windows", "numerics"]
    assert [src[m]["external_value_encoding"] for m in ("ecg", "cxr", "waveform", "note")] == [
        "wfdb",
        "jpeg",
        "wfdb",
        None,
    ]
    assert src["note"]["parts"] == ["discharge", "radiology"]
    assert [f"{d['name']}/{d['version']}" for d in meta["source_datasets"]] == meta["dataset_version"].split(
        ","
    )


def test_parts_selection(synthetic_root, demo_raw_dir, tmp_path):
    """Radiology reports only, echo measurements without any DICOM, and ECGs."""
    root, _ = synthetic_root
    out = tmp_path / "meds"
    run_script(
        "mimic-iv-meds-multimodal", "--root", str(root), "--output-dir", str(out),
        "--modules", "echo:measurements,note:radiology,ecg", "--common-dir", str(demo_raw_dir), "--no-enrich",
    )  # fmt: skip
    meta = json.loads((out / "metadata" / "dataset.json").read_text())
    assert meta["dataset_name"] == "MIMIC-IV+Note(radiology)+ECG+ECHO(measurements)"
    assert meta["additional_value_modality_columns"] == ["external_value_link"]
    assert "echo_measurement_id" in meta["raw_source_id_columns"]
    src = {d["module"]: d for d in meta["source_datasets"]}
    assert (src["note"]["parts"], src["echo"]["parts"], src["echo"]["external_value_encoding"]) == (
        ["radiology"],
        ["measurements"],
        None,
    )
    codes = pl.scan_parquet(out / "data" / "**" / "*.parquet").select("code").collect()["code"]
    assert (
        not codes.str.starts_with("NOTE//DISCHARGE").any() and codes.str.starts_with("NOTE//RADIOLOGY").any()
    )
    assert (
        not codes.str.starts_with("ECHO//DICOM").any()
        and codes.str.starts_with("ECHO//MEASUREMENT//").sum() == 3
    )
    assert {p.name for p in (out / "external_values").iterdir()} == {"ecg"}


def test_ecg(built):
    out, s = built
    ev = _events(out, "ECG//")
    assert ev.select("subject_id", "ecg_study_id", pl.col("time").dt.strftime(TS)).rows() == [
        (s[0], 40000001, "2150-01-01 10:00:00"),
        (s[1], 40000002, "2150-01-02 10:00:00"),
    ]
    assert set(ev["code"]) == {"ECG//WAVEFORM"} and set(ev["external_value_encoding"]) == {"wfdb"}
    for link, study in ev.select("external_value_link", "ecg_study_id").rows():
        assert link.startswith("external_values/ecg/")
        # The record keeps its own name inside its content-addressed folder, so its header finds its signal.
        assert link.endswith(f"/{study}")
        assert (out / f"{link}.hea").read_text().split("\n")[1].startswith(f"{study}.dat")
        assert (out / f"{link}.dat").exists()


def test_echo(built):
    out, s = built
    ev = _events(out, "ECHO//DICOM")
    assert ev.select("subject_id", "echo_study_id", pl.col("time").dt.strftime(TS)).rows() == [
        (s[0], 90000001, "2150-02-01 08:30:00"),
        (s[2], 90000002, "2150-02-02 09:00:00"),
    ]
    assert set(ev["external_value_encoding"]) == {"dicom"}
    for link, study in ev.select("external_value_link", "echo_study_id").rows():
        assert link.startswith("external_values/echo/") and link.endswith(".dcm")
        assert (out / link).read_bytes().startswith(f"DICM{study}".encode())


def test_echo_measurements(built):
    out, s = built
    ev = _events(out, "ECHO//MEASUREMENT//")
    cols = [
        "subject_id",
        "code",
        "numeric_value",
        "text_value",
        "unit",
        "echo_test_type",
        "echo_measurement_id",
        "echo_study_id",
    ]
    assert ev.select(pl.col("time").dt.strftime(TS), *cols).rows() == [
        (
            "2150-02-01 09:00:00",
            s[0],
            "ECHO//MEASUREMENT//lv_size",
            None,
            "Normal (<5.6cm)",
            None,
            "TTE",
            7001,
            90000001,
        ),
        ("2150-02-01 09:00:00", s[0], "ECHO//MEASUREMENT//lvef", 55.0, None, "%", "TTE", 7001, 90000001),
        ("2150-02-05 12:00:00", s[2], "ECHO//MEASUREMENT//lvef", 60.5, None, "%", "TEE", 7002, None),
    ]
    codes = pl.read_parquet(out / "metadata" / "codes.parquet").filter(
        pl.col("code") == "ECHO//MEASUREMENT//lvef"
    )
    assert "LV ejection fraction" in codes["description"].item()


def test_waveform(built):
    out, s = built
    ev = _events(out, "WAVEFORM//WINDOW")
    cols = ["subject_id", "hadm_id", "waveform_record_id", "waveform_segment", "waveform_window"]
    # Both segments are shorter than a window, so each is one window, linked unchanged and timed at its
    # END: 23:59:58.5 + 625/62.5 s, and + (750 + 500)/62.5 s.
    assert ev.select(pl.col("time").dt.strftime(TS), *cols, "waveform_duration_s").rows() == [
        ("2151-01-01 00:00:08", s[0], 21000001, 88000001, "88000001_0001", 0, 10.0),
        ("2151-01-01 00:00:18", s[0], 21000001, 88000001, "88000001_0002", 0, 8.0),
    ]
    assert set(ev["external_value_encoding"]) == {"wfdb"}
    # Each window's folder holds that window and nothing else: no other segment, no master header,
    # no numerics.
    for link, seg in ev.select("external_value_link", "waveform_segment").rows():
        assert link.startswith("external_values/waveform/") and link.endswith(f"/{seg}")
        assert sorted(p.name for p in (out / link).parent.iterdir()) == [f"{seg}.hea", f"{seg}e.dat"]

    # The numerics are events: HR 80 at counter tick 0, in the first minute, timed at its end.
    vitals = _events(out, "WAVEFORM//NUMERIC//")
    assert vitals.select(
        pl.col("time").dt.strftime(TS), "subject_id", "code", "numeric_value", "unit", "waveform_record_id"
    ).rows() == [("2151-01-01 00:00:58", s[0], "WAVEFORM//NUMERIC//HR", 80.0, "bpm", 88000001)]

    sources = json.loads((out / "metadata" / "sources.json").read_text())
    staged = next(x for x in sources["sources"] if x["module"] == "waveform")["staging"]
    assert {
        "listed_patients": 3,
        "patients_outside_core_subjects": 1,
        "patients_not_on_disk": 1,
        "records": 1,
        "segments": 2,
        "segments_cut": 0,
        "window_events": 2,
        "numerics_files": 1,
        "numeric_events": 1,
    }.items() <= staged.items()


def test_cxr(built):
    out, s = built
    ev = _events(out, "CXR//IMAGE//")
    assert ev.select("subject_id", "code", "cxr_dicom_id", pl.col("time").dt.strftime(TS)).rows() == [
        (s[0], "CXR//IMAGE//PA//UNK", "aaaa-1", "2150-03-01 00:42:12"),
        (s[0], "CXR//IMAGE//UNK//UNK", "aaaa-2", "2150-03-01 10:42:12"),
        (s[1], "CXR//IMAGE//AP//UNK", "bbbb-1", "2150-03-02 23:59:59"),
    ]
    links = dict(ev.select("cxr_dicom_id", "external_value_link").rows())
    assert links["aaaa-1"] == links["bbbb-1"] != links["aaaa-2"]
    sha = hashlib.sha256(b"JPEG-A").hexdigest()
    assert links["aaaa-1"] == f"external_values/cxr/{sha[:2]}/{sha}.jpg"
    assert set(ev["external_value_encoding"]) == {"jpeg"}
    assert (out / links["aaaa-2"]).read_bytes() == b"JPEG-B"


def test_notes(built):
    out, s = built
    ev = _events(out, "NOTE//").filter(pl.col("note_id").is_not_null())
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


def test_external_values_and_sources(built, synthetic_root):
    out, _ = built
    root, _ = synthetic_root
    links = (
        pl.scan_parquet(out / "data" / "**" / "*.parquet")
        .filter(pl.col("external_value_link").is_not_null())
        .select("external_value_link", "external_value_encoding")
        .unique()
        .collect()
    )
    by_folder = links.group_by(pl.col("external_value_link").str.split("/").list.get(1).alias("m")).len()
    assert sorted(by_folder.rows()) == [("cxr", 2), ("ecg", 2), ("echo", 2), ("waveform", 2)]
    assert {p.name for p in (out / "external_values").iterdir()} == {"cxr", "ecg", "echo", "waveform"}
    assert not (out / "metadata" / "objects.parquet").exists()
    # Every file under external_values/ belongs to a link (the build checks this too).
    assert finalize.check_external_values(out)["links"] == 8
    # Single-file objects are the release's bytes, unchanged.
    cxr = root / "mimic-cxr-jpg" / "2.1.0" / "files"
    jpgs = {p.read_bytes() for p in cxr.rglob("*.jpg")}
    for link in links.filter(pl.col("external_value_encoding") == "jpeg")["external_value_link"]:
        assert (out / link).read_bytes() in jpgs

    sources = json.loads((out / "metadata" / "sources.json").read_text())
    assert sources["extensions"] == EXTENSIONS.split(",")
    assert "synthetic_extensions" not in sources
    assert [x["module"] for x in sources["sources"] if x["release"] == "synthetic"] == ["mne", "cxr_db"]
    assert "one point in time" in sources["loader_rule"]
    by = {x["module"]: x for x in sources["sources"]}
    assert {"listed": 3, "missing_on_disk": 1, "emitted": 2}.items() <= by["ecg"]["staging"].items()
    assert by["echo"]["staging"]["dicoms"]["outside_core_subjects"] == 1
    assert {"cohort_rows": 4, "empty_result": 1, "emitted": 3, "linked_to_study": 2}.items() <= by["echo"][
        "staging"
    ]["measurements"].items()
    assert by["cxr"]["staging"]["distinct_objects"] == 2
    assert by["cxr"]["objects"] == {"objects": 2, "files_written": 2, "bytes_written": 12}
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


def test_mne_and_cxr_db_from_the_simulated_releases(built):
    """MNE and CXR-DB events from `data/synthetic_demo`, restricted to the cohort, CXR-DB without images."""
    out, _ = built
    data = pl.scan_parquet(out / "data" / "**" / "*.parquet")
    subjects = set(pl.read_parquet(out / "metadata" / "subject_splits.parquet")["subject_id"])
    mne = data.filter(pl.col("code").str.starts_with("NOTE//") & pl.col("note_id").is_null()).collect()
    assert mne.height > 0 and set(mne["subject_id"]) <= subjects
    assert {"NOTE//ALLERGY", "NOTE//PHYSICAL_EXAM"} <= {"//".join(c.split("//")[:2]) for c in mne["code"]}
    cxr_db = data.filter(
        pl.col("code").str.starts_with("CXR//STUDY//") | pl.col("code").str.starts_with("CXR_NLP//")
    ).collect()
    assert cxr_db.height > 0 and set(cxr_db["subject_id"]) <= subjects
    assert cxr_db.filter(pl.col("code").str.starts_with("CXR//STUDY//"))["cxr_study_id"].null_count() == 0
    # Only MIMIC-CXR-JPG's image events (each with its link): CXR-DB's were left out.
    images = data.filter(pl.col("code").str.starts_with("CXR//IMAGE//")).collect()
    assert images["external_value_link"].null_count() == 0
