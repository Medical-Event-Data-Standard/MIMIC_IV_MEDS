"""Unit tests of the multimodal build that need no data: release discovery, spec composition, placement.

The end-to-end build (MEDS-Extract included) is in `e2e_multimodal_synthetic_test.py`.
"""

import doctest
import gzip
import itertools
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from MIMIC_IV_MEDS.multimodal import compose, external_values, finalize, modules, staging
from MIMIC_IV_MEDS.multimodal.modules import CORE, MODULES, ODBL, PHYSIONET_CREDENTIALED, find_release


@pytest.mark.parametrize(
    "mod", [modules, staging, compose, finalize, external_values], ids=lambda m: m.__name__
)
def test_doctests(mod):
    result = doctest.testmod(mod)
    assert result.failed == 0


def _mk(root: Path, rel: str, marker: str) -> Path:
    d = root / rel
    (d / marker).parent.mkdir(parents=True, exist_ok=True)
    (d / marker).touch()
    return d


def test_find_release_layouts(tmp_path):
    # ZIP-style names with title words, and wget-style <slug>/<version>/ under physionet.org/files.
    core = _mk(tmp_path, "mimic-iv-clinical-database-demo-2.2", "hosp/patients.csv.gz")
    ecg = _mk(
        tmp_path, "mimic-iv-ecg-demo-diagnostic-electrocardiogram-matched-subset-demo-0.1", "record_list.csv"
    )
    (ecg / "files").mkdir()
    note = _mk(tmp_path, "physionet.org/files/mimic-iv-note/2.2", "note/radiology.csv.gz")
    _mk(tmp_path, "mimic-iv-ed-demo-2.2", "ed/edstays.csv.gz")

    f = find_release(CORE, tmp_path)
    assert (f.root, f.release.project, f.version, f.release.demo) == (core, "mimic-iv-demo", "2.2", True)
    f = find_release(MODULES["ecg"], tmp_path)
    assert (f.root, f.tag, f.release.license) == (ecg, "mimic-iv-ecg-demo/0.1", ODBL)
    f = find_release(MODULES["note"], tmp_path)
    assert (f.root, f.tag, f.release.credentialed) == (note, "mimic-iv-note/2.2", True)
    # The ED demo folder must not be taken for the full ED release, but is found as the demo.
    assert find_release(MODULES["ed"], tmp_path).tag == "mimic-iv-ed-demo/2.2"


def test_find_release_demo_downloader_layout(tmp_path):
    """What `mimic-demo-mm download` writes: `<slug>-<version>/`, plus a tables-only full ECG release."""
    root = tmp_path / "mimic-demo-multimodal"
    _mk(root, "mimic-iv-demo-2.2", "hosp/patients.csv.gz")
    _mk(root, "mimic-iv-ed-demo-2.2", "ed/edstays.csv.gz")
    ecg_demo = _mk(root, "mimic-iv-ecg-demo-0.1", "record_list.csv")
    (ecg_demo / "files").mkdir()
    _mk(root, "mimic-iv-ecg-1.0", "record_list.csv")  # filtered tables, no files/: not the waveforms
    _mk(root, "mimic4wdb-0.1.0", "waves/RECORDS")
    _mk(root, "mimic-cxr-jpg-2.1.0", "mimic-cxr-2.0.0-metadata.csv.gz")
    _mk(root, "mimic-iv-echo-1.0.1", "echo-record-list.csv")
    _mk(root, "mimic-iv-note-2.2", "note/discharge.csv.gz")

    assert find_release(CORE, root).tag == "mimic-iv-demo/2.2"
    tags = {m: find_release(MODULES[m], root).tag for m in ("ed", "ecg", "waveform", "cxr", "echo", "note")}
    assert tags == {
        "ed": "mimic-iv-ed-demo/2.2",
        "ecg": "mimic-iv-ecg-demo/0.1",
        "waveform": "mimic4wdb/0.1.0",
        "cxr": "mimic-cxr-jpg/2.1.0",
        "echo": "mimic-iv-echo/1.0.1",
        "note": "mimic-iv-note/2.2",
    }


def test_find_release_synthetic_fallback_and_several_roots(tmp_path):
    """A simulated release (SYNTHETIC_<slug>.json) is used only when no real one is in any root."""
    import json

    real, synth = tmp_path / "real", tmp_path / "synthetic_demo"
    _mk(real, "mimic-iv-note-2.2", "note/discharge.csv.gz")
    for slug, marker in (
        ("mimic-iv-note", "note/discharge.csv.gz"),
        ("mimic-iv-ext-note-extract", "allergies.csv.gz"),
    ):
        d = _mk(synth, slug, marker)
        info = {"synthetic": True, "dataset_version": "1.0+synthetic-0.1", "license": ODBL, "simulates": slug}
        (d / f"SYNTHETIC_{slug}.json").write_text(json.dumps(info))

    note = find_release(MODULES["note"], [synth, real])
    assert (note.tag, note.synthetic) == ("mimic-iv-note/2.2", False)
    mne = find_release(MODULES["mne"], [real, synth])
    assert (mne.tag, mne.synthetic, mne.license, mne.credentialed) == (
        "mimic-iv-ext-note-extract/1.0+synthetic-0.1",
        True,
        ODBL,
        False,
    )
    assert mne.label == "MNE[synthetic]"
    spec = compose.compose(_found("core", demo=True), [mne])
    assert spec["etl"]["license"] == ODBL  # nothing credentialed went in
    assert "[synthetic]" in spec["etl"]["dataset_name"]
    meta = finalize.source_dataset(mne)
    # Simulated is a kind of release, not a separate flag (a dataset-level flag is meds#111's topic).
    assert (meta["extension"], meta["release"], meta["url"]) == ("mimic-iv-mne", "synthetic", None)
    assert "synthetic" not in meta


def test_cxr_db_yields_its_images_to_cxr_jpg():
    """Both CXR releases in one build: one image event per image (CXR-JPG's, with the link)."""
    core = _found("core", demo=True)
    alone = compose.compose(core, [_found("cxr_db")])
    assert "image" in alone["cxr_db/radiographs"]
    both = compose.compose(core, [_found("cxr"), _found("cxr_db")])
    assert "image" not in both["cxr_db/radiographs"]
    assert {"study", "chexpert_edema"} <= set(both["cxr_db/radiographs"])
    assert "image" in both["cxr/images"]
    assert both["etl"]["dataset_name"] == "MIMIC-IV+CXR+CXR-DB"
    # Same id columns in both, declared once.
    raw = both["etl"]["raw_source_id_columns"]
    assert raw.count("cxr_study_id") == raw.count("cxr_dicom_id") == 1


def test_find_release_prefers_full_and_newest(tmp_path):
    _mk(tmp_path, "mimic-iv-ed-demo-2.2", "ed/edstays.csv.gz")
    _mk(tmp_path, "mimic-iv-ed/2.0", "ed/edstays.csv.gz")
    _mk(tmp_path, "mimic-iv-ed/2.2", "ed/edstays.csv.gz")
    _mk(tmp_path, "mimic-iv-ed/3.0", "not-ed.txt")  # no marker: ignored
    assert find_release(MODULES["ed"], tmp_path).tag == "mimic-iv-ed/2.2"
    assert find_release(MODULES["ed"], tmp_path, version="2.0").tag == "mimic-iv-ed/2.0"


def test_find_release_explicit_path_and_errors(tmp_path):
    d = _mk(tmp_path, "somewhere/echo-files", "echo-record-list.csv")
    with pytest.raises(ValueError, match="--version mimic-iv-echo"):
        find_release(MODULES["echo"], tmp_path, path=d)
    assert find_release(MODULES["echo"], tmp_path, path=d, version="1.0.1").tag == "mimic-iv-echo/1.0.1"
    with pytest.raises(FileNotFoundError, match="--path mimic-iv-cxr="):
        find_release(MODULES["cxr"], tmp_path)


def _found(name: str, demo: bool = False) -> modules.FoundRelease:
    m = CORE if name == "core" else MODULES[name]
    release = (
        next(r for r in m.releases if r.demo == demo)
        if any(r.demo == demo for r in m.releases)
        else m.releases[0]
    )
    return modules.FoundRelease(m, release, "9.9", Path("/nowhere"))


@pytest.mark.parametrize(
    "combo", [c for n in range(len(MODULES) + 1) for c in itertools.combinations(MODULES, n)], ids=",".join
)
def test_compose_every_combination(combo):
    core = _found("core", demo=True)
    found = [_found(m, demo=True) for m in combo]
    spec = compose.compose(core, found)
    base = compose.base_spec()

    # Every base table, unchanged, plus the fragments' tables; no sources: the build stages input itself.
    assert "sources" not in spec
    for k, v in base.items():
        if k not in ("etl", "sources"):
            assert spec[k] == v
    for m in combo:
        assert set(compose.fragment(m)) <= set(spec)

    etl = spec["etl"]
    assert etl["dataset_name"] == "+".join(["MIMIC-IV", *(MODULES[m].label for m in combo)])
    assert etl["raw_dataset_version"].split(",") == [f.tag for f in [core, *found]]
    # One link column for every object module, whichever are built.
    links = ["external_value_link"] if {"ecg", "echo", "cxr", "waveform"} & set(combo) else []
    assert etl.get("additional_value_modality_columns", []) == links
    credentialed = any(f.release.credentialed for f in found)
    assert etl["license"] == (PHYSIONET_CREDENTIALED if credentialed else ODBL)
    assert (
        etl["raw_source_id_columns"][: len(base["etl"]["raw_source_id_columns"])]
        == base["etl"]["raw_source_id_columns"]
    )


def test_place_objects_modes_and_reruns(tmp_path):
    src = tmp_path / "release"
    (src / "a").mkdir(parents=True)
    (src / "a" / "r.hea").write_text("r 1 500 10\nr.dat 16 200 16 0 0 0 0 I\n")
    (src / "a" / "r.dat").write_bytes(b"\x00\x01")
    (src / "a" / "x.jpg").write_bytes(b"jpg")
    objects = pl.DataFrame(
        {
            "link": ["ecg/ab/abcd/r", "cxr/cd/cdef.jpg", "cxr/cd/cdef.jpg"],
            "modality": ["ecg", "cxr", "cxr"],
            "size_bytes": [10, 3, 3],
            "files": [["a/r.hea", "a/r.dat"], ["a/x.jpg"], ["a/x.jpg"]],
        }
    )
    for mode in finalize.LINK_MODES:
        out = tmp_path / mode
        counts = finalize.place_objects(objects, {"ecg": src, "cxr": src}, out, mode)
        # The duplicate link is placed once; the record keeps its file names next to its link.
        assert counts == {
            "ecg": {
                "objects": 1,
                "files_written": 2,
                "bytes_written": (src / "a" / "r.hea").stat().st_size + 2,
            },
            "cxr": {"objects": 1, "files_written": 1, "bytes_written": 3},
        }
        assert (out / "ecg/ab/abcd/r.hea").read_text().startswith("r ")
        assert (out / "cxr/cd/cdef.jpg").read_bytes() == b"jpg"
        assert (out / "cxr/cd/cdef.jpg").is_symlink() == (mode == "symlink")
        again = finalize.place_objects(objects, {"ecg": src, "cxr": src}, out, mode)
        assert again["ecg"]["files_written"] == again["cxr"]["files_written"] == 0


def test_stage_ed_filters_to_the_cohort_and_keeps_empty_cells_null(tmp_path):
    """The full ED release next to the demo core: only the cohort's rows, written so types infer as raw."""
    import gzip

    ed = tmp_path / "mimic-iv-ed" / "2.2" / "ed"
    ed.mkdir(parents=True)
    with gzip.open(ed / "edstays.csv.gz", "wt") as f:
        f.write("subject_id,stay_id,intime\n1,11,2150-01-01 00:00:00\n2,22,2150-01-02 00:00:00\n")
    with gzip.open(ed / "triage.csv.gz", "wt") as f:
        f.write('subject_id,stay_id,temperature,chiefcomplaint\n1,11,,Fever\n2,22,98.6,"Pain, chest"\n')
    found = modules.FoundRelease(MODULES["ed"], MODULES["ed"].releases[1], "2.2", ed.parent)
    raw = tmp_path / "raw"
    staged = staging.stage_ed(found, raw, pl.Series([2]))
    assert staged.stats["filtered"] and staged.stats["stays_outside_core_subjects"] == 1
    assert not (raw / "ed").is_symlink()
    triage = pl.read_csv(raw / "ed" / "triage.csv.gz")
    assert triage.rows() == [(2, 22, 98.6, "Pain, chest")]
    assert triage.schema["temperature"] == pl.Float64  # numeric, as in the raw file

    # Nothing outside the cohort: the folder is linked in unchanged.
    raw2 = tmp_path / "raw2"
    assert not staging.stage_ed(found, raw2, pl.Series([1, 2])).stats["filtered"]
    assert (raw2 / "ed").is_symlink()


@pytest.mark.parametrize(
    "selection, skip, name, links, raw_extra, absent",
    [
        # Only the measurements: no DICOM table, no link, but the measurement ids.
        (
            "echo:measurements",
            set(),
            "MIMIC-IV+ECHO(measurements)",
            [],
            ["echo_measurement_id", "echo_study_id"],
            ["echo/records"],
        ),
        # Both parts asked for by name is the whole module: plain label.
        (
            "echo:dicom+measurements",
            set(),
            "MIMIC-IV+ECHO",
            ["external_value_link"],
            ["echo_study_id", "echo_measurement_id"],
            [],
        ),
        # Default (all parts) but the measurement table was not found: its columns are not declared.
        (
            "echo",
            {"echo/measurements"},
            "MIMIC-IV+ECHO",
            ["external_value_link"],
            ["echo_study_id"],
            ["echo/measurements"],
        ),
        (
            "note:radiology,ecg",
            set(),
            "MIMIC-IV+Note(radiology)+ECG",
            ["external_value_link"],
            ["note_id", "ecg_study_id"],
            [],
        ),
        # The waveform database's vital signs alone link nothing; its windows alone do.
        (
            "mimic-iv-wdb:numerics",
            set(),
            "MIMIC-IV+WDB(numerics)",
            [],
            ["waveform_record_id"],
            ["waveform/windows"],
        ),
        (
            "mimic-iv-wdb:windows",
            set(),
            "MIMIC-IV+WDB(windows)",
            ["external_value_link"],
            ["waveform_record_id"],
            ["waveform/numerics"],
        ),
    ],
)
def test_compose_parts(selection, skip, name, links, raw_extra, absent):
    core = _found("core", demo=True)
    found = [
        modules.FoundRelease(MODULES[m], _found(m).release, "9.9", Path("/nowhere"), parts, explicit)
        for m, (parts, explicit) in modules.parse_selection(selection).items()
    ]
    spec = compose.compose(core, found, skip)
    etl = spec["etl"]
    assert etl["dataset_name"] == name
    assert etl.get("additional_value_modality_columns", []) == links
    assert (
        etl["raw_source_id_columns"][len(compose.base_spec()["etl"]["raw_source_id_columns"]) :] == raw_extra
    )
    for table in absent:
        assert table not in spec


def test_parse_selection_errors():
    with pytest.raises(ValueError, match="unknown extension"):
        modules.parse_selection("mimic-iv-ecg,mimic-iv-mri")
    with pytest.raises(ValueError, match="all-or-nothing"):
        modules.parse_selection("mimic-iv-cxr:labels")


def test_every_extension_name_resolves():
    """Canonical `mimic-iv-<x>` names, PhysioNet slugs and short names all select the same module."""
    for m in MODULES.values():
        assert m.names[0].startswith("mimic-iv-")
        for n in m.names:
            assert list(modules.parse_selection(n)) == [m.name]
    assert list(modules.parse_selection("mimic-iv-mne, mimic-iv-cxr")) == ["mne", "cxr"]


def test_stage_common_reuses_the_cache(tmp_path):
    """A concept map already in the cache (with the right SHA-256) is linked, not downloaded again."""
    import hashlib

    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "map.csv").write_text("a,b\n")
    entry = {
        "urls": [
            {
                "url": "http://invalid.invalid/map.csv",  # would fail if fetched
                "rel_path": "map.csv",
                "sha256": hashlib.sha256(b"a,b\n").hexdigest(),
            }
        ]
    }
    staging.stage_common(tmp_path / "raw", [entry], [], cache)
    assert (tmp_path / "raw" / "map.csv").read_text() == "a,b\n"


def test_download_demo_asks_mimic_demo_mm_for_the_selected_sources(tmp_path, monkeypatch):
    """`--download-demo` runs `mimic-demo-mm download` for the core plus the extensions' demo sources."""
    import importlib.util
    import subprocess

    from MIMIC_IV_MEDS.multimodal import cli

    calls = []
    monkeypatch.setattr(importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(
        subprocess, "run", lambda args, **kw: calls.append(args) or subprocess.CompletedProcess(args, 0)
    )
    sel = list(modules.parse_selection("mimic-iv-ed,mimic-iv-mne,mimic-iv-cxr,mimic-iv-wdb"))
    cli.download_demo(tmp_path, sel, yes=True)
    [args] = calls
    assert args[1:6] == ["-m", "mimic_demo_mm", "download", "--out", str(tmp_path)]
    only = args[args.index("--only") + 1 : args.index("--yes")]
    # MNE has no demo subset: nothing to fetch for it (the build warns).
    assert only == ["mimic-iv-demo", "mimic-iv-ed-demo", "mimic-cxr-jpg", "mimic4wdb"]

    monkeypatch.setattr(importlib.util, "find_spec", lambda name: None)
    with pytest.raises(SystemExit, match=r"demo-download"):
        cli.download_demo(tmp_path, sel, yes=False)


def test_cut_wfdb_windows_is_lossless(tmp_path):
    """A multi-rate, two-file, FLAC-compressed segment cut into windows keeps every sample and setting."""
    wfdb = pytest.importorskip("wfdb")
    import numpy as np

    rng = np.random.default_rng(0)
    frames, spf = 250, [4, 1, 1]  # a 4x signal in one file, two 1x signals in another
    sig = [rng.integers(-2000, 2000, frames * k).astype(np.int64) for k in spf]
    rec = wfdb.Record(
        record_name="seg_0001", n_sig=3, fs=62.5, sig_len=frames, e_d_signal=sig,
        file_name=["seg_0001e.dat", "seg_0001f.dat", "seg_0001f.dat"], fmt=["516"] * 3,
        samps_per_frame=spf, adc_gain=[200.0, 100.0, 1.0], baseline=[0, 0, 0], units=["mV", "mV", "NU"],
        sig_name=["II", "V", "Pleth"], adc_res=[16] * 3, adc_zero=[0] * 3,
        init_value=[int(s[0]) for s in sig], checksum=[0] * 3, block_size=[0] * 3,
    )  # fmt: skip
    src = tmp_path / "src"
    src.mkdir()
    rec.wrsamp(write_dir=str(src), expanded=True)

    start = datetime(2150, 1, 1, 0, 0, 0)
    windows = staging.cut_wfdb_windows(src / "seg_0001", tmp_path / "out", 100, start)
    assert [(n, s, e) for n, s, e, _ in windows] == [
        ("seg_0001_w0000", 0, 100),
        ("seg_0001_w0001", 100, 200),
        ("seg_0001_w0002", 200, 250),
    ]
    # Signal groups keep their suffixes; each window is a header plus its own two signal files.
    assert [p.name for p in windows[1][3]] == [
        "seg_0001_w0001.hea",
        "seg_0001_w0001e.dat",
        "seg_0001_w0001f.dat",
    ]
    for name, s, e, _ in windows:
        w = wfdb.rdrecord(str(tmp_path / "out" / name), physical=False, smooth_frames=False)
        assert (w.sig_len, list(w.fmt), list(w.samps_per_frame), list(w.adc_gain)) == (
            e - s, ["516"] * 3, spf, [200.0, 100.0, 1.0],
        )  # fmt: skip
        for got, full, k in zip(w.e_d_signal, sig, spf, strict=True):
            assert np.array_equal(got, full[s * k : e * k])
        assert datetime.combine(w.base_date, w.base_time) == start + timedelta(seconds=s / 62.5)


def test_read_wfdb_numerics_times_each_summary_at_the_end_of_its_interval(tmp_path):
    m = staging.parse_wfdb_master(
        "#wfdb 10.7\n8000/3 2 62.5/1000 6250 00:00:00 01/01/2150\n8000_0001 6250\n# hadm_id 1\n"
    )
    fp = tmp_path / "8000n.csv.gz"
    # Ticks of a 1 kHz counter: readings at 0.5 s, 59.5 s, 60.5 s and 90 s; one is not a number.
    fp.write_bytes(
        gzip.compress(
            b'"time","HR [bpm]","SpO2 [%]","Note"\n500,80,97,\n59500,90,,\n60500,70,98,x\n90000,abc,99,\n'
        )
    )
    per_minute = staging.read_wfdb_numerics(fp, m, 60.0)
    t0, td = datetime(2150, 1, 1), timedelta
    assert per_minute.rows() == [
        (t0 + td(seconds=60), "WAVEFORM//NUMERIC//HR", 85.0, "bpm"),
        (t0 + td(seconds=60), "WAVEFORM//NUMERIC//SpO2", 97.0, "%"),
        (t0 + td(seconds=120), "WAVEFORM//NUMERIC//HR", 70.0, "bpm"),
        (t0 + td(seconds=120), "WAVEFORM//NUMERIC//SpO2", 98.5, "%"),
    ]
    every = staging.read_wfdb_numerics(fp, m, 0)
    assert every.height == 6 and every["time"].min() == t0 + td(milliseconds=500)


def test_external_values_check(tmp_path):
    rec = tmp_path / "external_values/ecg/ab/abcd"
    rec.mkdir(parents=True)
    (rec / "r.hea").write_text("r 1 500 10\nr.dat 16 200 16 0 0 0 0 I\n")
    (rec / "r.dat").write_bytes(b"..")
    (tmp_path / "external_values/cxr").mkdir(parents=True)
    (tmp_path / "external_values/cxr/x.jpg").write_bytes(b"jpg")
    links = {"external_values/ecg/ab/abcd/r": "wfdb", "external_values/cxr/x.jpg": "jpeg"}
    assert external_values.check(tmp_path, links) == {"links": 2, "files": 3}

    (tmp_path / "external_values/cxr/stray.jpg").write_bytes(b"jpg")
    with pytest.raises(ValueError, match="belong to no linked object"):
        external_values.check(tmp_path, links)
    (tmp_path / "external_values/cxr/stray.jpg").unlink()
    (rec / "r.dat").unlink()
    with pytest.raises(ValueError, match="missing"):
        external_values.check(tmp_path, links)
    for bad in ("../x.jpg", "cxr/x.jpg", "/abs/x.jpg"):
        with pytest.raises(ValueError, match="relative paths under external_values"):
            external_values.check(tmp_path, {bad: "jpeg"})
    with pytest.raises(ValueError, match="unknown encoding"):
        external_values.check(tmp_path, {"external_values/cxr/x.jpg": "png?"})


def test_place_objects_never_symlinks_files_the_build_wrote(tmp_path):
    generated = tmp_path / "work" / "w.hea"
    generated.parent.mkdir()
    generated.write_text("w 1 1 1\n")
    objects = pl.DataFrame(
        {"link": ["external_values/waveform/ab/abcd/w"], "modality": ["waveform"], "size_bytes": [8],
         "files": [[generated.as_posix()]]}
    )  # fmt: skip
    finalize.place_objects(objects, {"waveform": tmp_path / "release"}, tmp_path / "meds", "symlink")
    placed = tmp_path / "meds/external_values/waveform/ab/abcd/w"
    assert placed.read_text() == "w 1 1 1\n" and not placed.is_symlink()
