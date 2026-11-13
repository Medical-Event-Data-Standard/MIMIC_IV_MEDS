"""Unit tests of the multimodal build that need no data: release discovery, spec composition, placement.

The end-to-end build (MEDS-Extract included) is in `e2e_multimodal_synthetic_test.py`.
"""

import doctest
import itertools
from pathlib import Path

import polars as pl
import pytest

from MIMIC_IV_MEDS.multimodal import compose, finalize, modules, staging
from MIMIC_IV_MEDS.multimodal.modules import CORE, MODULES, ODBL, PHYSIONET_CREDENTIALED, find_release


@pytest.mark.parametrize("mod", [modules, staging, compose, finalize], ids=lambda m: m.__name__)
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


def test_find_release_prefers_full_and_newest(tmp_path):
    _mk(tmp_path, "mimic-iv-ed-demo-2.2", "ed/edstays.csv.gz")
    _mk(tmp_path, "mimic-iv-ed/2.0", "ed/edstays.csv.gz")
    _mk(tmp_path, "mimic-iv-ed/2.2", "ed/edstays.csv.gz")
    _mk(tmp_path, "mimic-iv-ed/3.0", "not-ed.txt")  # no marker: ignored
    assert find_release(MODULES["ed"], tmp_path).tag == "mimic-iv-ed/2.2"
    assert find_release(MODULES["ed"], tmp_path, version="2.0").tag == "mimic-iv-ed/2.0"


def test_find_release_explicit_path_and_errors(tmp_path):
    d = _mk(tmp_path, "somewhere/echo-files", "echo-record-list.csv")
    with pytest.raises(ValueError, match="--version echo"):
        find_release(MODULES["echo"], tmp_path, path=d)
    assert find_release(MODULES["echo"], tmp_path, path=d, version="1.0.1").tag == "mimic-iv-echo/1.0.1"
    with pytest.raises(FileNotFoundError, match="--path cxr="):
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
    links = [MODULES[m].link_column for m in combo if MODULES[m].link_column]
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
