"""After MEDS-Extract: put the objects inside the MEDS root and record where every one came from.

MEDS (0.4) defines `additional_value_modality_columns` but nothing about the files those columns
point at, and MEDS-Extract only writes tables. These steps are the part of a multimodal build that
has no home upstream yet; each is a self-contained function so it can move into MEDS-Extract as a
post-stage later:

- `place_objects`: each object goes to `<meds_root>/<link>` (an ECG record's `.hea`/`.dat` into
  its own folder, keeping their names), by copy, hard link or symlink;
- `write_objects_table`: `metadata/objects.parquet`, one row per source object: link, modality,
  format, sha256, size, the release and the path it came from, and its source IDs;
- `write_sources`: `metadata/sources.json`, every release the build used (project, version, demo
  or full, license) and what staging kept or dropped from each, as counts;
- `check_links`: every link in `data/` must be in `objects.parquet` and exist on disk.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import polars as pl

from .modules import FoundRelease
from .staging import OBJECT_SCHEMA, Staged

LINK_MODES = ("copy", "hardlink", "symlink")

#: How a model may use the link columns without seeing the future; written to `sources.json`.
LOADER_RULE = (
    "A link's data is available from its event's `time` on, never before, and a loader reads only the "
    "record or file the link names. Every object event is timed when its data is complete (a waveform "
    "segment or record at its end; `waveform_duration_s` gives the window [time - duration, time]). A "
    "waveform record folder also holds its later segments and its numerics: do not list or glob a link's "
    "folder; the whole record and its numerics are reachable only through WAVEFORM//RECORD, at the "
    "record's end."
)


def _place(src: Path, dest: Path, mode: str) -> bool:
    """Put `src` at `dest`; False when `dest` already holds it (a re-run, or a duplicate object)."""
    if dest.is_symlink() or dest.exists():
        same = (
            dest.resolve() == src.resolve()
            if dest.is_symlink()
            else dest.stat().st_size == src.stat().st_size
        )
        if same and (mode == "symlink") == dest.is_symlink():
            return False
        dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    if mode == "symlink":
        dest.symlink_to(src.resolve())
    elif mode == "hardlink":
        os.link(src, dest)
    else:
        tmp = dest.with_name(dest.name + ".partial")
        shutil.copy2(src, tmp)
        tmp.replace(dest)
    return True


def place_objects(objects: pl.DataFrame, roots: dict[str, Path], meds_root: Path, mode: str) -> dict:
    """Place every object of `objects` (the staged manifest) under `meds_root`; returns counts per
    modality."""
    if mode not in LINK_MODES:
        raise ValueError(f"link mode must be one of {LINK_MODES}, got {mode!r}")
    counts: dict[str, dict[str, int]] = {}
    for row in objects.unique("link", keep="first", maintain_order=True).iter_rows(named=True):
        root = roots[row["modality"]]
        c = counts.setdefault(row["modality"], {"objects": 0, "files_written": 0, "bytes_written": 0})
        c["objects"] += 1
        dest = meds_root / row["link"]
        for rel in row["files"]:
            # A multi-file record's link names the record, not a file: its files go next to it. Objects can
            # share files (a waveform segment's files are also its record's): each is placed once.
            target = dest.parent / Path(rel).name if len(row["files"]) > 1 else dest
            if _place(root / rel, target, mode):
                c["files_written"] += 1
                c["bytes_written"] += (root / rel).stat().st_size
    return counts


def write_objects_table(objects: pl.DataFrame, meds_root: Path) -> Path:
    fp = meds_root / "metadata" / "objects.parquet"
    fp.parent.mkdir(parents=True, exist_ok=True)
    objects.cast(OBJECT_SCHEMA).sort("modality", "subject_id", "link").write_parquet(fp)
    return fp


def write_sources(
    meds_root: Path,
    core: FoundRelease,
    found: list[FoundRelease],
    staged: dict[str, Staged],
    placed: dict,
    link_mode: str,
) -> Path:
    def release(f: FoundRelease) -> dict:
        return {
            "module": f.module.name,
            "title": f.module.title,
            "project": f.release.project,
            "version": f.version,
            "url": f"{f.release.url}{f.version}/",
            "release": "demo" if f.release.demo else "full",
            "license": f.release.license,
            "link_column": f.module.link_column,
            "staging": staged[f.module.name].stats if f.module.name in staged else {},
            "objects": placed.get(f.module.name),
        }

    doc = {
        "generated_by": f"MIMIC_IV_MEDS.multimodal {version('MIMIC-IV-MEDS')}",
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "modules": [core.module.name, *(f.module.name for f in found)],
        "object_link_mode": link_mode,
        "loader_rule": LOADER_RULE,
        "notes": (
            "Releases whose `release` is `full` may be a subset of that release (e.g. only the MIMIC-IV "
            "demo subjects); `staging` counts what each contributed. Object links are relative to the "
            "MEDS root; metadata/objects.parquet maps each to its source file and SHA-256."
        ),
        "sources": [release(core), *(release(f) for f in found)],
    }
    fp = meds_root / "metadata" / "sources.json"
    fp.write_text(json.dumps(doc, indent=2) + "\n")
    return fp


def check_links(meds_root: Path, link_columns: list[str]) -> dict[str, int]:
    """Count the links in `data/` per column; raise if any is unknown to objects.parquet or missing on
    disk."""
    known = set()
    objects_fp = meds_root / "metadata" / "objects.parquet"
    if objects_fp.exists():
        known = set(pl.read_parquet(objects_fp, columns=["link"])["link"])
    data = pl.scan_parquet(meds_root / "data" / "**" / "*.parquet")
    present = set(data.collect_schema().names())
    counts = {}
    for col in link_columns:
        if col not in present:
            raise ValueError(f"{col} is declared but not in data/")
        links = data.select(pl.col(col).drop_nulls().unique()).collect()[col].to_list()
        unknown = [x for x in links if x not in known]
        # An ECG link names a WFDB record (no extension); its header is `<link>.hea`.
        missing = [x for x in links if not ((meds_root / x).exists() or (meds_root / f"{x}.hea").exists())]
        if unknown or missing:
            raise ValueError(
                f"{col}: {len(unknown)} links not in objects.parquet, {len(missing)} not on disk"
            )
        counts[col] = len(links)
    return counts


def check_dataset_json(meds_root: Path, link_columns: list[str]) -> dict:
    """The links MEDS-Extract was asked to declare must be declared; returns dataset.json."""
    meta = json.loads((meds_root / "metadata" / "dataset.json").read_text())
    declared = set(meta.get("additional_value_modality_columns") or [])
    if missing := sorted(set(link_columns) - declared):
        raise ValueError(f"dataset.json does not list {missing} under additional_value_modality_columns")
    return meta
