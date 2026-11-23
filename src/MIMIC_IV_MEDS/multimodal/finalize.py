"""After MEDS-Extract: put the objects inside the MEDS root and record where every one came from.

MEDS (0.4) defines `additional_value_modality_columns` but nothing about the files those columns
point at, and MEDS-Extract only writes tables. These steps are the part of a multimodal build that
has no home upstream yet; each is a self-contained function so it can move into MEDS-Extract as a
post-stage later:

- `place_objects`: each object goes to `<meds_root>/<link>` under `external_values/` (a WFDB
  record's `.hea`/`.dat` files into its own folder, keeping their names), by copy, hard link or
  symlink;
- `add_source_datasets`: `source_datasets` in `metadata/dataset.json`, every release used (project,
  version, demo, full or synthetic, license, parts, and the encoding of its linked objects);
- `write_sources`: `metadata/sources.json`, the same plus what staging kept or dropped from each
  release, as counts;
- `check_external_values`: every link in `data/` resolves to all of its files, and every file
  under `external_values/` belongs to a linked object (`external_values.check`).
"""

from __future__ import annotations

import json
import os
import re
import shutil
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path

import polars as pl

from . import compose, external_values
from .modules import FoundRelease
from .staging import Staged

LINK_MODES = ("copy", "hardlink", "symlink")

#: How a model may use the linked objects; written to `sources.json`.
LOADER_RULE = (
    "Every linked object is short enough to treat as one point in time (an ECG, an image, an echo DICOM, "
    "a waveform window of at most --waveform-window-s seconds) and its event is timed when its data is "
    "complete, so a link may be read from its event's `time` on. `waveform_duration_s` gives a waveform "
    "window's span, [time - duration, time]."
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
            # A multi-file record's link names the record, not a file: its files go next to it. A file the
            # build wrote itself (absolute: a waveform window, `root / rel` is then `rel`) is never symlinked,
            # since the build's work folder is not part of the MEDS root.
            target = dest.parent / Path(rel).name if len(row["files"]) > 1 else dest
            file_mode = "hardlink" if Path(rel).is_absolute() and mode == "symlink" else mode
            if _place_any(root / rel, target, file_mode):
                c["files_written"] += 1
                c["bytes_written"] += (root / rel).stat().st_size
    return counts


def _place_any(src: Path, dest: Path, mode: str) -> bool:
    """`_place`, falling back from a hard link to a copy across file systems."""
    try:
        return _place(src, dest, mode)
    except OSError:
        if mode != "hardlink":
            raise
        return _place(src, dest, "copy")


def source_dataset(f: FoundRelease, skip_tables: set[str] = frozenset()) -> dict:
    """One release the build used: what `dataset.json`'s `source_datasets` lists."""
    doc = {
        "extension": "mimic-iv" if f.module.name == "core" else f.module.names[0],
        "module": f.module.name,
        "name": f.release.project,
        "version": f.version,
        "release": "synthetic" if f.synthetic else "demo" if f.release.demo else "full",
        "license": f.license,
        "url": f"{f.release.url}{f.version}/",
        "parts": list(compose.active_parts(f, skip_tables)),
        "external_value_encoding": compose.encoding(f, skip_tables),
    }
    if f.synthetic:
        info = f.extras.get("synthetic", {})
        doc["url"] = None
        doc["simulates"] = info.get("simulates")
        doc["warning"] = info.get("warning")
    return doc


def auxiliary_sources() -> list[dict]:
    """The public concept maps the base spec joins (MIT-LCP mimic-code), with the tag they are pinned to.

    Examples:
        >>> aux = auxiliary_sources()
        >>> aux[0]["name"], aux[0]["version"].startswith("v"), len(aux[0]["files"]) > 5
        ('mimic-code', True, True)
    """
    out: dict[tuple[str, str], dict] = {}
    for entry in compose.common_sources():
        for item in entry.get("urls", []):
            m = re.search(r"github(?:usercontent)?\.com/[^/]+/([^/]+)/([^/]+)/", item["url"])
            name, ver = (m.group(1), m.group(2)) if m else (item["url"], None)
            doc = out.setdefault((name, ver), {"name": name, "version": ver, "files": []})
            doc["files"].append({"file": item["rel_path"], "sha256": item.get("sha256")})
    return list(out.values())


def write_sources(
    meds_root: Path,
    core: FoundRelease,
    found: list[FoundRelease],
    staged: dict[str, Staged],
    placed: dict,
    link_mode: str,
    skip_tables: set[str] = frozenset(),
) -> Path:
    def release(f: FoundRelease) -> dict:
        return {
            **source_dataset(f, skip_tables),
            "title": f.module.title,
            "parts_requested": list(f.parts),
            "staging": staged[f.module.name].stats if f.module.name in staged else {},
            "objects": placed.get(f.module.name),
        }

    doc = {
        "generated_by": f"MIMIC_IV_MEDS.multimodal {version('MIMIC-IV-MEDS')}",
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "extensions": [f.module.names[0] for f in found],
        "object_link_mode": link_mode,
        "loader_rule": LOADER_RULE,
        "notes": (
            "Releases whose `release` is `full` may be a subset of that release (e.g. only the MIMIC-IV "
            "demo subjects); `staging` counts what each contributed. A `synthetic` release is simulated "
            "data. external_value_link is relative to the MEDS root; external_value_encoding says how to "
            "read it."
        ),
        "sources": [release(core), *(release(f) for f in found)],
        "auxiliary_sources": auxiliary_sources(),
    }
    fp = meds_root / "metadata" / "sources.json"
    fp.write_text(json.dumps(doc, indent=2) + "\n")
    return fp


def add_source_datasets(
    meds_root: Path, core: FoundRelease, found: list[FoundRelease], skip_tables: set[str] = frozenset()
) -> dict:
    """Add `source_datasets` to `metadata/dataset.json`: every release used, with its version and parts.

    `dataset_version` already names each release as `<slug>/<version>` (MEDS-Extract writes it from the
    spec), but as one string. This adds the same in structured form, plus what MEDS has no field for:
    demo, full or synthetic, license per release, which parts of each module were built, and how its
    linked objects are encoded. MEDS's dataset
    metadata schema allows extra keys (suMEDS adds `sumeds_enrichment` the same way); a candidate for
    an `etl.source_datasets` option in MEDS-Extract.
    """
    fp = meds_root / "metadata" / "dataset.json"
    meta = json.loads(fp.read_text())
    meta["source_datasets"] = [
        source_dataset(core, skip_tables),
        *(source_dataset(f, skip_tables) for f in found),
    ]
    fp.write_text(json.dumps(meta) + "\n")
    return meta


def check_external_values(meds_root: Path) -> dict[str, int]:
    """Check every link in `data/` and the `external_values/` folder (`external_values.check`); counts."""
    data = pl.scan_parquet(meds_root / "data" / "**" / "*.parquet")
    link, enc = external_values.LINK_COLUMN, external_values.ENCODING_COLUMN
    names = set(data.collect_schema().names())
    if link not in names:
        links = {}
    elif enc not in names:
        raise ValueError(f"{link} is in data/ without {enc}")
    else:
        pairs = data.filter(pl.col(link).is_not_null()).select(link, enc).unique().collect()
        if pairs[enc].null_count() or pairs[link].n_unique() != pairs.height:
            raise ValueError(f"every {link} needs exactly one {enc}")
        links = dict(pairs.iter_rows())
    return external_values.check(meds_root, links)


def check_dataset_json(meds_root: Path, link_columns: list[str]) -> dict:
    """The links MEDS-Extract was asked to declare must be declared; returns dataset.json."""
    meta = json.loads((meds_root / "metadata" / "dataset.json").read_text())
    declared = set(meta.get("additional_value_modality_columns") or [])
    if missing := sorted(set(link_columns) - declared):
        raise ValueError(f"dataset.json does not list {missing} under additional_value_modality_columns")
    return meta
