"""External values: data an event links to instead of holding (an ECG, a DICOM, an image, a waveform).

The convention follows the discussion in Medical-Event-Data-Standard/meds#109:

- **Two columns.** `external_value_link` holds the object's location and `external_value_encoding`
  how to read it, from a fixed set (`ENCODINGS`). The modality is in the event's `code`
  (`ECG//…`, `CXR//IMAGE//…`); one event links to at most one object. The link column is declared
  in `dataset.json`'s `additional_value_modality_columns`; MEDS-Extract files the encoding column
  under `other_extension_columns`.
- **One folder.** Every object lives under `$MEDS_ROOT/external_values/`, and the link is a POSIX
  path relative to the MEDS root. Every file in that folder belongs to an object some event links to
  (`check`). How the folder is organized below that is the ETL's choice: this build uses
  `external_values/<extension>/<sha[:2]>/<sha>…`, so identical objects share one link.
- **Point-in-time objects.** A linked object should be short enough to treat as happening at one
  time; its event is timed when its data is complete. Long recordings are cut into windows first
  (the waveform database: see `staging.stage_waveform`).

`read` shows how each encoding is opened; its readers are optional dependencies (the
`external-values` extra).
"""

from __future__ import annotations

import importlib
from pathlib import Path

ROOT = "external_values"
LINK_COLUMN = "external_value_link"
ENCODING_COLUMN = "external_value_encoding"

#: Encoding -> what the link names, and how to read it (`root` is the MEDS root).
ENCODINGS: dict[str, dict[str, str]] = {
    "wfdb": {
        "names": "a WFDB record: `<link>.hea` plus the signal files it names, in the same folder",
        "read": "wfdb.rdrecord(root / link)",
        "module": "wfdb",
    },
    "dicom": {
        "names": "one DICOM file",
        "read": "pydicom.dcmread(root / link)",
        "module": "pydicom",
    },
    "jpeg": {
        "names": "one JPEG image",
        "read": "PIL.Image.open(root / link)",
        "module": "PIL",
    },
}


def files_of(meds_root: Path, link: str, encoding: str) -> list[Path]:
    """The files that make up the object `link` names.

    Examples:
        >>> import tempfile
        >>> root = Path(tempfile.mkdtemp())
        >>> rec = root / "external_values/ecg/ab/abcd"
        >>> rec.mkdir(parents=True)
        >>> _ = (rec / "r.hea").write_text("r 1 500 10\\nr.dat 16 200 16 0 0 0 0 I\\n")
        >>> _ = (rec / "r.dat").write_bytes(b"..")
        >>> [p.name for p in files_of(root, "external_values/ecg/ab/abcd/r", "wfdb")]
        ['r.hea', 'r.dat']
    """
    path = meds_root / link
    if encoding != "wfdb":
        return [path]
    header = path.with_name(path.name + ".hea")
    signal_files = []
    for line in header.read_text().splitlines()[1:]:
        tok = line.split()
        if tok and not line.startswith("#") and tok[0] != "~" and tok[0] not in signal_files:
            signal_files.append(tok[0])
    return [header, *(path.parent / f for f in signal_files)]


def read(meds_root: Path, link: str, encoding: str):
    """Open one external value: a `wfdb.Record`, a `pydicom.Dataset` or a `PIL.Image.Image`."""
    if encoding not in ENCODINGS:
        raise ValueError(f"unknown encoding {encoding!r}; known: {sorted(ENCODINGS)}")
    try:
        mod = importlib.import_module(ENCODINGS[encoding]["module"])
    except ImportError as e:
        raise ImportError(
            f"reading {encoding} needs {ENCODINGS[encoding]['module']}: "
            'pip install "MIMIC-IV-MEDS[external-values]"'
        ) from e
    path = meds_root / link
    if encoding == "wfdb":
        return mod.rdrecord(str(path))
    if encoding == "dicom":
        return mod.dcmread(path)
    return importlib.import_module("PIL.Image").open(path)


def check(meds_root: Path, links: dict[str, str]) -> dict[str, int]:
    """Check the folder against the links in the data (`link -> encoding`); returns counts.

    Every link must stay inside `external_values/` and resolve to all of its files, and every file under
    `external_values/` must belong to one of the links.
    """
    folder = meds_root / ROOT
    owned: set[Path] = set()
    for link, encoding in links.items():
        if Path(link).is_absolute() or ".." in Path(link).parts or Path(link).parts[0] != ROOT:
            raise ValueError(f"{LINK_COLUMN} values must be relative paths under {ROOT}/")
        if encoding not in ENCODINGS:
            raise ValueError(f"{ENCODING_COLUMN} has an unknown encoding {encoding!r}")
        try:
            files = files_of(meds_root, link, encoding)
        except FileNotFoundError:
            files = [meds_root / link]
        if missing := [f for f in files if not f.exists()]:
            raise ValueError(f"{len(missing)} file(s) of a linked {encoding} object are missing")
        owned.update(f.absolute() for f in files)
    on_disk = [p for p in folder.rglob("*") if p.is_file() or p.is_symlink()] if folder.exists() else []
    if stray := [p for p in on_disk if p.absolute() not in owned]:
        raise ValueError(f"{len(stray)} file(s) under {ROOT}/ belong to no linked object")
    return {"links": len(links), "files": len(on_disk)}
