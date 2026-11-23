"""Stage the raw input MEDS-Extract reads, and the manifest of the objects it will link to.

MEDS-Extract transforms tables; it cannot hash files, check that a file exists, or decide where it
goes in the output. This step does that work before extraction and hands MEDS-Extract a tidy table
per module that already carries the link column:

- the core (`hosp/`, `icu/`) and ED (`ed/`) tables are symlinked in unchanged;
- the public concept-map CSVs the base spec joins (its `sources.common` block) are linked in from a
  local copy, or downloaded once into a cache and checked against the spec's SHA-256;
- notes, ECGs, echo DICOMs, chest radiographs and waveform windows become one parquet table each
  under `<module>/`, with the event code, a leakage-safe time in MIMIC-IV's own format, the
  extension columns and, for objects, the link (`external_values/<module>/<sha[:2]>/<sha>...`,
  relative to the MEDS root) and its encoding (see `external_values.py`).

Rows are kept only for subjects in the core `hosp/patients` (so a module can never add subjects
that would reshuffle the core cohort's splits) and, for objects, only when every file of the object
is on disk (so no link dangles; re-running after a download finishes picks up the rest). What was
dropped and why is counted in the returned statistics, which end up in `metadata/sources.json`.
Nothing here prints row values: only counts.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import logging
import re
import urllib.request
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from .external_values import ROOT as EXTERNAL_ROOT
from .modules import FoundRelease

logger = logging.getLogger(__name__)

#: MIMIC-IV's own timestamp format; the fragments cast with exactly this.
TIME_FMT = "%Y-%m-%d %H:%M:%S"
_CHUNK = 1 << 20


@dataclass
class Staged:
    """What staging one module produced."""

    module: str
    stats: dict = field(default_factory=dict)
    #: One row per source object (see `OBJECT_SCHEMA`), or None for table-only modules.
    objects: pl.DataFrame | None = None
    #: Fragment tables this module could not stage (an optional raw table is absent); left out of the spec.
    skip_tables: set[str] = field(default_factory=set)


#: Columns of the object manifest, which `finalize.place_objects` uses to fill `external_values/`.
OBJECT_SCHEMA = {
    "link": pl.String,
    "modality": pl.String,
    "encoding": pl.String,
    "size_bytes": pl.Int64,
    # The object's files, relative to the release root; absolute for files the build wrote itself (the
    # windows of a long waveform segment), which are never symlinked into the MEDS root.
    "files": pl.List(pl.String),
}


# --------------------------------------------------------------------------------------- helpers
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(_CHUNK):
            h.update(chunk)
    return h.hexdigest()


def read_sha256sums(root: Path) -> dict[str, str]:
    """`relative path -> sha256` from a PhysioNet `SHA256SUMS.txt`, or {} when the release has none."""
    fp = root / "SHA256SUMS.txt"
    if not fp.is_file():
        return {}
    sums = {}
    for line in fp.read_text().splitlines():
        digest, _, name = line.strip().partition(" ")
        if digest and name:
            sums[name.strip().lstrip("*")] = digest
    return sums


def hash_files(root: Path, rel_paths: list[str], sums: dict[str, str], workers: int) -> dict[str, str]:
    """SHA-256 of each existing file in `rel_paths`: from `sums` when listed there, else computed."""
    todo = [p for p in rel_paths if p not in sums and (root / p).is_file()]
    out = {p: sums[p] for p in rel_paths if p in sums and (root / p).is_file()}
    if todo:
        logger.info(f"hashing {len(todo)} files under {root.name} ({len(out)} taken from SHA256SUMS.txt)")
        with ThreadPoolExecutor(max_workers=workers) as ex:
            out.update(zip(todo, ex.map(lambda p: sha256_file(root / p), todo), strict=False))
    return out


def object_digest(file_digests: dict[str, str]) -> str:
    """One SHA-256 for a multi-file object (a WFDB record): the hash of its sorted `<sha>  <name>` lines.

    It depends only on the files' contents and base names, so it is derivable from `SHA256SUMS.txt`
    without reading the files, and stable across releases that move the record.

    Examples:
        >>> a = object_digest({"a/r.hea": "11", "a/r.dat": "22"})
        >>> a == object_digest({"b/r.dat": "22", "b/r.hea": "11"})
        True
    """
    lines = sorted(f"{d}  {Path(p).name}\n" for p, d in file_digests.items())
    return hashlib.sha256("".join(lines).encode()).hexdigest()


def link_for(module: str, digest: str, suffix: str = "", record_name: str | None = None) -> str:
    """The object's path relative to the MEDS root, under `external_values/`.

    Examples:
        >>> link_for("cxr", "b70e" + "0" * 60, ".jpg")
        'external_values/cxr/b7/b70e000000000000000000000000000000000000000000000000000000000000.jpg'
        >>> link_for("ecg", "3fa2" + "0" * 60, record_name="40689238")[:28]
        'external_values/ecg/3f/3fa20'
        >>> link_for("ecg", "3fa2" + "0" * 60, record_name="40689238").endswith("/40689238")
        True
    """
    if record_name is not None:
        return f"{EXTERNAL_ROOT}/{module}/{digest[:2]}/{digest}/{record_name}"
    return f"{EXTERNAL_ROOT}/{module}/{digest[:2]}/{digest}{suffix}"


def link_expr(module: str, suffix: str = "", record_col: str | None = None) -> pl.Expr:
    """`link_for` as a polars expression over the `sha256` column; null where `sha256` is null."""
    sha = pl.col("sha256")
    head = pl.lit(f"{EXTERNAL_ROOT}/{module}/") + sha.str.slice(0, 2) + "/" + sha
    return (head + "/" + pl.col(record_col) if record_col else head + suffix).alias("link")


def parse_time(expr: pl.Expr) -> pl.Expr:
    """Parse a timestamp written as `YYYY-MM-DD HH:MM:SS[.fff]` or `YYYY-MM-DD`; null when neither."""
    s = expr.str.strip_chars()
    return pl.coalesce(
        s.str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S%.f", strict=False),
        s.str.strptime(pl.Datetime("us"), "%Y-%m-%d %H:%M:%S", strict=False),
        s.str.strptime(pl.Datetime("us"), "%Y-%m-%d", strict=False),
    )


def read_table(root: Path, *names: str) -> pl.DataFrame:
    """Read the first of `names` that exists under `root`, every column as a string."""
    for name in names:
        if (root / name).is_file():
            return pl.read_csv(root / name, infer_schema=False)
    raise FileNotFoundError(f"none of {names} under {root}")


def stream_subject_rows(
    stream: io.BufferedIOBase, subjects: Iterable[int], batch_rows: int = 1_000_000
) -> pl.DataFrame:
    r"""Read a CSV (gzipped or not) from a byte stream, keeping only the rows of `subjects`; all strings.

    For the tables that are too big to load whole and hold every patient of the release (MIMIC-IV-ECHO's
    `structured-measurement.csv.gz`: 28 M rows, 190 MB compressed): rows are parsed one at a time and
    only the cohort's are kept, so memory follows the cohort, not the file. The stream can be a local
    file or an HTTP response, so a download can filter the same way without storing the whole table.
    `csv` handles quoted commas and line breaks.

    Examples:
        >>> raw = b"subject_id,x\n1,a\n2,\"b, c\"\n3,d\n2,e\n"
        >>> stream_subject_rows(io.BytesIO(gzip.compress(raw)), [2]).rows()
        [('2', 'b, c'), ('2', 'e')]
        >>> stream_subject_rows(io.BytesIO(raw), [], batch_rows=1).height
        0
    """
    keep = {str(s) for s in subjects}
    buffered = io.BufferedReader(stream) if not isinstance(stream, io.BufferedReader) else stream
    if buffered.peek(2)[:2] == b"\x1f\x8b":
        buffered = gzip.GzipFile(fileobj=buffered)
    reader = csv.reader(io.TextIOWrapper(buffered, encoding="utf-8", newline=""))
    header = next(reader)
    col = header.index("subject_id")
    frames, rows = [], []
    for row in reader:
        if row and row[col] in keep:
            rows.append(row)
            if len(rows) >= batch_rows:
                frames.append(pl.DataFrame(rows, schema=header, orient="row"))
                rows = []
    frames.append(pl.DataFrame(rows, schema=dict.fromkeys(header, pl.String), orient="row"))
    return pl.concat(frames)


def cohort_subjects(core: FoundRelease) -> pl.Series:
    return (
        pl.read_csv(
            core.root / "hosp" / "patients.csv.gz",
            columns=["subject_id"],
            schema_overrides={"subject_id": pl.Int64},
        )
        .get_column("subject_id")
        .unique()
    )


def _link_dir(src: Path, dst: Path) -> None:
    if dst.is_symlink() or dst.exists():
        if dst.is_symlink() and dst.resolve() == src.resolve():
            return
        raise FileExistsError(f"{dst} already exists and is not a link to {src}")
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.symlink_to(src.resolve(), target_is_directory=src.is_dir())


# ------------------------------------------------------------------------------ table modules
def stage_core(core: FoundRelease, raw_input: Path) -> Staged:
    for sub in ("hosp", "icu"):
        if not (core.root / sub).is_dir():
            raise FileNotFoundError(f"{core.root / sub} is missing; the core build needs both hosp/ and icu/")
        _link_dir(core.root / sub, raw_input / sub)
    return Staged("core", {"subjects": int(cohort_subjects(core).len())})


def stage_common(raw_input: Path, common: list[dict], search_dirs: list[Path], cache_dir: Path) -> None:
    """Link the base spec's `sources.common` files into `raw_input`, downloading any not found locally.

    `common` is the base spec's `sources.common` list (http entries with `url`, `rel_path`, `sha256`).
    A local copy counts only when its SHA-256 matches, so a stale file is never used.
    """
    search_dirs = [*search_dirs, cache_dir]  # earlier downloads count as a local copy
    for entry in common:
        for item in entry.get("urls", []):
            rel, want = item["rel_path"], item.get("sha256")
            hit = next(
                (
                    d / rel
                    for d in search_dirs
                    if (d / rel).is_file() and (not want or sha256_file(d / rel) == want)
                ),
                None,
            )
            if hit is None:
                hit = cache_dir / rel
                hit.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"downloading {rel} (public concept map) into {cache_dir}")
                tmp = hit.with_suffix(hit.suffix + ".partial")
                req = urllib.request.Request(item["url"], headers={"User-Agent": "MIMIC_IV_MEDS-multimodal"})
                with urllib.request.urlopen(req, timeout=120) as r, open(tmp, "wb") as f:
                    while chunk := r.read(_CHUNK):
                        f.write(chunk)
                if want and (got := sha256_file(tmp)) != want:
                    tmp.unlink()
                    raise ValueError(f"{item['url']}: SHA-256 {got} does not match the spec's {want}")
                tmp.replace(hit)
            _link_dir(hit, raw_input / rel)


def stage_ed(ed: FoundRelease, raw_input: Path, subjects: pl.Series) -> Staged:
    """`ed/`: the MIMIC-IV-ED tables as the `MIMIC-IV-ED` spec reads them, restricted to the core cohort.

    When every ED stay belongs to a core subject (the ED demo next to the MIMIC-IV demo, or the full releases
    together) the folder is linked in unchanged. Otherwise (the full ED release next to the MIMIC-IV demo)
    each table is streamed and rewritten as csv.gz with only the cohort's rows, so ED cannot add subjects;
    MEDS-Extract then infers its column types exactly as from the raw files.
    """
    src = ed.root / "ed"
    stays = pl.read_csv(
        src / "edstays.csv.gz", columns=["subject_id"], schema_overrides={"subject_id": pl.Int64}
    )
    outside = int((~stays["subject_id"].is_in(subjects.implode())).sum())
    stats = {"stays": stays.height, "stays_outside_core_subjects": outside, "filtered": bool(outside)}
    if not outside:
        _link_dir(src, raw_input / "ed")
        return Staged("ed", stats)
    logger.info(
        f"ED: {outside} of {stays.height} stays are outside the core cohort; filtering every ED table"
    )
    dst = raw_input / "ed"
    dst.mkdir(parents=True, exist_ok=True)
    for fp in sorted(src.glob("*.csv.gz")):
        with open(fp, "rb") as f:
            kept = stream_subject_rows(f, subjects.to_list())
        # `csv` reads an empty field as "", which polars would write back quoted (a string); null keeps
        # it empty, as in the raw file, so MEDS-Extract infers the same column types.
        with gzip.open(dst / fp.name, "wb") as g:
            kept.with_columns(pl.all().replace("", None)).write_csv(g)
        stats[f"{fp.name.removesuffix('.csv.gz')}_rows_kept"] = kept.height
    return Staged("ed", stats)


def _subject_filtered_csv(src: Path, dst: Path, subjects: pl.Series) -> dict:
    """Link `src` (a csv[.gz] with `subject_id`) to `dst`, or write only the cohort's rows when it has
    others."""
    ids = pl.read_csv(src, columns=["subject_id"], schema_overrides={"subject_id": pl.Int64})["subject_id"]
    outside = int((~ids.is_in(subjects.implode())).sum())
    dst.parent.mkdir(parents=True, exist_ok=True)
    if not outside:
        dst.symlink_to(src.resolve())
        return {"rows": ids.len(), "rows_outside_core_subjects": 0}
    with open(src, "rb") as f:
        kept = stream_subject_rows(f, subjects.to_list())
    out = dst.with_name(dst.name.removesuffix(".gz").removesuffix(".csv") + ".csv.gz")
    with gzip.open(out, "wb") as g:
        kept.with_columns(pl.all().replace("", None)).write_csv(g)
    return {"rows": ids.len(), "rows_outside_core_subjects": outside}


def stage_mne(mne: FoundRelease, raw_input: Path, subjects: pl.Series) -> Staged:
    """MIMIC-Note-Extract: its tables at the raw-input root, as the `MIMIC-IV-MNE` spec reads them.

    Each table the fragment extracts is linked in, or, when it has rows of subjects outside the core
    cohort (the full release next to the MIMIC-IV demo), rewritten with only the cohort's rows.
    """
    from .compose import fragment

    stats: dict = {}
    for table in fragment("mne"):
        src = next(
            (
                mne.root / f"{table}{ext}"
                for ext in (".csv.gz", ".csv")
                if (mne.root / f"{table}{ext}").is_file()
            ),
            None,
        )
        if src is None:
            raise FileNotFoundError(f"MIMIC-Note-Extract table {table}.csv.gz is missing from {mne.root}")
        s = _subject_filtered_csv(src, raw_input / src.name, subjects)
        stats[f"{table}_rows"] = s["rows"] - s["rows_outside_core_subjects"]
        stats["rows_outside_core_subjects"] = (
            stats.get("rows_outside_core_subjects", 0) + s["rows_outside_core_subjects"]
        )
    return Staged("mne", stats)


def stage_cxr_db(db: FoundRelease, raw_input: Path, subjects: pl.Series) -> Staged:
    """MIMIC-CXR-DB: `cxr_db/<table>.parquet`, as the `MIMIC-IV-CXRDB` spec reads them.

    The release's semicolon-delimited CSVs are converted with `prepare_cxr_db.convert_table` (which adds
    `radiographs.study_end_datetime`); parquet files already there (a prepared download, the simulated
    release) are used as they are. Rows are restricted to the core cohort: radiographs by subject,
    report concepts by the kept studies, discharge concepts by the cohort's admissions.
    """
    from MIMIC_IV_MEDS.prepare_cxr_db import OUT_SUBDIR, TABLES, convert_table

    out = raw_input / OUT_SUBDIR
    out.mkdir(parents=True, exist_ok=True)
    frames = {}
    for t in TABLES:
        pq = db.root / OUT_SUBDIR / f"{t}.parquet"
        if pq.is_file():
            frames[t] = pl.read_parquet(pq)
        elif (db.root / f"{t}.csv").is_file():
            frames[t] = convert_table(db.root / f"{t}.csv", out / f"{t}.parquet")
        else:
            raise FileNotFoundError(f"MIMIC-CXR-DB: neither {pq} nor {db.root / f'{t}.csv'} exists")
    adm = raw_input / "hosp" / "admissions.csv.gz"
    hadm = pl.read_csv(adm, columns=["subject_id", "hadm_id"]).filter(
        pl.col("subject_id").is_in(subjects.implode())
    )["hadm_id"]
    rad = frames["radiographs"].filter(pl.col("subject_id").cast(pl.Int64).is_in(subjects.implode()))
    kept = {
        "radiographs": rad,
        "radiology_jev": frames["radiology_jev"].filter(pl.col("study_id").is_in(rad["study_id"].implode())),
        "discharge_jev": frames["discharge_jev"].filter(pl.col("hadm_id").is_in(hadm.implode())),
    }
    stats = {}
    for t, df in kept.items():
        df.write_parquet(out / f"{t}.parquet")
        stats[f"{t}_rows"] = df.height
        stats[f"{t}_rows_outside_core_cohort"] = frames[t].height - df.height
    return Staged("cxr_db", stats)


#: `(table, note_type) -> (code, description)`; other note types get `NOTE//<TABLE>//<type>`.
NOTE_CODES = {
    ("discharge", "DS"): (
        "NOTE//DISCHARGE_SUMMARY",
        "Discharge summary (MIMIC-IV-Note discharge, note_type DS)",
    ),
    ("discharge", "AD"): (
        "NOTE//DISCHARGE_SUMMARY//ADDENDUM",
        "Addendum to a discharge summary (MIMIC-IV-Note discharge, note_type AD)",
    ),
    ("radiology", "RR"): (
        "NOTE//RADIOLOGY_REPORT",
        "Radiology report (MIMIC-IV-Note radiology, note_type RR)",
    ),
    ("radiology", "AR"): (
        "NOTE//RADIOLOGY_REPORT//ADDENDUM",
        "Addendum to a radiology report (MIMIC-IV-Note radiology, note_type AR)",
    ),
}


def stage_note(note: FoundRelease, raw_input: Path, subjects: pl.Series) -> Staged:
    """`note/notes.parquet`: one row per discharge summary or radiology report, text in `text`.

    Timed at `max(charttime, storetime)`: `charttime` is when the documented care happened (for a
    discharge summary often just the discharge date at midnight), `storetime` when the note was
    saved and could first be read. Using the later of the two keeps a model from seeing a note
    before it existed; `charttime` alone is used when `storetime` is missing.
    """
    frames, stats = [], {}
    for table in note.parts:  # "discharge", "radiology", or the one asked for
        fp = note.root / "note" / f"{table}.csv.gz"
        if not fp.is_file():
            if note.explicit:
                raise FileNotFoundError(
                    f"--extensions mimic-iv-note:{table} asked for {fp}, which does not exist"
                )
            stats[table] = {"present": False}
            continue
        df = pl.read_csv(fp, infer_schema=False).with_columns(
            pl.col("subject_id").cast(pl.Int64),
            pl.col("hadm_id").cast(pl.Int64, strict=False),
            parse_time(pl.col("charttime")).alias("_chart"),
            parse_time(pl.col("storetime")).alias("_store"),
        )
        in_cohort = df["subject_id"].is_in(subjects.implode())
        delay_h = (df["_store"] - df["_chart"]).dt.total_seconds() / 3600
        stats[table] = {
            "present": True,
            "rows": df.height,
            "outside_core_subjects": int((~in_cohort).sum()),
            "unparsable_charttime": int(df["_chart"].is_null().sum() - df["charttime"].is_null().sum()),
            "missing_storetime": int(df["_store"].is_null().sum()),
            "storetime_before_charttime": int((delay_h < 0).sum()),
            "storetime_minus_charttime_hours_median": None
            if delay_h.drop_nulls().is_empty()
            else round(float(delay_h.median()), 2),
        }
        code_map = {t: c for (tb, t), c in NOTE_CODES.items() if tb == table}
        df = df.filter(in_cohort).with_columns(
            pl.max_horizontal("_chart", "_store").dt.strftime(TIME_FMT).alias("note_time"),
            pl.col("note_type")
            .replace_strict(
                {k: v[0] for k, v in code_map.items()},
                default=pl.lit(f"NOTE//{table.upper()}//") + pl.col("note_type").fill_null("UNK"),
            )
            .alias("note_code"),
            pl.col("note_type")
            .replace_strict(
                {k: v[1] for k, v in code_map.items()},
                default=pl.lit(f"Note from MIMIC-IV-Note {table}, note_type ")
                + pl.col("note_type").fill_null("UNK"),
            )
            .alias("note_description"),
        )
        frames.append(
            df.select(
                "subject_id", "hadm_id", "note_id", "note_code", "note_description", "note_time", "text"
            )
        )
    if not frames:
        raise FileNotFoundError(
            f"{note.root / 'note'} has none of {', '.join(f'{t}.csv.gz' for t in note.parts)}"
        )
    out = pl.concat(frames)
    (raw_input / "note").mkdir(parents=True, exist_ok=True)
    out.write_parquet(raw_input / "note" / "notes.parquet")
    stats["emitted"] = out.height
    stats["emitted_without_time"] = int(out["note_time"].is_null().sum())
    return Staged("note", stats)


# ----------------------------------------------------------------------------- object modules
def _object_stats(listed: int, outside: int, missing: int, bad_time: int, kept: pl.DataFrame) -> dict:
    return {
        "listed": listed,
        "outside_core_subjects": outside,
        "missing_on_disk": missing,
        "unparsable_time": bad_time,
        "emitted": kept.height,
        "distinct_objects": kept["link"].n_unique() if kept.height else 0,
    }


def _finish_objects(
    found: FoundRelease,
    df: pl.DataFrame,
    subjects: pl.Series,
    out_table: Path,
    event_cols: list[str],
    encoding: str,
) -> Staged:
    """Shared tail of the object modules: filter, write the event table, build the manifest.

    `df` has one row per listed object with `subject_id`, `files`, `sha256` (null when any file is
    missing), `size_bytes`, `link`, `time` (parsed) and the module's event columns. The event table
    gets `encoding` too (the fragments map it to `external_value_encoding`).
    """
    df = df.with_columns(pl.lit(encoding).alias("encoding"))
    event_cols = [*event_cols, "encoding"]
    listed = df.height
    in_cohort = df["subject_id"].is_in(subjects.implode())
    on_disk = df["sha256"].is_not_null()
    has_time = df["time"].is_not_null()
    kept = df.filter(in_cohort & on_disk & has_time)
    stats = _object_stats(
        listed,
        outside=int((~in_cohort).sum()),
        missing=int((in_cohort & ~on_disk).sum()),
        bad_time=int((in_cohort & on_disk & ~has_time).sum()),
        kept=kept,
    )
    if stats["missing_on_disk"]:
        logger.warning(
            f"{found.module.name}: {stats['missing_on_disk']} of {listed} listed objects are not (fully) "
            "on disk and get no event; re-run once the download completes"
        )
    out_table.parent.mkdir(parents=True, exist_ok=True)
    kept.with_columns(pl.col("time").dt.strftime(TIME_FMT)).select(event_cols).write_parquet(out_table)
    objects = kept.select(
        pl.col("link"), pl.lit(found.module.name).alias("modality"), "encoding", "size_bytes", "files"
    ).cast(OBJECT_SCHEMA)
    return Staged(found.module.name, stats, objects)


def _single_file_objects(found: FoundRelease, df: pl.DataFrame, suffix: str, workers: int) -> pl.DataFrame:
    """Add `files`, `sha256`, `size_bytes`, `link` to a frame with one file (`source_path`) per row."""
    rels = df["source_path"].to_list()
    digests = hash_files(found.root, rels, read_sha256sums(found.root), workers)
    sizes = {p: (found.root / p).stat().st_size for p in digests}
    return df.with_columns(
        pl.concat_list("source_path").alias("files"),
        pl.col("source_path").replace_strict(digests, default=None, return_dtype=pl.String).alias("sha256"),
        pl.col("source_path").replace_strict(sizes, default=None, return_dtype=pl.Int64).alias("size_bytes"),
    ).with_columns(link_expr(found.module.name, suffix))


def stage_ecg(ecg: FoundRelease, raw_input: Path, subjects: pl.Series, workers: int) -> Staged:
    """`ecg/records.parquet`: one `ECG//WAVEFORM` row per diagnostic ECG (a WFDB `.hea` + `.dat` pair).

    Timed at `record_list.ecg_time`, the acquisition time (the release warns the ECG machines'
    clocks were mostly unsynchronized). The link is the WFDB record name inside its own
    `ecg/<sha[:2]>/<sha>/` folder, so `wfdb.rdrecord(<meds_root>/<link>)` reads it: the `.hea`
    names its `.dat`, so the files keep their original names.
    """
    df = read_table(ecg.root, "record_list.csv", "record_list.csv.gz").select(
        pl.col("subject_id").cast(pl.Int64),
        pl.col("study_id").cast(pl.Int64),
        pl.col("path").str.strip_chars().str.strip_prefix("./").alias("source_path"),
        parse_time(pl.col("ecg_time")).alias("time"),
    )
    df = df.with_columns(pl.col("source_path").str.split("/").list.last().alias("object_id"))
    rels = [f"{p}{ext}" for p in df["source_path"] for ext in (".hea", ".dat")]
    digests = hash_files(ecg.root, rels, read_sha256sums(ecg.root), workers)
    rec_digest, rec_size = {}, {}
    for p in df["source_path"]:
        files = {f"{p}{ext}": digests.get(f"{p}{ext}") for ext in (".hea", ".dat")}
        if all(files.values()):
            rec_digest[p] = object_digest(files)
            rec_size[p] = sum((ecg.root / f).stat().st_size for f in files)
    df = df.with_columns(
        pl.concat_list(pl.col("source_path") + ".hea", pl.col("source_path") + ".dat").alias("files"),
        pl.col("source_path")
        .replace_strict(rec_digest, default=None, return_dtype=pl.String)
        .alias("sha256"),
        pl.col("source_path")
        .replace_strict(rec_size, default=None, return_dtype=pl.Int64)
        .alias("size_bytes"),
    ).with_columns(
        link_expr("ecg", record_col="object_id"),
        pl.col("study_id").alias("ecg_study_id"),
        pl.lit("ECG//WAVEFORM").alias("code"),
    )
    return _finish_objects(
        ecg,
        df,
        subjects,
        raw_input / "ecg" / "records.parquet",
        ["subject_id", "code", "time", "ecg_study_id", "link"],
        "wfdb",
    )


MEASUREMENT_FILES = (
    "structured-measurement.csv.gz",
    "structured_measurement.csv.gz",
    "structured-measurement.csv",
    "structured_measurement.csv",
)
_NUMBER = r"^[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$"


def stage_echo_measurements(echo: FoundRelease, raw_input: Path, subjects: pl.Series) -> dict:
    """`echo/measurements.parquet`: one `ECHO//MEASUREMENT//<measurement>` row per reported measurement.

    From MIMIC-IV-ECHO's `structured-measurement` table, the measurements of each echo report (it covers
    every echo of the release's patients, most of them without released DICOMs). The table holds every
    patient, so it is streamed and filtered to the core cohort (`stream_subject_rows`). Rows with an
    empty `result` are blank fields of the report form and get no event. A numeric `result` goes to
    `numeric_value`, any other (`Normal (<1.2cm)`) to `text_value`, under the same code; the unit is
    the code modifier `unit`, as for labs. Timed at `measurement_datetime`. `echo_study_id` is set
    where `echo-study-list` links the measurement to a study with DICOMs.
    """
    fp = next((echo.root / n for n in MEASUREMENT_FILES if (echo.root / n).is_file()), None)
    if fp is None:
        return {"present": False}
    logger.info(f"echo: streaming {fp.name} for the cohort's {subjects.len()} subjects")
    with open(fp, "rb") as f:
        df = stream_subject_rows(f, subjects.to_list())
    result = pl.col("result").str.strip_chars()
    df = df.with_columns(
        pl.col("subject_id").cast(pl.Int64),
        pl.col("measurement_id").cast(pl.Int64),
        parse_time(pl.col("measurement_datetime")).alias("_time"),
        result.replace("", None).alias("result"),
        pl.col("unit").str.strip_chars().replace("", None).alias("unit"),
        pl.col("test_type").str.strip_chars().replace("", None).alias("echo_test_type"),
    )
    try:
        links = read_table(echo.root, "echo-study-list.csv", "echo-study-list.csv.gz").select(
            pl.col("measurement_id").cast(pl.Int64, strict=False), pl.col("study_id").cast(pl.Int64)
        )
    except FileNotFoundError:
        links = pl.DataFrame(schema={"measurement_id": pl.Int64, "study_id": pl.Int64})
    links = links.drop_nulls().unique("measurement_id", keep="any")
    is_number = pl.col("result").str.contains(_NUMBER)
    kept = (
        df.filter(pl.col("result").is_not_null() & pl.col("_time").is_not_null())
        .join(links, on="measurement_id", how="left")
        .select(
            "subject_id",
            (pl.lit("ECHO//MEASUREMENT//") + pl.col("measurement").str.strip_chars()).alias("code"),
            pl.col("_time").dt.strftime(TIME_FMT).alias("time"),
            pl.when(is_number).then(pl.col("result").cast(pl.Float64, strict=False)).alias("numeric_value"),
            pl.when(~is_number).then(pl.col("result")).alias("text_value"),
            "unit",
            pl.col("measurement_description").str.strip_chars().alias("description"),
            "echo_test_type",
            pl.col("measurement_id").alias("echo_measurement_id"),
            pl.col("study_id").alias("echo_study_id"),
        )
    )
    out = raw_input / "echo" / "measurements.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    kept.write_parquet(out)
    return {
        "present": True,
        "file": fp.name,
        "cohort_rows": df.height,
        "empty_result": int(df["result"].is_null().sum()),
        "unparsable_time": int(df["_time"].is_null().sum()),
        "emitted": kept.height,
        "emitted_numeric": int(kept["numeric_value"].is_not_null().sum()),
        "emitted_text": int(kept["text_value"].is_not_null().sum()),
        "linked_to_study": int(kept["echo_study_id"].is_not_null().sum()),
        "subjects": kept["subject_id"].n_unique(),
        "codes": kept["code"].n_unique(),
    }


def stage_echo(echo: FoundRelease, raw_input: Path, subjects: pl.Series, workers: int) -> Staged:
    """`echo/records.parquet`: one `ECHO//DICOM` row per echocardiogram DICOM file, plus the measurements.

    Timed at `echo-record-list.acquisition_datetime`. Most files are cine loops, some single frames; the code
    does not say which because telling them apart means reading every DICOM header. The measurements
    (`stage_echo_measurements`) are optional: without the table the spec leaves `echo/measurements` out.
    """
    measurements = {"selected": False}
    if echo.wants("measurements"):
        measurements = stage_echo_measurements(echo, raw_input, subjects)
        if not measurements["present"] and echo.explicit:
            raise FileNotFoundError(
                f"--extensions mimic-iv-echo:measurements: none of {MEASUREMENT_FILES} in {echo.root}"
            )
    has_list = any((echo.root / n).is_file() for n in ("echo-record-list.csv", "echo-record-list.csv.gz"))
    if not echo.wants("dicom") or (not has_list and not echo.explicit):
        if not measurements.get("present"):
            raise FileNotFoundError(
                f"{echo.root}: nothing to build for echo (no record list, no measurements)"
            )
        return Staged(
            "echo",
            {"dicoms": {"selected": echo.wants("dicom"), "present": has_list}, "measurements": measurements},
            skip_tables={"echo/records"},
        )
    df = (
        read_table(echo.root, "echo-record-list.csv", "echo-record-list.csv.gz")
        .select(
            pl.col("subject_id").cast(pl.Int64),
            pl.col("study_id").cast(pl.Int64),
            pl.col("dicom_filepath").str.strip_chars().str.strip_prefix("./").alias("source_path"),
            parse_time(pl.col("acquisition_datetime")).alias("time"),
        )
        .with_columns(
            pl.when(pl.col("source_path").str.starts_with("files/"))
            .then(pl.col("source_path"))
            .otherwise(pl.lit("files/") + pl.col("source_path"))
            .alias("source_path"),
        )
        .with_columns(
            pl.col("source_path").str.split("/").list.last().str.strip_suffix(".dcm").alias("object_id"),
            pl.col("study_id").alias("echo_study_id"),
            pl.lit("ECHO//DICOM").alias("code"),
        )
    )
    df = _single_file_objects(echo, df, ".dcm", workers)
    staged = _finish_objects(
        echo,
        df,
        subjects,
        raw_input / "echo" / "records.parquet",
        ["subject_id", "code", "time", "echo_study_id", "link"],
        "dicom",
    )
    staged.stats = {"dicoms": staged.stats, "measurements": measurements}
    if not measurements.get("present"):
        staged.skip_tables.add("echo/measurements")
    return staged


def stage_cxr(cxr: FoundRelease, raw_input: Path, subjects: pl.Series, workers: int) -> Staged:
    """`cxr/images.parquet`: one `CXR//IMAGE//<ViewPosition>//<PatientOrientation>` row per JPG.

    The code is MIMIC-CXR-DB's image code (`UNK` for a missing value), so the same image is coded
    the same way whichever of the two releases it comes from; with both in the build, these linked
    events stand for the images and CXR-DB adds the rest (see `Module.yields_to`).

    Timed at the image's `StudyDate` + `StudyTime` (DICOM `HHMMSS.fff`, leading zeros dropped in
    the CSV). The CheXpert labels are not extracted: they are mined from the report, which exists
    only after the images, and MIMIC-CXR-JPG gives no report time to place them at.
    """
    df = read_table(cxr.root, "mimic-cxr-2.0.0-metadata.csv.gz", "mimic-cxr-2.0.0-metadata.csv")
    if "PatientOrientationCodeSequence_CodeMeaning" not in df.columns:
        df = df.with_columns(pl.lit(None, pl.String).alias("PatientOrientationCodeSequence_CodeMeaning"))
    hhmmss = pl.col("StudyTime").str.strip_chars().str.split(".").list.first().str.zfill(6)
    df = df.select(
        pl.col("subject_id").cast(pl.Int64),
        pl.col("study_id").cast(pl.Int64),
        pl.col("dicom_id").alias("object_id"),
        (
            pl.lit("files/p")
            + pl.col("subject_id").str.slice(0, 2)
            + "/p"
            + pl.col("subject_id")
            + "/s"
            + pl.col("study_id")
            + "/"
            + pl.col("dicom_id")
            + ".jpg"
        ).alias("source_path"),
        (pl.col("StudyDate").str.strip_chars() + hhmmss)
        .str.strptime(pl.Datetime("us"), "%Y%m%d%H%M%S", strict=False)
        .alias("time"),
        pl.col("ViewPosition").str.strip_chars().replace("", None).fill_null("UNK").alias("view"),
        pl.col("PatientOrientationCodeSequence_CodeMeaning")
        .str.strip_chars()
        .replace("", None)
        .fill_null("UNK")
        .alias("orientation"),
    ).with_columns(
        pl.col("study_id").alias("cxr_study_id"),
        pl.col("object_id").alias("cxr_dicom_id"),
        (pl.lit("CXR//IMAGE//") + pl.col("view") + "//" + pl.col("orientation")).alias("code"),
    )
    df = _single_file_objects(cxr, df, ".jpg", workers)
    return _finish_objects(
        cxr,
        df,
        subjects,
        raw_input / "cxr" / "images.parquet",
        ["subject_id", "code", "time", "view", "orientation", "cxr_study_id", "cxr_dicom_id", "link"],
        "jpeg",
    )


@dataclass
class WfdbMaster:
    """What a WFDB multi-segment master header says (only what the build needs)."""

    name: str
    fs: float  # frames per second
    n_samples: int
    start: datetime | None
    #: `(segment name or "~" for a gap, samples)`, in order.
    segments: list[tuple[str, int]]
    info: dict[str, str]
    #: Ticks per second of the counter the numerics' `time` column counts, and its value at the start.
    counter_freq: float | None = None
    counter_base: float = 0.0


def parse_wfdb_master(text: str) -> WfdbMaster:
    r"""Parse a multi-segment WFDB header: record line, segment lines, `# key value` info lines.

    Examples:
        >>> m = parse_wfdb_master(
        ...     "#wfdb 10.7\n"
        ...     "8000/3 7 62.5/999.9 1250 23:59:58.500 31/12/2150\n"
        ...     "8000_0000 0\n8000_0001 625\n~ 125\n8000_0002 500\n"
        ...     "# subject_id 10000032\n# hadm_id 22595853\n"
        ... )
        >>> m.name, m.fs, m.n_samples, m.start
        ('8000', 62.5, 1250, datetime.datetime(2150, 12, 31, 23, 59, 58, 500000))
        >>> m.counter_freq, m.counter_base
        (999.9, 0.0)
        >>> m.segments
        [('8000_0000', 0), ('8000_0001', 625), ('~', 125), ('8000_0002', 500)]
        >>> m.info
        {'subject_id': '10000032', 'hadm_id': '22595853'}
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    info = {}
    for ln in lines:
        if ln.startswith("#"):
            key, _, value = ln.lstrip("#").strip().partition(" ")
            if key != "wfdb":
                info[key] = value.strip()
    body = [ln for ln in lines if not ln.startswith("#")]
    rec = body[0].split()
    start = None
    if len(rec) >= 6:
        for fmt in ("%H:%M:%S.%f %d/%m/%Y", "%H:%M:%S %d/%m/%Y"):
            try:
                start = datetime.strptime(f"{rec[4]} {rec[5]}", fmt)
                break
            except ValueError:
                pass
    segments = [(tok[0], int(tok[1])) for tok in (ln.split() for ln in body[1:])]
    # `<frame freq>/<counter freq>(<base counter value>)`, the last two optional.
    freq, _, counter = rec[2].partition("/")
    counter_freq, _, base = counter.partition("(")
    return WfdbMaster(
        rec[0].split("/")[0],
        float(freq),
        int(rec[3]),
        start,
        segments,
        info,
        float(counter_freq) if counter_freq else None,
        float(base.rstrip(")")) if base else 0.0,
    )


def _segment_files(hea: Path) -> list[str]:
    """The signal files a segment header names (`~` marks a signal absent from the segment)."""
    files = []
    for ln in hea.read_text().splitlines()[1:]:
        tok = ln.split()
        if tok and not ln.startswith("#") and tok[0] != "~" and tok[0] not in files:
            files.append(tok[0])
    return files


#: Default length of a waveform window, in seconds (`--waveform-window-s`).
WAVEFORM_WINDOW_S = 600.0


def cut_wfdb_windows(
    record: Path, out_dir: Path, window_samples: int, start: datetime | None
) -> list[tuple[str, int, int, list[Path]]]:
    """Write the single-segment WFDB record `record` as consecutive windows of `window_samples` frames.

    Each window is a WFDB record of its own in `out_dir`, named `<record>_wNNNN`, with the same
    signals, signal file grouping, storage format (FLAC-compressed format 516 in MIMIC-IV-WDB), gains,
    baselines and samples per frame, and the digital samples unchanged; only the samples differ, and
    the base time is the window's start when `start` (the record's start) is known. Returns
    `(name, first frame, end frame, files)` per window. Needs `wfdb` (the `external-values` extra).
    """
    try:
        import wfdb
    except ImportError as e:
        raise ImportError(
            "cutting long waveform segments into windows needs wfdb: "
            'pip install "MIMIC-IV-MEDS[external-values]" (or --waveform-window-s 0 keeps whole segments, '
            "outside the point-in-time scope of a linked object)"
        ) from e
    header = wfdb.rdheader(str(record))
    seg = header.record_name
    groups = list(dict.fromkeys(header.file_name))
    out_dir.mkdir(parents=True, exist_ok=True)
    windows = []
    for k, s in enumerate(range(0, header.sig_len, window_samples)):
        e = min(header.sig_len, s + window_samples)
        r = wfdb.rdrecord(str(record), sampfrom=s, sampto=e, physical=False, smooth_frames=False)
        name = f"{seg}_w{k:04d}"
        r.record_name = name
        # `<seg>e.dat` -> `<seg>_w0000e.dat`: the signal groups keep their suffixes.
        r.file_name = [
            name + (fn[len(seg) :] if fn.startswith(seg) else f"_{groups.index(fn)}.dat")
            for fn in r.file_name
        ]
        if r.init_value is not None:
            r.init_value = [int(sig[0]) for sig in r.e_d_signal]
        if start is not None:
            ws = start + timedelta(seconds=s / header.fs)
            r.base_time, r.base_date = ws.time(), ws.date()
        r.wrsamp(write_dir=str(out_dir), expanded=True)
        files = [out_dir / f"{name}.hea", *(out_dir / f for f in dict.fromkeys(r.file_name))]
        windows.append((name, s, e, files))
    return windows


#: Default interval the vital signs are summarized over, in seconds (`--waveform-numerics-interval-s`).
NUMERICS_INTERVAL_S = 60.0
_NUMERIC_COLUMN = re.compile(r"^(?P<name>.*?)\s*\[(?P<unit>[^\]]*)\]\s*$")


def numeric_name_unit(column: str) -> tuple[str, str | None]:
    """`"HR [bpm]"` -> `("HR", "bpm")`; a column without a `[unit]` keeps its name and gets no unit.

    Examples:
        >>> numeric_name_unit("Pulse (SpO2) [bpm]"), numeric_name_unit("PVC [/min]"), numeric_name_unit("Foo")
        (('Pulse (SpO2)', 'bpm'), ('PVC', '/min'), ('Foo', None))
    """
    m = _NUMERIC_COLUMN.match(column)
    return (m["name"], m["unit"] or None) if m else (column.strip(), None)


def read_wfdb_numerics(fp: Path, m: WfdbMaster, interval_s: float) -> pl.DataFrame:
    """A record's numerics (`<record>n.csv.gz`) as `time, code, numeric_value, unit` rows.

    The first column counts ticks of the record's counter (`m.counter_freq` per second, `m.counter_base`
    at the record's start); every other column is one signal, `"<name> [<unit>]"`. With `interval_s > 0`
    each signal becomes its median over consecutive `interval_s`-second intervals from the record's
    start, **timed at the end of the interval**, so no value appears before all the readings it
    summarizes; with `interval_s <= 0` every reading is its own row, at its own time. Readings that are
    not numbers are dropped. Codes are `WAVEFORM//NUMERIC//<name>`.
    """
    df = pl.read_csv(fp, infer_schema_length=0)
    tcol = df.columns[0]
    long = (
        df.unpivot(index=tcol, variable_name="column", value_name="value")
        .with_columns(
            ((pl.col(tcol).cast(pl.Float64, strict=False) - m.counter_base) / m.counter_freq).alias("s"),
            pl.col("value").cast(pl.Float64, strict=False),
        )
        .drop_nulls(["s", "value"])
    )
    if interval_s > 0:
        long = (
            long.group_by("column", ((pl.col("s") // interval_s) + 1).alias("interval"))
            .agg(pl.col("value").median())
            .with_columns((pl.col("interval") * interval_s).alias("s"))
        )
    names = {c: numeric_name_unit(c) for c in df.columns[1:]}
    return long.select(
        (pl.lit(m.start) + pl.duration(microseconds=(pl.col("s") * 1e6).round().cast(pl.Int64))).alias(
            "time"
        ),
        (
            pl.lit("WAVEFORM//NUMERIC//")
            + pl.col("column").replace_strict({c: n for c, (n, _) in names.items()}, return_dtype=pl.String)
        ).alias("code"),
        pl.col("value").alias("numeric_value"),
        pl.col("column")
        .replace_strict({c: u for c, (_, u) in names.items()}, return_dtype=pl.String)
        .alias("unit"),
    ).sort("time", "code")


def stage_waveform(
    wdb: FoundRelease,
    raw_input: Path,
    subjects: pl.Series,
    workers: int,
    window_s: float = WAVEFORM_WINDOW_S,
    window_dir: Path | None = None,
    numerics_interval_s: float = NUMERICS_INTERVAL_S,
) -> Staged:
    """The MIMIC-IV Waveform Database: linked waveform windows and the monitors' vital signs.

    A record is a WFDB multi-segment record of one ICU stay (hours to days): a master header, one
    header plus signal files per segment (gaps between them), and a numerics table (`<record>n.csv.gz`).
    Neither is linked whole: a linked object should be short enough to treat as one point in time.

    - Part `windows`, `waveform/windows.parquet`: every segment longer than `window_s` is cut into
      windows of `window_s` seconds (the last one shorter), each written as a WFDB record of its own
      (`cut_wfdb_windows`, into `window_dir`); shorter segments are linked unchanged. Each window is one
      `WAVEFORM//WINDOW` event, **timed at its end** (`waveform_duration_s` gives its length), linking
      to a folder that holds that window and nothing else. `window_s <= 0` keeps whole segments.
    - Part `numerics`, `waveform/numerics.parquet`: the vital signs (HR, SpO2, ABP, ... about once a
      second) as ordinary events with a `numeric_value` and a `unit` (`read_wfdb_numerics`), each
      summarized over `numerics_interval_s` and timed at the end of its interval.

    The release's `RECORDS` lists every patient's folder; only the core cohort's are used, and only
    records whose master header is on disk.
    """
    want_windows, want_numerics = wdb.wants("windows"), wdb.wants("numerics")
    window_dir = window_dir or raw_input.parent / "generated" / "waveform"
    sums = read_sha256sums(wdb.root)
    subject_dirs = [
        ln.strip().rstrip("/") for ln in (wdb.root / "RECORDS").read_text().splitlines() if ln.strip()
    ]
    cohort = set(subjects.to_list())
    stats = {
        "listed_patients": len(subject_dirs),
        "patients_outside_core_subjects": 0,
        "patients_not_on_disk": 0,
        "records": 0,
        "records_missing_on_disk": 0,
        "records_without_start_time": 0,
    }
    if want_windows:
        stats |= {"segments": 0, "segments_missing_on_disk": 0, "segments_cut": 0}
        stats["window_s"] = window_s if window_s > 0 else None
    if want_numerics:
        stats |= {"numerics_files": 0, "numerics_files_missing": 0, "records_without_counter_frequency": 0}
        stats["numerics_interval_s"] = numerics_interval_s if numerics_interval_s > 0 else None
    rows, objects, numerics = [], [], []
    for sd in subject_dirs:
        subject = int(Path(sd).name.lstrip("p"))
        if subject not in cohort:
            stats["patients_outside_core_subjects"] += 1
            continue
        pdir = wdb.root / sd
        if not (pdir / "RECORDS").is_file():
            stats["patients_not_on_disk"] += 1
            continue
        for rec_rel in (ln.strip() for ln in (pdir / "RECORDS").read_text().splitlines() if ln.strip()):
            master = pdir / f"{rec_rel}.hea"
            if not master.is_file():
                stats["records_missing_on_disk"] += 1
                continue
            m = parse_wfdb_master(master.read_text())
            if m.start is None:
                stats["records_without_start_time"] += 1
                continue
            stats["records"] += 1
            rdir = master.parent
            hadm = int(m.info["hadm_id"]) if m.info.get("hadm_id", "").isdigit() else None
            record_id = int(m.name) if m.name.isdigit() else None
            if want_numerics:
                fp = rdir / f"{m.name}n.csv.gz"
                if not fp.is_file():
                    stats["numerics_files_missing"] += 1
                elif m.counter_freq is None:
                    stats["records_without_counter_frequency"] += 1
                else:
                    stats["numerics_files"] += 1
                    numerics.append(
                        read_wfdb_numerics(fp, m, numerics_interval_s).with_columns(
                            pl.lit(subject, pl.Int64).alias("subject_id"),
                            pl.lit(hadm, pl.Int64).alias("hadm_id"),
                            pl.lit(record_id, pl.Int64).alias("waveform_record_id"),
                        )
                    )
            if not want_windows:
                continue
            # Whole frames, rounded down: a window never exceeds `window_s`.
            window_samples = max(1, int(window_s * m.fs)) if window_s > 0 else 0
            offset = 0
            for seg, n in m.segments:
                seg_start, offset = offset, offset + n  # in frames from the record's start; gaps count too
                if seg == "~" or n == 0 or not (rdir / f"{seg}.hea").is_file():
                    continue
                signal_files = _segment_files(rdir / f"{seg}.hea")
                if not signal_files:
                    continue
                stats["segments"] += 1
                seg_rels = [
                    (rdir / f).relative_to(wdb.root).as_posix() for f in [f"{seg}.hea", *signal_files]
                ]
                if not all((wdb.root / r).is_file() for r in seg_rels):
                    stats["segments_missing_on_disk"] += 1
                    continue
                if window_samples and n > window_samples:
                    stats["segments_cut"] += 1
                    seg_t0 = m.start + timedelta(seconds=seg_start / m.fs)
                    cut = cut_wfdb_windows(rdir / seg, window_dir / m.name / seg, window_samples, seg_t0)
                    windows = [
                        (name, s, e, [p.resolve().as_posix() for p in files],
                         {p.resolve().as_posix(): sha256_file(p) for p in files})
                        for name, s, e, files in cut
                    ]  # fmt: skip
                else:
                    windows = [(seg, 0, n, seg_rels, hash_files(wdb.root, seg_rels, sums, workers))]
                for k, (name, s, e, files, digests) in enumerate(windows):
                    link = link_for("waveform", object_digest(digests), record_name=name)
                    rows.append({
                        "subject_id": subject, "code": "WAVEFORM//WINDOW",
                        "time": m.start + timedelta(seconds=(seg_start + e) / m.fs), "link": link,
                        "hadm_id": hadm, "waveform_record_id": record_id, "waveform_segment": seg,
                        "waveform_window": k, "waveform_duration_s": (e - s) / m.fs, "encoding": "wfdb",
                    })  # fmt: skip
                    # `wdb.root / f` is `f` itself when `f` is absolute (a window the build wrote).
                    size = sum((wdb.root / f).stat().st_size for f in files)
                    objects.append({
                        "link": link, "modality": "waveform", "encoding": "wfdb", "size_bytes": size,
                        "files": files,
                    })  # fmt: skip
    (raw_input / "waveform").mkdir(parents=True, exist_ok=True)
    skip = set()
    if want_windows:
        schema = {
            "subject_id": pl.Int64, "code": pl.String, "time": pl.Datetime("us"), "link": pl.String,
            "hadm_id": pl.Int64, "waveform_record_id": pl.Int64, "waveform_segment": pl.String,
            "waveform_window": pl.Int64, "waveform_duration_s": pl.Float64, "encoding": pl.String,
        }  # fmt: skip
        events = pl.DataFrame(rows, schema=schema, orient="row")
        events.with_columns(pl.col("time").dt.strftime(TIME_FMT)).write_parquet(
            raw_input / "waveform" / "windows.parquet"
        )
        stats["window_events"] = events.height
        if stats["records_missing_on_disk"] or stats["segments_missing_on_disk"]:
            logger.warning(
                f"waveform: {stats['records_missing_on_disk']} records and "
                f"{stats['segments_missing_on_disk']} segments of cohort patients are not (fully) on disk "
                "and get no event"
            )
    if want_numerics:
        cols = ["subject_id", "code", "time", "numeric_value", "unit", "hadm_id", "waveform_record_id"]
        table = pl.concat(numerics).select(cols) if numerics else None
        stats["numeric_events"] = table.height if table is not None else 0
        if table is None or not table.height:
            skip.add("waveform/numerics")  # MEDS-Extract fails on a configured table without input
        else:
            table.with_columns(pl.col("time").dt.strftime(TIME_FMT)).write_parquet(
                raw_input / "waveform" / "numerics.parquet"
            )
    manifest = pl.DataFrame(objects, schema=OBJECT_SCHEMA, orient="row") if objects else None
    return Staged(
        "waveform",
        stats,
        manifest if manifest is not None else pl.DataFrame(schema=OBJECT_SCHEMA),
        skip_tables=skip,
    )


OBJECT_STAGERS = {"ecg": stage_ecg, "echo": stage_echo, "cxr": stage_cxr, "waveform": stage_waveform}
#: Extensions without objects: `(found, raw_input, subjects) -> Staged`.
TABLE_STAGERS = {"ed": stage_ed, "note": stage_note, "mne": stage_mne, "cxr_db": stage_cxr_db}
