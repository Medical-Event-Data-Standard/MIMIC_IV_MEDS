"""`mimic-iv-meds-multimodal`: build one MEDS dataset from MIMIC-IV plus any combination of extensions.

    mimic-iv-meds-multimodal --root /data/mimic-demo-multimodal --output-dir /data/meds_mm \\
        --extensions mimic-iv-ed,mimic-iv-note,mimic-iv-mne,mimic-iv-ecg,mimic-iv-echo,mimic-iv-cxr

`--root` (repeatable) holds the PhysioNet releases side by side, in PhysioNet's own layouts (see
`modules.py`), such as the folder `mimic-demo-mm download` fills: the core MIMIC-IV (demo or full)
is required; each extension in `--extensions` must be there too (`--list-extensions` lists them).
`--download-demo` first runs that downloader (the optional `mimic-demo-mm` package) for the
extensions asked for. The output is a MEDS root with the objects inside it:

    <output-dir>/
      data/{train,tuning,held_out}/*.parquet   # one event per note / ECG / DICOM / image / waveform window
      metadata/dataset.json                    # name, versions, license, source_datasets, column categories
      metadata/codes.parquet, subject_splits.parquet
      metadata/sources.json                    # every release used, and what each contributed (counts)
      external_values/                         # every object an event links to, and nothing else
        ecg/<sha[:2]>/<sha>/<record>.hea, .dat
        echo/<sha[:2]>/<sha>.dcm
        cxr/<sha[:2]>/<sha>.jpg
        waveform/<sha[:2]>/<sha>/<window>.hea, .dat

Each object event has `external_value_link` (the path above, relative to the MEDS root) and
`external_value_encoding` (`wfdb`, `dicom` or `jpeg`; see `external_values.py`).

Steps: stage the raw input and the object manifest (`staging.py`), compose the spec for exactly
these modules (`compose.py`), run `meds-extract-run` on it without downloading, then place the
objects and write the source metadata (`finalize.py`). Logs show counts, never rows.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import polars as pl

from . import compose, finalize, staging
from .modules import CORE, MODULES, NAMES, describe, find_release, parse_selection

logger = logging.getLogger("MIMIC_IV_MEDS.multimodal")

ORDER = list(MODULES)  # canonical module order, whatever order they are asked in
WORK = ".multimodal"


#: Extension -> the `mimic-demo-mm` sources that hold its demo subset (MNE and CXR-DB have none).
DEMO_DOWNLOAD_SOURCES = {
    "core": ("mimic-iv-demo",),
    "ed": ("mimic-iv-ed-demo",),
    "note": ("mimic-iv-note",),
    "ecg": ("mimic-iv-ecg-demo",),
    "echo": ("mimic-iv-echo",),
    "cxr": ("mimic-cxr-jpg",),
    "waveform": ("mimic4wdb",),
}


def _kv(values: list[str], flag: str) -> dict[str, str]:
    """`<extension>=<value>` pairs, keyed by internal module name (`core` for MIMIC-IV itself)."""
    out = {}
    for v in values:
        k, sep, val = v.partition("=")
        name = "core" if k in ("core", "mimic-iv", "mimiciv") else NAMES.get(k)
        if not sep or name is None:
            raise SystemExit(
                f"{flag} takes <extension>=<value> with extension core or one of "
                f"{','.join(m.names[0] for m in MODULES.values())}; got {v!r}"
            )
        out[name] = val
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mimic-iv-meds-multimodal",
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--root",
        type=Path,
        action="append",
        default=[],
        help="folder holding the PhysioNet releases (repeatable: every one is searched; add this "
        "repository's data/synthetic_demo to fill MNE and CXR-DB with simulated data)",
    )
    p.add_argument("--output-dir", type=Path)
    p.add_argument(
        "--extensions",
        "--modules",
        dest="modules",
        default="",
        help=f"comma-separated, any of: {','.join(m.names[0] for m in MODULES.values())}, or 'all'; "
        "empty for MIMIC-IV alone. An extension can be narrowed to some of its parts: "
        "mimic-iv-note:radiology, mimic-iv-echo:measurements (see --list-extensions)",
    )
    p.add_argument(
        "--list-extensions",
        "--list-modules",
        dest="list_modules",
        action="store_true",
        help="list the extensions, their names, parts and releases",
    )
    p.add_argument(
        "--download-demo",
        action="store_true",
        help="first download the multimodal demo for these extensions into the first --root, with "
        "`mimic-demo-mm download` (install it with the `demo-download` extra)",
    )
    p.add_argument(
        "--yes", "-y", action="store_true", help="with --download-demo: don't ask before downloading"
    )
    p.add_argument(
        "--link-mode",
        choices=finalize.LINK_MODES,
        default="copy",
        help="how objects get into the MEDS root (copy suits a demo; hardlink/symlink full data)",
    )
    p.add_argument(
        "--path",
        action="append",
        default=[],
        metavar="MODULE=DIR",
        help="use DIR for MODULE instead of searching --root (repeatable)",
    )
    p.add_argument(
        "--version",
        action="append",
        default=[],
        metavar="MODULE=VERSION",
        help="pick or state a module's release version (repeatable)",
    )
    p.add_argument(
        "--common-dir",
        type=Path,
        action="append",
        default=[],
        help="folder with the base spec's concept-map CSVs (d_labitems_to_loinc.csv, ...); "
        "missing ones are downloaded into --cache-dir",
    )
    p.add_argument(
        "--cache-dir",
        type=Path,
        default=Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "MIMIC_IV_MEDS",
    )
    p.add_argument(
        "--waveform-window-s",
        type=float,
        default=staging.WAVEFORM_WINDOW_S,
        help="cut waveform segments longer than this many seconds into windows of this length, each a "
        "linked object of its own (needs the `external-values` extra); 0 keeps whole segments",
    )
    p.add_argument(
        "--waveform-numerics-interval-s",
        type=float,
        default=staging.NUMERICS_INTERVAL_S,
        help="summarize each waveform vital sign as its median over intervals of this many seconds, "
        "timed at the end of each interval; 0 keeps every reading at its own time",
    )
    p.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1), help="threads for hashing")
    p.add_argument(
        "--no-enrich", action="store_true", help="skip the suMEDS code-metadata enrichment (offline)"
    )
    p.add_argument("--overwrite", action="store_true", help="replace an existing build in --output-dir")
    p.add_argument(
        "--keep-staging",
        action="store_true",
        help=f"keep <output-dir>/{WORK}/raw_input and MEDS-Extract's per-stage intermediate output",
    )
    a = p.parse_args(argv)
    if a.list_modules:
        print(describe())
        raise SystemExit(0)
    if not a.root or a.output_dir is None:
        p.error("--root and --output-dir are required")
    try:
        a.modules = parse_selection(a.modules)
    except ValueError as e:
        p.error(str(e))
    a.path = {k: Path(v) for k, v in _kv(a.path, "--path").items()}
    a.version = _kv(a.version, "--version")
    return a


def _prepare_output(out: Path, overwrite: bool) -> None:
    if out.exists() and any(out.iterdir()):
        looks_like_build = (out / "metadata" / "dataset.json").exists() or (out / WORK).exists()
        if not overwrite:
            raise SystemExit(f"{out} is not empty; pass --overwrite to replace a previous build")
        if not looks_like_build:
            raise SystemExit(
                f"{out} is not empty and does not look like a previous build; refusing to delete it"
            )
        shutil.rmtree(out)
    out.mkdir(parents=True, exist_ok=True)


def run_meds_extract(spec_fp: Path, out: Path, raw_input: Path, enrich: bool, cleanup: bool) -> None:
    scripts = sysconfig.get_path("scripts")
    exe = shutil.which("meds-extract-run", path=scripts) or shutil.which("meds-extract-run")
    if exe is None:
        raise SystemExit("meds-extract-run not found; install MIMIC-IV-MEDS with its dependencies")
    args = [exe, f"spec={spec_fp}", f"output_dir={out}", "do_download=false", f"input_dir={raw_input}"]
    if not enrich:
        args.append("skip_stages=[sumeds_enrich]")
    if cleanup:
        args.append("do_cleanup_intermediates=true")
    # The runner spawns each stage as its own console script: they must be on PATH.
    env = {**os.environ, "PATH": os.pathsep.join([scripts, os.environ.get("PATH", "")])}
    logger.info("running meds-extract-run")
    if (rc := subprocess.run(args, env=env).returncode) != 0:
        raise SystemExit(f"meds-extract-run failed (exit {rc}); see {out}/.logs")


def download_demo(root: Path, modules: list[str], yes: bool) -> None:
    """Fill `root` with the multimodal demo's sources for `modules`, via the `mimic-demo-mm` package."""
    import importlib.util

    if importlib.util.find_spec("mimic_demo_mm") is None:
        raise SystemExit(
            "--download-demo needs the mimic-demo-mm package: pip install 'MIMIC_IV_MEDS[demo-download]' "
            "(or run `uvx --from git+https://github.com/rvandewater/MIMIC-IV-Multimodal-Demo mimic-demo-mm "
            f"download --out {root}` yourself and drop --download-demo)"
        )
    sources = [s for m in ("core", *modules) for s in DEMO_DOWNLOAD_SOURCES.get(m, ())]
    if missing := [MODULES[m].names[0] for m in modules if m not in DEMO_DOWNLOAD_SOURCES]:
        logger.warning(
            f"the multimodal demo has no {', '.join(missing)}; add a --root with a release of it "
            "(e.g. this repository's data/synthetic_demo for simulated data)"
        )
    args = [sys.executable, "-m", "mimic_demo_mm", "download", "--out", str(root), "--only", *sources]
    if yes:
        args.append("--yes")
    logger.info(f"downloading the multimodal demo ({', '.join(sources)}) into {root}")
    # exit 1 means the file check found something incomplete; the release search below will tell.
    if (rc := subprocess.run(args).returncode) not in (0, 1):
        raise SystemExit(f"mimic-demo-mm download failed (exit {rc})")


def build(a: argparse.Namespace) -> Path:
    roots, out = [r.resolve() for r in a.root], a.output_dir.resolve()
    if a.download_demo:
        roots[0].mkdir(parents=True, exist_ok=True)
        download_demo(roots[0], list(a.modules), a.yes)
    core = find_release(CORE, roots, a.path.get("core"), a.version.get("core"))
    found = [
        find_release(MODULES[m], roots, a.path.get(m), a.version.get(m), parts, explicit)
        for m, (parts, explicit) in a.modules.items()
    ]
    for f in [core, *found]:
        kind = "SYNTHETIC" if f.synthetic else "demo" if f.release.demo else "full"
        logger.info(f"{f.module.names[0]:>16}: {f.tag} ({kind}, {f.license}) at {f.root}")
        if f.synthetic:
            logger.warning(
                f"{f.module.names[0]} is SIMULATED data ({f.root}): for pipeline testing and tutorials "
                "only, not for research; dataset.json and sources.json say so"
            )

    _prepare_output(out, a.overwrite)
    work = out / WORK
    raw_input = work / "raw_input"

    staged = {"core": staging.stage_core(core, raw_input)}
    staging.stage_common(
        raw_input, compose.common_sources(), [*a.common_dir, core.root], a.cache_dir / "common"
    )
    subjects = staging.cohort_subjects(core)
    for f in found:
        name = f.module.name
        if name in staging.TABLE_STAGERS:
            staged[name] = staging.TABLE_STAGERS[name](f, raw_input, subjects)
        elif name == "waveform":
            staged[name] = staging.stage_waveform(
                f,
                raw_input,
                subjects,
                a.workers,
                a.waveform_window_s,
                work / "generated" / "waveform",
                a.waveform_numerics_interval_s,
            )
        else:
            staged[name] = staging.OBJECT_STAGERS[name](f, raw_input, subjects, a.workers)
        logger.info(f"{f.module.names[0]:>16}: staged {staged[name].stats}")

    skip = set().union(*(s.skip_tables for s in staged.values()))
    spec_fp = compose.write_spec(compose.compose(core, found, skip), work / "spec.yaml")
    run_meds_extract(spec_fp, out, raw_input, enrich=not a.no_enrich, cleanup=not a.keep_staging)

    manifests = [s.objects for s in staged.values() if s.objects is not None]
    objects = pl.concat(manifests) if manifests else pl.DataFrame(schema=staging.OBJECT_SCHEMA)
    placed = finalize.place_objects(objects, {f.module.name: f.root for f in found}, out, a.link_mode)
    finalize.write_sources(out, core, found, staged, placed, a.link_mode, skip)
    finalize.add_source_datasets(out, core, found, skip)
    link_cols = compose.link_columns(found, skip)
    finalize.check_dataset_json(out, link_cols)
    counts = finalize.check_external_values(out)
    logger.info(
        f"external_values: {counts['links']} distinct links, all resolve; {counts['files']} files, "
        "each belonging to a linked object"
    )
    if not a.keep_staging:
        shutil.rmtree(raw_input)
        shutil.rmtree(work / "generated", ignore_errors=True)
    logger.info(f"done: {out}")
    return out


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    build(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
