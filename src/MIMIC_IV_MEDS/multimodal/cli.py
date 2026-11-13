"""`mimic-iv-meds-multimodal`: build one MEDS dataset from MIMIC-IV plus any combination of modules.

    mimic-iv-meds-multimodal --root /data/MIMIC-IV_Multimodal_Demo --output-dir /data/meds_mm \\
        --modules ed,note,ecg,echo,cxr --link-mode copy

`--root` holds the PhysioNet releases side by side, in PhysioNet's own layout (see `modules.py`):
the core MIMIC-IV (demo or full) is required; each module in `--modules` must be there too. The
output is a MEDS root with the objects inside it:

    <output-dir>/
      data/{train,tuning,held_out}/*.parquet   # one event per note / ECG / echo DICOM / CXR image
      metadata/dataset.json                    # name, versions, license, additional_value_modality_columns
      metadata/codes.parquet, subject_splits.parquet
      metadata/objects.parquet                 # link -> release, source path, sha256, size, format
      metadata/sources.json                    # every release used, and what each contributed (counts)
      ecg/<sha[:2]>/<sha>/<record>.hea, .dat
      echo/<sha[:2]>/<sha>.dcm
      cxr/<sha[:2]>/<sha>.jpg

Steps: stage the raw input and the object manifest (`staging.py`), compose the spec for exactly
these modules (`compose.py`), run `meds-extract-run` on it without downloading, then place the
objects and write the object and source metadata (`finalize.py`). Logs show counts, never rows.
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
from .modules import CORE, MODULES, find_release

logger = logging.getLogger("MIMIC_IV_MEDS.multimodal")

ORDER = list(MODULES)  # canonical module order, whatever order they are asked in
WORK = ".multimodal"


def _kv(values: list[str], flag: str) -> dict[str, str]:
    out = {}
    for v in values:
        k, sep, val = v.partition("=")
        if not sep or k not in ("core", *MODULES):
            raise SystemExit(
                f"{flag} takes <module>=<value> with module one of core,{','.join(MODULES)}; got {v!r}"
            )
        out[k] = val
    return out


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="mimic-iv-meds-multimodal",
        description=__doc__.split("\n\n")[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--root", type=Path, required=True, help="folder holding the PhysioNet releases")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument(
        "--modules",
        default="",
        help=f"comma-separated, any of: {','.join(MODULES)} ('all' for every one; empty for the core alone)",
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
    mods = [m.strip() for m in a.modules.split(",") if m.strip()]
    if mods == ["all"]:
        mods = list(MODULES)
    if bad := [m for m in mods if m not in MODULES]:
        p.error(f"unknown module(s) {bad}; choose from {','.join(MODULES)}")
    a.modules = sorted(set(mods), key=ORDER.index)
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


def build(a: argparse.Namespace) -> Path:
    root, out = a.root.resolve(), a.output_dir.resolve()
    core = find_release(CORE, root, a.path.get("core"), a.version.get("core"))
    found = [find_release(MODULES[m], root, a.path.get(m), a.version.get(m)) for m in a.modules]
    for f in [core, *found]:
        logger.info(
            f"{f.module.name:>4}: {f.tag} ({'demo' if f.release.demo else 'full'}, {f.release.license})"
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
        if name == "ed":
            staged[name] = staging.stage_ed(f, raw_input, subjects)
        elif name == "note":
            staged[name] = staging.stage_note(f, raw_input, subjects)
        else:
            staged[name] = staging.OBJECT_STAGERS[name](f, raw_input, subjects, a.workers)
        logger.info(f"{name:>4}: staged {staged[name].stats}")

    skip = set().union(*(s.skip_tables for s in staged.values()))
    spec_fp = compose.write_spec(compose.compose(core, found, skip), work / "spec.yaml")
    run_meds_extract(spec_fp, out, raw_input, enrich=not a.no_enrich, cleanup=not a.keep_staging)

    manifests = [s.objects for s in staged.values() if s.objects is not None]
    objects = pl.concat(manifests) if manifests else pl.DataFrame(schema=staging.OBJECT_SCHEMA)
    placed = finalize.place_objects(objects, {f.module.name: f.root for f in found}, out, a.link_mode)
    finalize.write_objects_table(objects, out)
    finalize.write_sources(out, core, found, staged, placed, a.link_mode)
    link_cols = [f.module.link_column for f in found if f.module.link_column]
    finalize.check_dataset_json(out, link_cols)
    for col, n in finalize.check_links(out, link_cols).items():
        logger.info(f"{col}: {n} distinct links, all resolve")
    if not a.keep_staging:
        shutil.rmtree(raw_input)
    logger.info(f"done: {out}")
    return out


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s: %(message)s")
    build(parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
