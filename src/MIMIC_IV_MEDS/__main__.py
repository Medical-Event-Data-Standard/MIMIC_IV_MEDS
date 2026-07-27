#!/usr/bin/env python

import logging
import os
from pathlib import Path

import hydra
from omegaconf import DictConfig

from . import ETL_CFG, EVENT_CFG, HAS_PRE_MEDS, MAIN_CFG, dataset_info
from . import __version__ as PKG_VERSION
from .commands import coerce_download_workers, resolve_console_script, run_command

if HAS_PRE_MEDS:
    from .pre_MEDS import main as pre_MEDS_transform

logger = logging.getLogger(__name__)


@hydra.main(version_base=None, config_path=str(MAIN_CFG.parent), config_name=MAIN_CFG.stem)
def main(cfg: DictConfig):
    """Runs the end-to-end MEDS Extraction pipeline."""

    raw_input_dir = Path(cfg.raw_input_dir)
    pre_MEDS_dir = Path(cfg.pre_MEDS_dir)
    MEDS_output_dir = Path(cfg.MEDS_output_dir)
    stage_runner_fp = cfg.get("stage_runner_fp", None)

    # Step 0: Data downloading, via MEDS-Extract's download layer. The `sources:` block
    # in the event config declares the buckets; `key=` selects `demo` or `dataset` (the
    # `common` concept-map bucket is always appended). Verified-existing files are
    # skipped, so resumed runs only fetch what is missing.
    if cfg.do_download:
        download_workers = coerce_download_workers(cfg.get("download_workers", 1))
        key = "demo" if cfg.get("do_demo", False) else "dataset"
        logger.info(f"Downloading '{key}' + 'common' source buckets (concurrency={download_workers}).")
        raw_input_dir.mkdir(parents=True, exist_ok=True)
        run_command(
            [
                resolve_console_script("meds-extract-download"),
                f"spec={EVENT_CFG.resolve()!s}",
                f"raw_input_dir={raw_input_dir.resolve()!s}",
                f"key={key}",
                f"concurrency={download_workers}",
            ]
        )
    else:  # pragma: no cover
        logger.info("Skipping data download.")

    # Step 1: Pre-MEDS Data Wrangling
    if HAS_PRE_MEDS:
        pre_MEDS_transform(
            input_dir=raw_input_dir,
            output_dir=pre_MEDS_dir,
            do_overwrite=cfg.get("do_overwrite", None),
            do_copy=cfg.get("do_copy", None),
        )
    else:
        pre_MEDS_dir = raw_input_dir

    # Step 2: MEDS Cohort Creation
    env = {
        "DATASET_NAME": dataset_info.dataset_name,
        "DATASET_VERSION": f"{dataset_info.raw_dataset_version}:{PKG_VERSION}",
        "EVENT_CONVERSION_CONFIG_FP": str(EVENT_CFG.resolve()),
        "PRE_MEDS_DIR": str(pre_MEDS_dir.resolve()),
    }

    # Resolve via the venv's bin/ next to sys.executable rather than relying on PATH —
    # users who invoke `./venvs/.../MEDS_extract-MIMIC_IV` without first activating the
    # venv would otherwise hit FileNotFoundError on this subprocess even though the
    # script is installed in the same environment as the python interpreter calling it.
    command_parts = [resolve_console_script("MEDS_transform-pipeline"), str(ETL_CFG.resolve())]

    if stage_runner_fp:
        command_parts.append(f"--stage_runner_fp={stage_runner_fp}")
    if cfg.get("do_profile", False):
        command_parts.append("--do_profile")

    # Build overrides list
    overrides = [f"output_dir={MEDS_output_dir.resolve()!s}"]

    if cfg.get("do_overwrite") is not None:
        overrides.append(f"do_overwrite={cfg.do_overwrite}")
    if cfg.get("seed") is not None:
        overrides.append(f"seed={cfg.seed}")
    if int(os.getenv("N_WORKERS", 1)) <= 1:
        overrides.append("~parallelize")  # disable joblib for serial execution
    # Add any overrides to the command
    if overrides:
        command_parts.append("--overrides")
        command_parts.extend(overrides)
    run_command(command_parts, env=env)


if __name__ == "__main__":
    main()
