#!/usr/bin/env python

import logging
import os
from pathlib import Path

import hydra
from omegaconf import DictConfig

from . import ETL_CFG, EVENT_CFG, HAS_PRE_MEDS, MAIN_CFG, dataset_info
from . import __version__ as PKG_VERSION
from .commands import run_command
from .download import coerce_download_workers, download_data

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

    # Step 0: Data downloading
    if cfg.do_download:
        # Single shared coercer keeps the CLI and library API in lockstep on what counts
        # as a valid `download_workers` value and on how invalid input is reported.
        download_workers = coerce_download_workers(cfg.get("download_workers", 1))
        # Don't lie about parallelism in the log — workers=1 is sequential.
        if download_workers == 1:
            workers_blurb = "sequentially (1 worker)"
        else:
            workers_blurb = f"with {download_workers} parallel workers"
        if cfg.get("do_demo", False):
            logger.info(f"Downloading demo data {workers_blurb}.")
            download_data(
                raw_input_dir,
                dataset_info,
                do_demo=True,
                download_workers=download_workers,
            )
        else:
            logger.info(f"Downloading data {workers_blurb}.")
            download_data(
                raw_input_dir,
                dataset_info,
                download_workers=download_workers,
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

    command_parts = ["MEDS_transform-pipeline", str(ETL_CFG.resolve())]

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
