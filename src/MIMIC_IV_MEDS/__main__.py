#!/usr/bin/env python

import logging
from pathlib import Path

import hydra
from omegaconf import DictConfig

from . import ETL_CFG, EVENT_CFG, HAS_PRE_MEDS, MAIN_CFG
from . import __version__ as PKG_VERSION
from . import dataset_info
from .commands import run_command
from .download import download_data

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
        if cfg.get("do_demo", False):
            logger.info("Downloading demo data.")
            download_data(raw_input_dir, dataset_info, do_demo=True)
        else:
            logger.info("Downloading data.")
            download_data(raw_input_dir, dataset_info)
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
    # First we need to set some environment variables
    command_parts = [
        f"DATASET_NAME={dataset_info.dataset_name}",
        f"DATASET_VERSION={dataset_info.raw_dataset_version}:{PKG_VERSION}",
        f"EVENT_CONVERSION_CONFIG_FP={str(EVENT_CFG.resolve())}",
        f"PRE_MEDS_DIR={str(pre_MEDS_dir.resolve())}",
        f"MEDS_OUTPUT_DIR={str(MEDS_output_dir.resolve())}",
    ]

    command_parts.append("MEDS_transform-pipeline")
    command_parts.append(str(ETL_CFG.resolve()))

    if stage_runner_fp:
        command_parts.append(f"stage_runner_fp={stage_runner_fp}")

    # Build overrides list
    overrides = []

    # Add output_dir as it's required by the pipeline
    overrides.append(f"output_dir={str(MEDS_output_dir.resolve())}")

    # Add any overrides to the command
    if overrides:
        command_parts.append("--overrides")
        command_parts.extend(overrides)
    run_command(command_parts, cfg=None)


if __name__ == "__main__":
    main()
