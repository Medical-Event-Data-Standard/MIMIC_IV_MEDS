#!/usr/bin/env python

import logging
import os
from pathlib import Path

import hydra
from omegaconf import DictConfig

from . import ETL_CFG, EVENT_CFG, HAS_PRE_MEDS, MAIN_CFG, dataset_info
from . import __version__ as PKG_VERSION
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
        # Treat an absent or null config value as the default of 1; reject anything that
        # can't be coerced to a positive integer with a clear error so config typos don't
        # surface later as a confusing log line ("Downloading data with -1 parallel workers"
        # would otherwise just silently take the sequential path inside download_data).
        raw_workers = cfg.get("download_workers", 1)
        if raw_workers is None:
            raw_workers = 1
        # Reject bool explicitly before int() — `int(True) == 1` would silently take the
        # sequential path, but `download_workers: true` in YAML is almost certainly a
        # config typo (or an attempt to express "yes parallelize" without picking a count).
        # Rejecting here keeps the error consistent with download_data's same guard.
        if isinstance(raw_workers, bool):
            raise ValueError(f"download_workers must be an integer, got {raw_workers!r} (bool)")
        try:
            download_workers = int(raw_workers)
        except (TypeError, ValueError) as e:
            raise ValueError(f"download_workers must be an integer, got {raw_workers!r}") from e
        if download_workers < 1:
            raise ValueError(f"download_workers must be >= 1, got {download_workers}")
        if cfg.get("do_demo", False):
            logger.info("Downloading demo data.")
            download_data(
                raw_input_dir,
                dataset_info,
                do_demo=True,
                download_workers=download_workers,
            )
        else:
            logger.info(f"Downloading data with {download_workers} parallel workers.")
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
