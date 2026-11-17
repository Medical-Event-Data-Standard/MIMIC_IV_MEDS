"""The ``mimic_iv_unit_keys`` pre-stage: which infusion codes need their unit in the code.

An infusion start is ``INFUSION_START//{itemid}`` with the rate (unit ``rateuom``) as its value, and
an infusion end is ``INFUSION_END//{itemid}`` with the amount (unit ``amountuom``). Most itemids record
one unit, but some record several, which would give their code several units. Only the codes of those
itemids get the unit as their last component (``INFUSION_START//{itemid}//{rateuom}``,
``INFUSION_END//{itemid}//{amountuom}``), decided separately for starts and ends; every other code
stays as it was.

Which itemids those are depends on the data, so this stage writes them as a side table,
``inputevents_unit_in_code`` (one row per such itemid, with ``start_unit_in_code`` and
``end_unit_in_code`` true where the starts or ends mix units, null otherwise), that the
``icu/inputevents`` block of ``event_configs.yaml`` joins on the itemid. MEDS-Extract reads the staged
directory before the raw one (``input_overlay_dirs``).
"""

import logging
import os
from pathlib import Path
from typing import Any

import polars as pl
from MEDS_extract.io import resolve_source_files
from MEDS_transforms.stages import Stage

logger = logging.getLogger(__name__)

STAGE_NAME = "mimic_iv_unit_keys"
SIDE_TABLE_NAME = "inputevents_unit_in_code"
INPUTEVENTS = "icu/inputevents"

#: Per code family: the flag column of the side table, the event time, and the unit of the value.
FAMILIES = {
    "start_unit_in_code": ("starttime", "rateuom"),
    "end_unit_in_code": ("endtime", "amountuom"),
}


def itemids_with_unit_in_code(inputevents: pl.LazyFrame) -> pl.DataFrame:
    """The itemids whose starts or ends are recorded in more than one unit.

    Only rows that become events count (a time and an itemid). Units are compared stripped, with
    blanks and nulls left out, as the ``mimic_iv_collapse_unit_metadata`` post-stage collapses them.

    Examples:
        >>> inputevents = pl.LazyFrame({
        ...     "itemid": ["1", "1", "1", "2", "2", "3", "3", "4"],
        ...     "starttime": ["t"] * 7 + [None],
        ...     "endtime": ["t"] * 8,
        ...     "rateuom": ["mg/hour", "mcg/hour", None, "mL/hour", " mL/hour", None, "mL/hour", "mg/hour"],
        ...     "amountuom": ["mg", "mg", "mg", "mL", "mL", "mL", "L", "mL"],
        ... })
        >>> itemids_with_unit_in_code(inputevents).rows()
        [(1, True, None), (3, None, True)]
    """
    flags = []
    for flag, (time, unit) in FAMILIES.items():
        stripped = pl.col(unit).cast(pl.String).str.strip_chars()
        flags.append(
            inputevents.filter(pl.col(time).is_not_null() & pl.col("itemid").is_not_null())
            .select(pl.col("itemid").cast(pl.Int64), stripped.alias("unit"))
            .filter(pl.col("unit").is_not_null() & (pl.col("unit") != ""))
            .group_by("itemid")
            .agg(pl.col("unit").n_unique().alias("n"))
            .filter(pl.col("n") > 1)
            .select("itemid", pl.lit(True).alias(flag))
        )
    start, end = flags
    return start.join(end, on="itemid", how="full", coalesce=True).sort("itemid").collect(engine="streaming")


def write_unit_keys(raw_input_dir: Path, output_dir: Path) -> Path:
    """Write the ``inputevents_unit_in_code`` side table of the release in ``raw_input_dir``.

    Returns:
        The written file, replaced atomically.
    """
    columns = ["itemid", *(c for pair in FAMILIES.values() for c in pair)]
    scans = []
    for fp in resolve_source_files(Path(raw_input_dir), INPUTEVENTS):
        if "".join(fp.suffixes).lower().endswith((".parquet", ".par")):
            scans.append(pl.scan_parquet(fp, glob=False).select(columns))
        else:
            # All columns as read: only the itemid is cast, and the units are compared as Strings.
            scans.append(pl.scan_csv(fp, infer_schema=False).select(columns))
    table = itemids_with_unit_in_code(pl.concat(scans, how="vertical_relaxed"))
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / f"{SIDE_TABLE_NAME}.parquet"
    tmp = out.with_name(f".{out.name}.tmp")
    table.write_parquet(tmp)
    os.replace(tmp, out)
    logger.info(
        f"{table['start_unit_in_code'].sum()} itemids have starts and {table['end_unit_in_code'].sum()} "
        "have ends in more than one unit; their codes end in it."
    )
    return out


def _main(cfg: Any) -> None:
    """Write the ``inputevents_unit_in_code`` side table of the release in ``input_dir``."""
    if cfg.worker != 0:
        return
    write_unit_keys(Path(cfg.input_dir), Path(cfg.stage_cfg.output_dir))


main = Stage.register(main_fn=_main, stage_name=STAGE_NAME, is_metadata=False, default_config={})
