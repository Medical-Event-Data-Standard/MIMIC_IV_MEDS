"""Performs pre-MEDS data wrangling for MIMIC-IV.

After the migration to MEDS-Extract 0.7 (see #58), the remaining pre-MEDS transformations are:

1. ICD code dot normalization for metadata tables (d_icd_diagnoses, d_icd_procedures).
   The transformation itself is now expressible in dftly (>= 0.4), but these columns are
   consumed via ``_metadata`` blocks, whose values are still parsed by the MEDS-transforms
   DSL rather than dftly. Tracked upstream: mmcdermott/MEDS_extract#146.

2. Death time join for hosp/patients — joining the earliest ``deathtime`` per subject from
   admissions requires a group_by().min() aggregation before joining, which the MEDS-extract
   join config does not yet support. Tracked upstream: mmcdermott/MEDS_extract#65 (fix in
   flight in mmcdermott/MEDS_extract#98). Only the aggregated join lives here; the
   deathtime-vs-dod coalescing and format parsing happen declaratively in the event config
   via non-strict casts (dftly >= 0.1.3).

The discharge-time joins (diagnoses_icd, procedures_icd, drgcodes), DOB computation, and
mixed-format time parsing (pharmacy, death times) are all handled declaratively via the
event config's ``_table`` and ``coalesce(...::?"fmt")`` features.
"""

import logging
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl
from MEDS_extract.io import resolve_source_files, scan_source
from MEDS_transforms.dataframe import write_df

logger = logging.getLogger(__name__)

# Passed to csv-family scans; ``scan_source`` drops it for parquet inputs.
CSV_INFER_SCHEMA_LENGTH = 100000


def get_shard_prefix(base_path: Path, fp: Path) -> str:
    """Extracts the relative path of ``fp`` under ``base_path``, without data-format suffixes.

    This mirrors the helper MEDS-Extract shipped through 0.6.x (removed in 0.7).

    Args:
        base_path: The root directory ``fp`` lives under.
        fp: The file path to extract the prefix from.

    Returns:
        The relative path of the file under the base path, minus all suffixes.

    Examples:
        >>> get_shard_prefix(Path("/data"), Path("/data/hosp/admissions.csv.gz"))
        'hosp/admissions'
        >>> get_shard_prefix(Path("/data"), Path("/data/demo_subject_id.csv"))
        'demo_subject_id'
    """
    relative_path = fp.relative_to(base_path)
    relative_parent = relative_path.parent
    file_name = relative_path.name.split(".")[0]
    return str(relative_parent / file_name)


def add_dot(code: pl.Expr, position: int) -> pl.Expr:
    """Adds a dot to the code expression at the specified position.

    Args:
        code: The code expression.
        position: The position to add the dot.

    Returns:
        The expression which would yield the code string with a dot added at the specified position

    Example:
        >>> pl.select(add_dot(pl.lit("12345"), 3))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 123.45  │
        └─────────┘
        >>> pl.select(add_dot(pl.lit("12345"), 1))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 1.2345  │
        └─────────┘
        >>> pl.select(add_dot(pl.lit("12345"), 6))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 12345   │
        └─────────┘
    """
    return (
        pl.when(code.str.len_chars() > position)
        .then(code.str.slice(0, position) + "." + code.str.slice(position))
        .otherwise(code)
    )


def add_icd_diagnosis_dot(icd_version: pl.Expr, icd_code: pl.Expr) -> pl.Expr:
    """Adds the appropriate dot to the ICD diagnosis codebased on the version.

    Args:
        icd_version: The ICD version.
        icd_code: The ICD code.

    Returns:
        The ICD code with appropriate dot syntax based on the version.

    Examples:
        >>> pl.select(add_icd_diagnosis_dot(pl.lit("9"), pl.lit("12345")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 123.45  │
        └─────────┘
        >>> pl.select(add_icd_diagnosis_dot(pl.lit("9"), pl.lit("E1234")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ E123.4  │
        └─────────┘
        >>> pl.select(add_icd_diagnosis_dot(pl.lit("9"), pl.lit("F1234")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ F12.34  │
        └─────────┘
        >>> pl.select(add_icd_diagnosis_dot(pl.lit("10"), pl.lit("12345")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 123.45  │
        └─────────┘
        >>> pl.select(add_icd_diagnosis_dot(pl.lit("10"), pl.lit("E1234")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ E12.34  │
        └─────────┘
    """

    icd9_code = (
        pl.when(icd_code.str.starts_with("E")).then(add_dot(icd_code, 4)).otherwise(add_dot(icd_code, 3))
    )

    icd10_code = add_dot(icd_code, 3)

    return pl.when(icd_version == "9").then(icd9_code).otherwise(icd10_code)


def add_icd_procedure_dot(icd_version: pl.Expr, icd_code: pl.Expr) -> pl.Expr:
    """Adds the appropriate dot to the ICD procedure code based on the version.

    Args:
        icd_version: The ICD version.
        icd_code: The ICD code.

    Returns:
        The ICD code with appropriate dot syntax based on the version.

    Examples:
        >>> pl.select(add_icd_procedure_dot(pl.lit("9"), pl.lit("12345")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 12.345  │
        └─────────┘
        >>> pl.select(add_icd_procedure_dot(pl.lit("10"), pl.lit("12345")))
        shape: (1, 1)
        ┌─────────┐
        │ literal │
        │ ---     │
        │ str     │
        ╞═════════╡
        │ 12345   │
        └─────────┘
    """

    icd9_code = add_dot(icd_code, 2)
    icd10_code = icd_code

    return pl.when(icd_version == "9").then(icd9_code).otherwise(icd10_code)


def fix_static_data(raw_static_df: pl.LazyFrame, death_times_df: pl.LazyFrame) -> pl.LazyFrame:
    """Joins the earliest per-subject ``deathtime`` from admissions into the patients table.

    Only the aggregated join happens here — the event config coalesces the joined
    ``deathtime`` (full datetime) with the raw date-only ``dod`` declaratively via
    non-strict casts, so no parsing or normalization is needed in Python. All original
    columns are preserved so downstream ``_table.cols`` expressions (e.g.,
    ``year_of_birth = $anchor_year - $anchor_age``) can reference them.

    This aggregated join (group_by + min) cannot be expressed in the MEDS-extract join
    config, which only supports flat left joins. Tracked upstream:
    mmcdermott/MEDS_extract#65 (fix in flight in mmcdermott/MEDS_extract#98).

    Args:
        raw_static_df: The raw static data.
        death_times_df: The death times data (from admissions).

    Returns:
        The static data with the earliest per-subject ``deathtime`` joined on, and all
        original columns (including raw ``dod``) preserved.
    """

    death_times_df = death_times_df.group_by("subject_id").agg(pl.col("deathtime").min())

    return raw_static_df.join(death_times_df, on="subject_id", how="left")


FUNCTIONS = {
    "hosp/patients": (
        fix_static_data,
        ("hosp/admissions", ["subject_id", "deathtime"]),
    ),
}

ICD_DFS_TO_FIX = [
    ("hosp/d_icd_diagnoses", add_icd_diagnosis_dot),
    ("hosp/d_icd_procedures", add_icd_procedure_dot),
]


def main(
    input_dir: Path,
    output_dir: Path,
    do_overwrite: bool | None = None,
    do_copy: bool | None = None,
):
    """Performs pre-MEDS data wrangling for MIMIC-IV.

    Inputs are the raw MIMIC files, read from the `input_dir` config parameter. Output files are either
    symlinked (if they are not modified) or written in processed form to the `output_dir` config
    parameter. Hydra is used to manage configuration parameters and logging.
    """

    done_fp = output_dir / ".done"
    if done_fp.is_file() and not do_overwrite:
        logger.info(
            f"Pre-MEDS transformation already complete as {done_fp} exists and "
            f"do_overwrite={do_overwrite}. Returning."
        )
        return

    all_fps = list(input_dir.rglob("*/*.*"))
    all_fps += list(input_dir.rglob("*.*"))

    dfs_to_load = {}
    seen_pfxs = set()
    icd_pfxs = {pfx for pfx, _ in ICD_DFS_TO_FIX}

    for in_fp in all_fps:
        pfx = get_shard_prefix(input_dir, in_fp)

        if pfx in seen_pfxs:
            continue

        try:
            fps = resolve_source_files(input_dir, pfx)
        except (FileNotFoundError, ValueError) as e:
            logger.info(f"Skipping {pfx} @ {in_fp.resolve()!s}: {e}")
            continue

        seen_pfxs.add(pfx)

        if pfx in icd_pfxs:
            continue  # Processed in the dedicated ICD normalization loop below.

        if pfx in FUNCTIONS:
            out_fp = output_dir / f"{pfx}.parquet"
            if out_fp.is_file():
                print(f"Done with {pfx}. Continuing")
                continue

            fn, need_df = FUNCTIONS[pfx]
            if not need_df:
                st = datetime.now()
                logger.info(f"Processing {pfx}...")
                df = scan_source(fps, infer_schema_length=CSV_INFER_SCHEMA_LENGTH)
                processed_df = fn(df)
                out_fp.parent.mkdir(parents=True, exist_ok=True)
                write_df(processed_df, out_fp)
                logger.info(f"  Processed and wrote to {out_fp.resolve()!s} in {datetime.now() - st}")
            else:
                needed_pfx, needed_cols = need_df
                if needed_pfx not in dfs_to_load:
                    dfs_to_load[needed_pfx] = {"pfxs": set(), "cols": set()}

                dfs_to_load[needed_pfx]["pfxs"].add(pfx)
                dfs_to_load[needed_pfx]["cols"].update(needed_cols)
            continue

        # Passthrough: symlink or copy each source file unchanged.
        for fp in fps:
            out_fp = output_dir / fp.relative_to(input_dir)

            if out_fp.is_file():
                print(f"Done with {pfx}. Continuing")
                continue

            out_fp.parent.mkdir(parents=True, exist_ok=True)

            if do_copy:
                logger.info(f"No function needed for {pfx}: Copying {fp.resolve()!s} to {out_fp.resolve()!s}")
                shutil.copy(fp, out_fp)
            else:
                logger.info(
                    f"No function needed for {pfx}: Symlinking {fp.resolve()!s} to {out_fp.resolve()!s}"
                )
                out_fp.symlink_to(fp.resolve())

    for df_to_load_pfx, deps in dfs_to_load.items():
        load_fps = resolve_source_files(input_dir, df_to_load_pfx)

        st = datetime.now()
        logger.info(f"Loading {df_to_load_pfx} for manipulating other dataframes...")
        cols = sorted(deps["cols"])
        df = scan_source(load_fps, infer_schema_length=CSV_INFER_SCHEMA_LENGTH).select(cols)
        logger.info(f"  Loaded in {datetime.now() - st}")

        for pfx in deps["pfxs"]:
            out_fp = output_dir / f"{pfx}.parquet"

            logger.info(f"  Processing dependent df @ {pfx}...")
            fn, _ = FUNCTIONS[pfx]

            fp_st = datetime.now()
            fps = resolve_source_files(input_dir, pfx)
            fp_df = scan_source(fps, infer_schema_length=CSV_INFER_SCHEMA_LENGTH)
            processed_df = fn(fp_df, df)
            out_fp.parent.mkdir(parents=True, exist_ok=True)
            write_df(processed_df, out_fp)
            logger.info(f"    Processed and wrote to {out_fp.resolve()!s} in {datetime.now() - fp_st}")

    for pfx, fn in ICD_DFS_TO_FIX:
        fps = resolve_source_files(input_dir, pfx)

        out_fp = output_dir / f"{pfx}.parquet"

        if out_fp.is_file():
            print(f"Done with {pfx}. Continuing")
            continue

        # ICD codes must never be schema-inferred (e.g., "0389" would parse as an int and
        # lose its leading zero), so csv-family inputs are read with inference disabled.
        # ``infer_schema`` is a csv-only kwarg, hence the conditional.
        is_csv = fps[0].name.endswith((".csv", ".csv.gz"))
        scan_kwargs = {"infer_schema": False} if is_csv else {}

        st = datetime.now()
        logger.info(f"Processing {pfx}...")
        processed_df = (
            scan_source(fps, **scan_kwargs)
            .collect()
            .with_columns(
                fn(
                    pl.col("icd_version").cast(pl.String),
                    pl.col("icd_code").cast(pl.String),
                ).alias("norm_icd_code")
            )
        )
        out_fp.parent.mkdir(parents=True, exist_ok=True)
        processed_df.write_parquet(out_fp, use_pyarrow=True)
        logger.info(f"  Processed and wrote to {out_fp.resolve()!s} in {datetime.now() - st}")

    logger.info(f"Done! All dataframes processed and written to {output_dir.resolve()!s}")
    done_fp.write_text(f"Finished at {datetime.now()}")
