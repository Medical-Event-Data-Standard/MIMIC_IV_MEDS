"""Convert the MIMIC-CXR-DB tables the `MIMIC-IV-CXRDB` spec reads into parquet MEDS-Extract can read.

MIMIC-CXR-DB v1.0 ships semicolon-delimited `.csv` files, and their fields contain commas (concept
labels, DICOM procedure names). MEDS-Extract 0.8.1 reads every `.csv` with polars' default comma
separator and has no option to change it, so the release cannot be read as downloaded. This is the
one step that runs between `meds-extract-download` and `meds-extract-run` for that spec:

    meds-extract-download spec=MIMIC-IV-CXRDB output_dir=$RAW
    python -m MIMIC_IV_MEDS.prepare_cxr_db $RAW
    meds-extract-run spec=MIMIC-IV-CXRDB do_download=false input_dir=$RAW output_dir=$MEDS_OUTPUT_DIR

For each of `radiographs`, `radiology_jev` and `discharge_jev` it reads `$RAW/<table>.csv` and writes
`$RAW/cxr_db/<table>.parquet`, with the column types polars infers over the whole file, which are the
types MEDS-Extract's own CSV conversion would give a comma-separated copy. The parquet files go to a
subdirectory because MEDS-Extract refuses a table that resolves to both a `.csv` and a `.parquet`,
and the downloaded `.csv` files stay in place so a re-run of the download still verifies them.

It adds exactly one column: `radiographs.study_end_datetime`, the latest `study_datetime` among the
images of the row's study. The study-level events `radiographs` emits (the study itself and its
CheXpert labels) are timed there. MEDS-Extract 0.8.1 cannot reduce a table over itself: its
aggregated joins can only target another table (a self-join is rejected at parse time, since every
joined column would collide with the left side and joined columns cannot be renamed), and dftly
has no window aggregation. `radiology_jev` reduces the image times with an ordinary aggregated join
and does not need this column. The 14 CheXpert label columns are read as Float64 rather than
inferred. Nothing else is changed: no row is dropped and no value is recoded.

A CSV separator option for MEDS-Extract (`etl.separator`, alongside `etl.encoding`) would make the
format conversion unnecessary; until it ships, this module does it. Even then, the per-study
`study_end_datetime` needs either this step or a MEDS-Extract way to reduce a table over itself.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

#: The release tables the spec reads (the Jev-screened edition of the two triple tables).
TABLES = ("radiographs", "radiology_jev", "discharge_jev")

#: Where the converted tables go, relative to the raw input directory.
OUT_SUBDIR = "cxr_db"

#: The 14 CheXpert label columns of `radiographs`.
CHEXPERT_LABELS = (
    "Atelectasis",
    "Cardiomegaly",
    "Consolidation",
    "Edema",
    "Enlarged_Cardiomediastinum",
    "Fracture",
    "Lung_Lesion",
    "Lung_Opacity",
    "No_Finding",
    "Pleural_Effusion",
    "Pleural_Other",
    "Pneumonia",
    "Pneumothorax",
    "Support_Devices",
)


def convert_table(src: Path, dest: Path) -> pl.DataFrame:
    """Read one semicolon-delimited release table and write it as parquet; return what was written.

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     src = Path(d) / "discharge_jev.csv"
        ...     _ = src.write_text("hadm_id;relation_type;label\\n1;Chief Complaint;cough, dry\\n2;x;\\n")
        ...     convert_table(src, Path(d) / "out" / "discharge_jev.parquet")
        shape: (2, 3)
        ┌─────────┬─────────────────┬────────────┐
        │ hadm_id ┆ relation_type   ┆ label      │
        │ ---     ┆ ---             ┆ ---        │
        │ i64     ┆ str             ┆ str        │
        ╞═════════╪═════════════════╪════════════╡
        │ 1       ┆ Chief Complaint ┆ cough, dry │
        │ 2       ┆ x               ┆ null       │
        └─────────┴─────────────────┴────────────┘

        `radiographs` gains `study_end_datetime`, the latest image time of each study:

        >>> with tempfile.TemporaryDirectory() as d:
        ...     src = Path(d) / "radiographs.csv"
        ...     _ = src.write_text(
        ...         "study_id;dicom_id;study_datetime\\n"
        ...         "7;a;2150-01-01 10:00:00.000\\n7;b;2150-01-01 10:05:00.000\\n"
        ...         "8;c;2150-02-01 09:00:00.000\\n"
        ...     )
        ...     convert_table(src, Path(d) / "radiographs.parquet").select("dicom_id", "study_end_datetime")
        shape: (3, 2)
        ┌──────────┬─────────────────────────┐
        │ dicom_id ┆ study_end_datetime      │
        │ ---      ┆ ---                     │
        │ str      ┆ str                     │
        ╞══════════╪═════════════════════════╡
        │ a        ┆ 2150-01-01 10:05:00.000 │
        │ b        ┆ 2150-01-01 10:05:00.000 │
        │ c        ┆ 2150-02-01 09:00:00.000 │
        └──────────┴─────────────────────────┘
    """
    header = pl.read_csv(src, separator=";", n_rows=0).columns
    # The CheXpert labels are 1.0 / 0.0 / -1.0 / blank. Pinned so that a column that is entirely
    # blank (possible in any subset of the release) is still numeric rather than inferred String.
    overrides = {c: pl.Float64 for c in CHEXPERT_LABELS if c in header}
    df = pl.read_csv(src, separator=";", infer_schema_length=None, schema_overrides=overrides)
    if src.stem == "radiographs":
        # The timestamps are fixed-width "%Y-%m-%d %H:%M:%S.%f" strings, so the string max is the
        # latest time; keeping it a string keeps it in the same format as `study_datetime`.
        df = df.with_columns(pl.col("study_datetime").max().over("study_id").alias("study_end_datetime"))
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(dest)
    return df


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "raw_dir", type=Path, help="the raw input directory the CXR-DB files were downloaded to"
    )
    args = parser.parse_args(argv)

    missing = [t for t in TABLES if not (args.raw_dir / f"{t}.csv").is_file()]
    if missing:
        print(f"missing in {args.raw_dir}: {', '.join(f'{t}.csv' for t in missing)}", file=sys.stderr)
        return 1
    for table in TABLES:
        dest = args.raw_dir / OUT_SUBDIR / f"{table}.parquet"
        df = convert_table(args.raw_dir / f"{table}.csv", dest)
        print(f"wrote {dest} ({df.height} rows, {df.width} columns)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
