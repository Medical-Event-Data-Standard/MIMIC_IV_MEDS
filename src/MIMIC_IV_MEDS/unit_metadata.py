"""The ``mimic_iv_unit_metadata`` post-stage: units stated in item labels, and unit siblings linked.

MEDS-Extract (0.9 and later) writes ``unit`` as one String per code: every code of this ETL has at
most one unit (where an item's unit varies by row it is the code's last component). This stage no
longer collapses anything. It gives an ICU item charted without a unit the unit its label states,
and links codes that differ only in their last unit component: they get the code without it as a
parent, with a row of its own, so they can be found together. The spec runs the stage after the
canonical pipeline and before the suMEDS enrichment (``etl.post_stages``); it is registered in the
``MEDS_transforms.stages`` entry-point group.
"""

import logging
import re
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from MEDS_transforms.stages import Stage

logger = logging.getLogger(__name__)

STAGE_NAME = "mimic_iv_unit_metadata"
UNIT = "unit"
PARENTS = "parent_codes"

#: A unit stated at the end of an item's label, in brackets after a space: `Height (cm)`. Only the
#: units below count; other bracketed endings (`(serum)`, `(CIWA)`, a bag size such as `(500ml)`, a
#: reference point such as `(cm at teeth)`) do not. Both were read off the public MIMIC-IV demo's
#: `icu/d_items` labels, which are the descriptions of the ICU item codes; no `hosp/d_labitems`
#: label states a unit.
DESCRIPTION_UNIT = re.compile(r"\s[(\[]([^()\[\]]+)[)\]]\s*$")
DESCRIPTION_UNITS = frozenset(
    {"%", "C", "CM", "cm", "Kg", "L/min", "lbs.", "min", "Minutes", "mL", "mL/hour", "mL/hr", "ml/min"}
    | {"mm", "RPM", "units/mL"}
)
#: The last component of a code whose unit was not recorded (the ETL's `?? 'UNK'`).
NO_UNIT_COMPONENT = "//UNK"


def complete_unit_metadata(codes_fp: Path, output_fp: Path | None = None) -> None:
    """Fill units stated in descriptions in a ``codes.parquet``, and link codes that differ only by unit.

    ``unit`` is the String column MEDS-Extract writes, one unit per code, and is kept as it is. A
    code whose unit component is missing (``…//UNK``) and that has no unit gets the unit its
    description states, if any (see :data:`DESCRIPTION_UNIT`): MIMIC-IV's own item label is the last
    source, after the data.

    A code is unit-suffixed when it ends with ``//`` and its unit; its stem is the code without that
    suffix. Where two or more unit-suffixed codes share a stem (the same item recorded in different
    units), each of them gets the stem appended to its ``parent_codes`` (created if missing; existing
    parents are kept and nothing is repeated), and the stem gets a row of its own unless it has one:
    the siblings' description if they all have the same one (or the same one but for the unit it
    states, which is then left out), the parents they all share (other than
    the stem), and null otherwise, its ``unit`` included. The rows are then sorted by code. A second
    run changes nothing. A file without ``unit`` is not changed; the existing rows' other columns are
    written back unchanged.

    Args:
        codes_fp: The ``codes.parquet`` to read.
        output_fp: Where to write the result; defaults to ``codes_fp`` (in place).

    Examples:
        >>> import tempfile
        >>> with tempfile.TemporaryDirectory() as d:
        ...     fp = Path(d) / "codes.parquet"
        ...     code = ["X//1//mcg", "X//1//mg", "X//2//mg", "Y"]
        ...     unit = ["mcg", "mg", "mg", None]
        ...     pl.DataFrame({"code": code, "unit": unit}).write_parquet(fp)
        ...     complete_unit_metadata(fp)
        ...     for row in pl.read_parquet(fp).rows():
        ...         print(row)
        ('X//1', None, None)
        ('X//1//mcg', 'mcg', ['X//1'])
        ('X//1//mg', 'mg', ['X//1'])
        ('X//2//mg', 'mg', None)
        ('Y', None, None)
    """
    output_fp = codes_fp if output_fp is None else output_fp
    table = pq.read_table(codes_fp)
    if UNIT not in table.column_names:
        if output_fp != codes_fp:
            pq.write_table(table, output_fp)
        return
    index = table.column_names.index(UNIT)
    table = table.set_column(index, pa.field(UNIT, pa.string()), _unit_from_description(table))
    pq.write_table(_link_unit_siblings(table), output_fp)


def description_unit(description: str | None) -> re.Match | None:
    """The match of a unit stated at the end of ``description``, if it states one of
    :data:`DESCRIPTION_UNITS`; its group 1 is the unit.

    Examples:
        >>> description_unit("Admission Weight (lbs.)").group(1)
        'lbs.'
        >>> description_unit("Glucose (serum)") is None, description_unit("O2 Delivery Device(s)") is None
        (True, True)
    """
    match = DESCRIPTION_UNIT.search(description or "")
    return match if match and match.group(1) in DESCRIPTION_UNITS else None


def _unit_from_description(table: pa.Table) -> pa.Array:
    """``unit``, filled from the description where the code's own unit component is missing.

    Only a code that ends in ``//UNK`` and has no unit gets one, so a unit from the data or from the
    code always wins. The code is not changed: the unit is per item, so the code still has one.
    """
    units = table[UNIT].to_pylist()
    if "description" not in table.column_names:
        return pa.array(units, pa.string())
    filled = 0
    for row, (code, description) in enumerate(
        zip(table["code"].to_pylist(), table["description"].to_pylist(), strict=True)
    ):
        if (
            units[row] is None
            and code.endswith(NO_UNIT_COMPONENT)
            and (match := description_unit(description))
        ):
            units[row] = match.group(1)
            filled += 1
    logger.info(f"{filled} codes without a recorded unit get the unit their description states")
    return pa.array(units, pa.string())


def _link_unit_siblings(table: pa.Table) -> pa.Table:
    """Give unit siblings their stem as a parent, and the stem a row (see :func:`complete_unit_metadata`)."""
    suffix = "//" + pl.col(UNIT)
    stems = (
        pl.from_arrow(table.select(["code", UNIT]))
        .select(
            stem=pl.when(pl.col("code").str.ends_with(suffix)).then(pl.col("code").str.strip_suffix(suffix))
        )
        .select(pl.when(pl.col("stem").count().over("stem") > 1).then(pl.col("stem")))["stem"]
        .to_list()
    )
    groups: dict[str, list[int]] = {}
    for row, stem in enumerate(stems):
        if stem is not None:
            groups.setdefault(stem, []).append(row)
    if not groups:
        logger.info("No unit siblings")
        return table

    columns = table.column_names
    if PARENTS in columns:
        field = table.schema.field(PARENTS)
        parents = table[PARENTS].to_pylist()
    else:
        field = pa.field(PARENTS, pa.list_(pa.string()))
        parents = [None] * table.num_rows
    descriptions = table["description"].to_pylist() if "description" in columns else None
    existing = set(table["code"].to_pylist())
    stem_rows = []
    for stem, rows in groups.items():
        if stem in existing:
            continue
        first, *others = (parents[row] or [] for row in rows)
        shared = [p for p in dict.fromkeys(first) if p != stem and all(p in other for other in others)]
        row = {"code": stem, PARENTS: shared or None}
        if descriptions is not None:
            described = {descriptions[r] for r in rows}
            if len(described) > 1:
                # Descriptions that differ only by the unit they state share the rest.
                bare = {d[: m.start()].strip() if (m := description_unit(d)) else d for d in described}
                described = bare if len(bare) == 1 else described
            row["description"] = described.pop() if len(described) == 1 else None
        stem_rows.append(row)

    merged = [
        old if stem is None else list(dict.fromkeys([*(old or []), stem]))
        for old, stem in zip(parents, stems, strict=True)
    ]
    column = pa.array(merged, type=field.type)
    if PARENTS in columns:
        table = table.set_column(columns.index(PARENTS), field, column)
    else:
        table = table.append_column(field, column)
    logger.info(
        f"{len(groups)} groups of unit siblings: {sum(map(len, groups.values()))} codes get their stem as "
        f"a parent, and {len(stem_rows)} stems get a row"
    )
    if not stem_rows:
        return table
    return pa.concat_tables([table, pa.Table.from_pylist(stem_rows, schema=table.schema)]).sort_by("code")


def _main(cfg: Any) -> None:
    """Finish ``unit`` in the finished cohort's ``codes.parquet`` (see the module)."""
    if cfg.worker != 0:
        return
    name = "codes.parquet"
    complete_unit_metadata(
        Path(cfg.stage_cfg.metadata_input_dir) / name, Path(cfg.stage_cfg.reducer_output_dir) / name
    )


main = Stage.register(main_fn=_main, stage_name=STAGE_NAME, is_metadata=True, default_config={})
