"""End-to-end run of the shipped ETL over the public MIMIC-IV demo release.

This distribution is `event_configs.yaml` plus the entry-point registration that names it
`MIMIC-IV` (and one small post-stage, tested in `collapse_unit_metadata_test.py`). So the main
thing worth testing is that
`meds-extract-run spec=MIMIC-IV` resolves that registration, downloads the demo bucket from
PhysioNet (no credentials needed), runs the pipeline, and leaves a well-formed MEDS cohort
behind. That build is the `demo_cohort` fixture of `conftest.py`, shared with
`run_with_enrichment_test.py` so the demo is downloaded once.

Whether the *contents* of that cohort have changed is a separate question, answered by the
summary-stats regression check. That check runs against the cohort *this* test builds
rather than building its own: the extraction downloads from PhysioNet and takes minutes, so
CI does it exactly once, on one Python version, and both checks read the result.
"""

import json
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from conftest import SPEC
from meds import CodeMetadataSchema

from MIMIC_IV_MEDS.unit_metadata import description_unit

#: The demo release the spec pins, as it appears in `dataset_version`'s `<release>:<pkg>` stamp.
DEMO_RELEASE = "2.2"


def test_e2e_demo(demo_cohort: Path):
    out = demo_cohort
    data_files = list((out / "data").glob("**/*.parquet"))
    assert data_files, f"No data shards written; {out / 'data'} holds {list((out / 'data').rglob('*'))}"

    metadata = out / "metadata"
    for fname in ("dataset.json", "codes.parquet", "subject_splits.parquet"):
        assert (metadata / fname).exists(), f"{fname} missing from {metadata}"

    # The identity the runner derives from the registration and the `sources:` block.
    dataset_json = json.loads((metadata / "dataset.json").read_text())
    assert dataset_json["dataset_name"] == SPEC
    assert dataset_json["dataset_version"].startswith(f"{DEMO_RELEASE}:")
    # Every code has one unit, so no column modifies a code: code metadata is keyed on the code alone,
    # and the events' `unit` is an ordinary extension column.
    assert not dataset_json.get("code_modifier_columns")
    assert "unit" in dataset_json["other_extension_columns"]

    # Code metadata, as read from disk, is valid MEDS code metadata, and every code is
    # described except lab items absent from mimic-code's lab map, which have no other
    # source (see "Code metadata" in the README). No description may hold a newline:
    # that is MEDS-Extract joining the output of two `_metadata` blocks for one code.
    codes = pq.read_table(metadata / "codes.parquet")
    CodeMetadataSchema.validate(codes)
    described = dict(zip(codes["code"].to_pylist(), codes["description"].to_pylist(), strict=True))
    undescribed = sorted(c for c, d in described.items() if d is None and not c.startswith("LAB//"))
    assert not undescribed, f"codes without a description: {undescribed}"
    multi = sorted(c for c, d in described.items() if d is not None and "\n" in d)
    assert not multi, f"codes with several joined descriptions: {multi}"

    # Codes whose last component is the unit carry exactly that unit, as one String (see "Units"
    # in the README); `UNK` there stands for a null unit, which gives none, as does a blank one.
    assert codes.schema.field("unit").type == "string"
    units = dict(zip(codes["code"].to_pylist(), codes["unit"].to_pylist(), strict=True))
    data_codes = set(pl.scan_parquet(data_files).select("code").unique().collect()["code"])
    infusions = ("INFUSION_START//", "INFUSION_END//")
    unit_coded = [c for c in units if c in data_codes and c.startswith(("LAB//", "SUBJECT_FLUID_OUTPUT//"))]
    unit_coded = [c for c in unit_coded if not c.startswith("LAB//SPECIMEN_COLLECTED//")]
    assert unit_coded, "no unit-coded codes in the demo"
    # Infusion codes end in their unit only where the itemid mixes units (`…//<itemid>//<unit>`).
    infusion_codes = [c for c in units if c in data_codes and c.startswith(infusions)]
    split = [c for c in infusion_codes if c.count("//") == 2]
    assert split, "no infusion itemid recorded in several units in the demo"
    assert len(split) < len(infusion_codes), "every infusion code has its unit in the code"
    unit_coded += split
    # Split from the left: a unit may itself contain a slash (`/hpf`).
    unit_part = {c: c.split("//", 3 if c.startswith("LAB//RESULT//") else 2)[-1] for c in unit_coded}
    expected = {c: None if u == "UNK" or not u.strip() else u for c, u in unit_part.items()}
    # Except where the item's label states the unit, which then fills it.
    from_label = {
        c: m.group(1) for c in unit_coded if unit_part[c] == "UNK" and (m := description_unit(described[c]))
    }
    assert from_label, "no unit from an item label in the demo"
    expected |= from_label
    wrong = sorted(c for c in unit_coded if units[c] != expected[c])
    assert not wrong, f"codes whose unit metadata is not their unit component: {wrong[:10]}"

    # Codes that differ only in their unit component have the code without it as a parent, which
    # has a row of its own without a unit.
    stems: dict[str, list[str]] = {}
    for c in unit_coded:
        if expected[c] is not None and c not in from_label:
            stems.setdefault(c.removesuffix(f"//{expected[c]}"), []).append(c)
    siblings = {stem: group for stem, group in stems.items() if len(group) > 1}
    assert siblings, "no unit siblings in the demo"
    parents = dict(zip(codes["code"].to_pylist(), codes["parent_codes"].to_pylist(), strict=True))
    unlinked = sorted(c for stem, group in siblings.items() for c in group if stem not in (parents[c] or []))
    assert not unlinked, f"unit siblings without their stem as a parent: {unlinked[:10]}"
    missing = sorted(stem for stem in siblings if stem not in units or units[stem] is not None)
    assert not missing, f"unit-sibling stems without a row of their own (and no unit): {missing[:10]}"

    # Every code's events, where they record a (non-blank) unit, have one unit, the code's `unit` in
    # `codes.parquet`: no code mixes units, whether or not the unit is in it.
    events = pl.scan_parquet(data_files).with_columns(pl.col("unit").str.strip_chars())
    events = events.filter(pl.col("unit").is_not_null() & (pl.col("unit") != ""))
    data_units = events.group_by("code").agg(pl.col("unit").unique()).collect()
    disagree = sorted(c for c, u in data_units.iter_rows() if u != [units.get(c)])
    assert not disagree, f"codes whose events' unit is not their unit metadata: {disagree[:10]}"
