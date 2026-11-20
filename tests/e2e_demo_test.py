"""End-to-end run of the shipped ETL over the public MIMIC-IV demo release.

This distribution is `event_configs.yaml` plus the entry-point registration that names it
`MIMIC-IV` (and one small pre-stage, tested in `unit_keys_test.py`). So the main thing worth
testing is that
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
import re
from importlib.resources import files
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import yaml
from conftest import SPEC
from meds import CodeMetadataSchema
from sumeds.units import to_ucum

#: The demo release the spec pins, as it appears in `dataset_version`'s `<release>:<pkg>` stamp.
DEMO_RELEASE = "2.2"


def _unit_sibling_options() -> dict:
    """The options the spec gives suMEDS's `sumeds_link_unit_siblings` post-stage."""
    spec = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())
    for stage in spec["etl"]["post_stages"]:
        if isinstance(stage, dict) and "sumeds_link_unit_siblings" in stage:
            return stage["sumeds_link_unit_siblings"]
    raise AssertionError("the spec does not run sumeds_link_unit_siblings")


def description_unit(description: str | None) -> re.Match | None:
    """The unit an item label states at its end, if it is one the spec accepts (group 1)."""
    options = _unit_sibling_options()
    match = re.search(options["description_unit_pattern"], description or "")
    return match if match and match.group(1) in options["description_units"] else None


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
    # Every code has one unit, so no column modifies a code: code metadata is keyed on the code alone.
    # That unit lives in `codes.parquet` only: the events have no `unit` column (join them to
    # `codes.parquet` on `code` for an event's unit).
    assert not dataset_json.get("code_modifier_columns")
    assert "unit" not in dataset_json.get("other_extension_columns", [])
    assert all("unit" not in pl.read_parquet_schema(f) for f in data_files)

    # Code metadata, as read from disk, is valid MEDS code metadata, and every code is
    # described except `hosp/labevents` items absent from mimic-code's lab map (the 53xxx
    # itemids), which have no other source (see "Code metadata" in the README), and the
    # unit-less stem suMEDS links their unit variants to (`LAB//RESULT//53153`). ICU chart
    # codes (`ICU_CHART//…`) are described from `icu/d_items` and get no exemption. No
    # description may hold a newline: that is MEDS-Extract joining the output of two
    # `_metadata` blocks for one code.
    codes = pq.read_table(metadata / "codes.parquet")
    CodeMetadataSchema.validate(codes)
    described = dict(zip(codes["code"].to_pylist(), codes["description"].to_pylist(), strict=True))
    unmapped_lab = re.compile(r"^LAB//(RESULT|SPECIMEN_COLLECTED)//53\d{3}(//|$)")
    undescribed = sorted(c for c, d in described.items() if d is None and not unmapped_lab.match(c))
    assert not undescribed, f"codes without a description: {undescribed}"
    multi = sorted(c for c, d in described.items() if d is not None and "\n" in d)
    assert not multi, f"codes with several joined descriptions: {multi}"

    # One row, so one unit, per code.
    assert len(set(codes["code"].to_pylist())) == codes.num_rows, "codes.parquet repeats a code"
    # `unit` is the UCUM code of the unit as recorded, which suMEDS's `sumeds_normalize_units` keeps in
    # `unit_source_value`; a spelling suMEDS does not know is kept as it is.
    # One String per code (`large_string` as MEDS-Extract writes it; suMEDS keeps the Arrow type).
    assert codes.schema.field("unit").type in ("string", "large_string")
    units = dict(zip(codes["code"].to_pylist(), codes["unit_source_value"].to_pylist(), strict=True))
    ucum = dict(zip(codes["code"].to_pylist(), codes["unit"].to_pylist(), strict=True))
    not_ucum = sorted(c for c, u in units.items() if ucum[c] != (u if u is None else to_ucum(u) or u))
    assert not not_ucum, f"codes whose unit is not the UCUM code of their unit_source_value: {not_ucum[:10]}"
    assert any(ucum[c] != u for c, u in units.items()), "no unit rewritten as UCUM on the demo"

    # Codes whose last component is the unit carry exactly that unit as recorded, as one String (see
    # "Units" in the README); `UNK` there stands for a null unit, which gives none, as does a blank one.
    data_codes = set(pl.scan_parquet(data_files).select("code").unique().collect()["code"])
    infusions = ("INFUSION_START//", "INFUSION_END//")
    unit_coded = [
        c
        for c in units
        if c in data_codes and c.startswith(("LAB//", "ICU_CHART//", "SUBJECT_FLUID_OUTPUT//"))
    ]
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
    # has a row of its own without a unit. A code whose unit was not recorded (`…//UNK`) joins its
    # item's siblings, if one of them has a unit in the code.
    stems: dict[str, list[str]] = {}
    real: set[str] = set()
    for c in unit_coded:
        if unit_part[c] == "UNK":
            stems.setdefault(c.removesuffix("//UNK"), []).append(c)
        elif expected[c] is not None:
            stems.setdefault(c.removesuffix(f"//{expected[c]}"), []).append(c)
            real.add(c)
    siblings = {stem: group for stem, group in stems.items() if len(group) > 1 and real & set(group)}
    assert any(c.endswith("//UNK") for group in siblings.values() for c in group), "no `…//UNK` sibling"
    assert siblings, "no unit siblings in the demo"
    parents = dict(zip(codes["code"].to_pylist(), codes["parent_codes"].to_pylist(), strict=True))
    unlinked = sorted(c for stem, group in siblings.items() for c in group if stem not in (parents[c] or []))
    assert not unlinked, f"unit siblings without their stem as a parent: {unlinked[:10]}"
    missing = sorted(stem for stem in siblings if stem not in units or units[stem] is not None)
    assert not missing, f"unit-sibling stems without a row of their own (and no unit): {missing[:10]}"

    # Every infusion end has a unit, its rows' `amountuom` (never null): MEDS-Extract (0.9) accepts a
    # code's unit metadata only if the code has one, so no infusion code mixes units, and joining the
    # events to `codes.parquet` on `code` gives each infusion end its unit.
    unitless = sorted(c for c in infusion_codes if c.startswith("INFUSION_END//") and units[c] is None)
    assert not unitless, f"infusion ends without a unit: {unitless[:10]}"
