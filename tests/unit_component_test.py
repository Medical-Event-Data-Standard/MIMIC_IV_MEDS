"""A lab, chart or output code's unit component is `valueuom` stripped, and `UNK` when missing or blank.

MEDS-Extract compares units stripped and drops blank ones when it writes `codes.parquet`, so a
code that kept the whitespace would split one unit over several codes (` mg`, `mg`, `mg `), and a
blank one would give a code ending in whitespace (or in `//`) with no unit. The expressions are
evaluated straight from the shipped config with dftly, as MEDS-Extract evaluates them.
"""

from importlib.resources import files

import polars as pl
import pytest
import yaml
from dftly import Parser

UNITS = ["mg", " mg", "mg ", "\tmg\n", "mL / h", "  ", "", None]
EXPECTED = ["mg", "mg", "mg", "mg", "mL / h", "UNK", "UNK", "UNK"]

#: (table, event, code prefix for itemid 1)
CASES = [
    ("hosp/labevents", "lab_result", "LAB//RESULT//1//"),
    ("icu/chartevents", "event", "ICU_CHART//1//"),
    ("icu/outputevents", "output", "SUBJECT_FLUID_OUTPUT//1//"),
]


@pytest.mark.parametrize("table,event,prefix", CASES)
def test_unit_component_is_stripped_and_blank_is_unk(table: str, event: str, prefix: str):
    block = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())[table]
    raw = pl.DataFrame(
        {"valueuom": UNITS, "itemid": [1] * len(UNITS)}, schema_overrides={"valueuom": pl.String}
    )
    helper = {"_value_unit": block["_table"]["cols"]["_value_unit"]}
    with_cols = raw.with_columns(**Parser.to_polars(helper))
    codes = with_cols.select(**Parser.to_polars({"code": block[event]["code"]}))["code"].to_list()
    assert codes == [prefix + unit for unit in EXPECTED]
    # `unit` stays the raw column; MEDS-Extract strips it when it writes `codes.parquet`.
    assert block[event]["_metadata"]["_self"]["unit"] == "$valueuom"
