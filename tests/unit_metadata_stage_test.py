"""The `mimic_iv_unit_metadata` post-stage on small synthetic `codes.parquet` files: the unit an item's label
states, and the code without its unit as a parent of codes that differ only by unit."""

from pathlib import Path

import polars as pl

from MIMIC_IV_MEDS.unit_metadata import complete_unit_metadata


def _run(tmp_path: Path, codes: pl.DataFrame) -> pl.DataFrame:
    fp = tmp_path / "codes.parquet"
    codes.write_parquet(fp)
    complete_unit_metadata(fp)
    return pl.read_parquet(fp)


def test_string_unit_passes_through(tmp_path: Path):
    codes = pl.DataFrame({"code": ["A", "B"], "unit": ["mg", None]})
    assert _run(tmp_path, codes).equals(codes)


def test_no_unit_column_is_unchanged(tmp_path: Path):
    codes = pl.DataFrame({"code": ["A"], "description": ["a"]})
    assert _run(tmp_path, codes).equals(codes)


def _by_code(codes: pl.DataFrame) -> dict[str, dict]:
    return {row["code"]: row for row in codes.iter_rows(named=True)}


def test_unit_siblings_get_their_stem_as_parent(tmp_path: Path):
    codes = pl.DataFrame(
        {
            "code": [
                "LAB//1//mg/dL",  # siblings, with a vocabulary parent already
                "LAB//1//mmol/L",
                "LAB//1//UNK",  # no unit: not a sibling
                "LAB//2//mg",  # a unit-suffixed code without a sibling
                "LAB//3//g",  # siblings that already list their stem
                "LAB//3//mg",
                "Weight (Lbs)",  # the unit is not the last component
            ],
            "parent_codes": [["LOINC/1"], None, None, ["LOINC/2"], [], ["LAB//3"], None],
            "unit": ["mg/dL", "mmol/L", None, "mg", "g", "mg", "Lbs"],
        }
    )
    out = _run(tmp_path, codes)
    assert out.schema["parent_codes"] == pl.List(pl.String)
    parents = {code: row["parent_codes"] for code, row in _by_code(out).items()}
    assert parents == {
        "LAB//1//mg/dL": ["LOINC/1", "LAB//1"],
        "LAB//1//mmol/L": ["LAB//1"],
        "LAB//1//UNK": None,
        "LAB//2//mg": ["LOINC/2"],
        "LAB//3//g": ["LAB//3"],
        "LAB//3//mg": ["LAB//3"],
        "Weight (Lbs)": None,
        # The stems' own rows: no parent all siblings share (never the stem itself).
        "LAB//1": None,
        "LAB//3": None,
    }
    # A code whose unit-free stem has no sibling gets no row for it.
    assert "LAB//2" not in parents
    assert out["code"].is_sorted()


def test_stem_row_shares_what_all_siblings_share(tmp_path: Path):
    codes = pl.DataFrame(
        {
            "code": ["A//1//g", "A//1//mg", "A//2//g", "A//2//mg"],
            "description": ["Sodium", "Sodium", "Potassium", "Potassium, serum"],
            "parent_codes": [["LOINC/1", "SNOMED/9"], ["LOINC/1"], ["LOINC/2"], ["LOINC/3"]],
            "code_template": ["t", "t", "t", "t"],
            "unit": ["g", "mg", "g", "mg"],
        }
    )
    out = _by_code(_run(tmp_path, codes))
    assert out["A//1"] == {
        "code": "A//1",
        "description": "Sodium",
        "parent_codes": ["LOINC/1"],
        "code_template": None,
        "unit": None,
    }
    # Different descriptions and no shared parent give none.
    assert out["A//2"]["description"] is None
    assert out["A//2"]["parent_codes"] is None


def test_existing_stem_row_is_untouched_and_a_second_run_adds_nothing(tmp_path: Path):
    codes = pl.DataFrame(
        {
            "code": ["B//1", "B//1//g", "B//1//mg", "C//1//g", "C//1//mg"],
            "description": ["kept", "x", "x", "y", "y"],
            "unit": ["kg", "g", "mg", "g", "mg"],
        }
    )
    once = _run(tmp_path, codes)
    assert _by_code(once)["B//1"] == {
        "code": "B//1",
        "description": "kept",
        "unit": "kg",
        "parent_codes": None,
    }
    assert once.height == codes.height + 1  # only C//1 is new
    assert _run(tmp_path, once).equals(once)


def test_parent_codes_column_is_created_for_siblings(tmp_path: Path):
    codes = pl.DataFrame({"code": ["X//a//g", "X//a//mg", "Y//mg"], "unit": ["g", "mg", "mg"]})
    out = _by_code(_run(tmp_path, codes))
    assert {code: row["parent_codes"] for code, row in out.items()} == {
        "X//a": None,
        "X//a//g": ["X//a"],
        "X//a//mg": ["X//a"],
        "Y//mg": None,
    }
    # Without siblings, the file gets no parent_codes column and no new row.
    assert _run(tmp_path, codes.tail(1)).columns == ["code", "unit"]


def test_unit_stated_in_the_description_fills_a_missing_unit(tmp_path: Path):
    codes = pl.DataFrame(
        {
            "code": ["LAB//1//UNK", "LAB//2//kg", "LAB//3//UNK", "LAB//4//UNK", "OTHER"],
            "description": [
                "Admission Weight (lbs.)",
                "Admission Weight (Kg)",  # the code's own unit wins
                "Glucose (serum)",  # not a unit
                "ADA-A (500ml)",  # a bag size, not a unit
                "Volume (mL)",  # has its unit component: not filled
            ],
            "unit": [None, "kg", None, None, None],
        }
    )
    out = _run(tmp_path, codes)
    assert out["unit"].to_list() == ["lbs.", "kg", None, None, None]
    assert out["code"].to_list() == codes["code"].to_list(), "codes are not changed"


def test_stem_description_leaves_out_the_unit_its_siblings_differ_by(tmp_path: Path):
    codes = pl.DataFrame(
        {
            "code": ["A//1//mL/hour", "A//1//mL/hr", "A//2//mL", "A//2//mm"],
            "description": ["Rate (mL/hour)", "Rate (mL/hr)", "Volume (mL)", "Size (mm)"],
            "unit": ["mL/hour", "mL/hr", "mL", "mm"],
        }
    )
    out = _by_code(_run(tmp_path, codes))
    assert out["A//1"]["description"] == "Rate"
    assert out["A//2"]["description"] is None
