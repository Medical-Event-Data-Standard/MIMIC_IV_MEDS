"""The vocabulary parents from the mimic-code concept maps are well-formed codes of their vocabulary.

Up to mimic-code v3.0.1, a spreadsheet had turned three `omop_concept_code` cells into numbers (#117,
MIT-LCP/mimic-code#2181): `2.26471E+14` for SNOMED 226471000000101, `968472` for LOINC 4551-8 and `-56064`
for LOINC 1746-7. The spec pins the maps to the mimic-code commit that fixes them (MIT-LCP/mimic-code#2182).
These tests check the maps the demo build staged (the same files as for the full release, pinned by
checksum), so a newly mangled code fails here, and the parents the demo build wrote.
"""

import re
from pathlib import Path

import polars as pl

#: The maps whose `omop_vocabulary_id//omop_concept_code` the spec writes as `parent_codes`.
PARENT_MAPS = (
    "d_labitems_to_loinc.csv",
    "inputevents_to_rxnorm.csv",
    "meas_chartevents_main.csv",
    "outputevents_to_loinc.csv",
    "proc_datetimeevents.csv",
    "proc_itemid.csv",
)

#: The shape of a code in each vocabulary the maps use: LOINC `<number>-<check digit>` (with an `LA`
#: prefix for an answer, such as `LA15173-0`), SNOMED CT and RxNorm a plain integer identifier.
CODE_PATTERNS = {
    "LOINC": re.compile(r"(?:LA)?[0-9]{1,7}-[0-9]"),
    "SNOMED": re.compile(r"[1-9][0-9]{5,17}"),
    "RxNorm": re.compile(r"[1-9][0-9]*"),
}

#: The three rows mimic-code#2182 fixed: (map, itemid) -> the parent the spec writes for them.
FIXED = {
    ("proc_itemid.csv", "225792"): "SNOMED//226471000000101",
    ("d_labitems_to_loinc.csv", "51643"): "LOINC//4551-8",
    ("d_labitems_to_loinc.csv", "51777"): "LOINC//1746-7",
}


def _map_codes(raw: Path) -> pl.DataFrame:
    frames = [
        pl.read_csv(raw / name, infer_schema=False)
        .select(
            pl.col("itemid (omop_source_code)").alias("itemid"),
            "omop_vocabulary_id",
            "omop_concept_code",
        )
        .with_columns(map=pl.lit(name))
        for name in PARENT_MAPS
    ]
    # A row without a code (a few lab rows) gives no parent: the f-string over a null is null.
    return (
        pl.concat(frames)
        .filter(pl.col("omop_vocabulary_id").is_not_null() & pl.col("omop_concept_code").is_not_null())
        .unique()
    )


def test_map_codes_are_well_formed(demo_raw_dir: Path):
    rows = _map_codes(demo_raw_dir).rows(named=True)
    vocabularies = {row["omop_vocabulary_id"] for row in rows}
    assert vocabularies <= CODE_PATTERNS.keys(), f"vocabularies without a pattern: {vocabularies}"

    malformed = {
        (row["map"], row["itemid"]): row["omop_concept_code"]
        for row in rows
        if not CODE_PATTERNS[row["omop_vocabulary_id"]].fullmatch(row["omop_concept_code"])
    }
    assert not malformed, f"malformed codes in the mimic-code maps: {malformed}"

    parents = {
        (row["map"], row["itemid"]): f"{row['omop_vocabulary_id']}//{row['omop_concept_code']}"
        for row in rows
    }
    assert {key: parents.get(key) for key in FIXED} == FIXED


def test_demo_parents_are_well_formed(demo_cohort: Path):
    codes = pl.read_parquet(demo_cohort / "metadata" / "codes.parquet", columns=["code", "parent_codes"])
    parents = codes.explode("parent_codes").drop_nulls("parent_codes")
    vocabulary_parents = parents.with_columns(
        vocabulary=pl.col("parent_codes").str.extract(r"^([^/]+)//"),
        concept_code=pl.col("parent_codes").str.extract(r"^[^/]+//(.*)$"),
    ).filter(pl.col("vocabulary").is_in(list(CODE_PATTERNS)))
    assert vocabulary_parents.height, "no LOINC, SNOMED or RxNorm parents in the demo"
    malformed = sorted(
        {
            row["parent_codes"]
            for row in vocabulary_parents.iter_rows(named=True)
            if not CODE_PATTERNS[row["vocabulary"]].fullmatch(row["concept_code"])
        }
    )
    assert not malformed, f"malformed vocabulary parents: {malformed[:10]}"

    # Invasive ventilation (225792) occurs in the demo (51643 and 51777 do not); its fixed SNOMED parent
    # is written.
    by_code = dict(zip(codes["code"].to_list(), codes["parent_codes"].to_list(), strict=True))
    for code in ("PROCEDURE//START//225792", "PROCEDURE//END//225792"):
        assert "SNOMED//226471000000101" in (by_code.get(code) or []), f"{code}: {by_code.get(code)}"
