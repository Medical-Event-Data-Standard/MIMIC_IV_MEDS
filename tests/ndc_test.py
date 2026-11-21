"""Every NDC in a code or parent is the 11-digit NDC, leading zeros included, however `ndc` was read.

MEDS-Extract types each CSV column from the whole file. Where every `hosp/prescriptions.ndc` is digits,
as in the public demo, the column is read as Int64 and `00904627761` arrives as `904627761`; the full
release has a few non-digit values, so it stays String and keeps the zeros. The spec pads the NDC back
to 11 digits in `_table.cols.ndc_clean`. The ETL runs the shipped medication tables over a few made-up
rows whose `ndc` column is all digits (no download), and the expressions are also evaluated with dftly
on both dtypes.
"""

from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest
import yaml
from conftest import run_script
from dftly import Parser

#: The medication tables, which take `ndc` from `hosp/prescriptions` by a `_table.joins`.
TABLES = ["hosp/emar", "hosp/pharmacy"]

RAW = {
    "hosp/prescriptions.csv": [
        "subject_id,hadm_id,pharmacy_id,drug_type,drug,ndc",
        # Every value is digits, so the column is read as Int64 and loses its leading zeros.
        "1,10,101,MAIN,Quetiapine Fumarate,00904627761",
        "1,10,101,BASE,Sodium Chloride 0.9%,0",
        "2,20,201,MAIN,Insulin,00002821501",
        "2,20,202,MAIN,Heparin,0",
        "3,30,301,MAIN,Acetaminophen,12345678901",
    ],
    "hosp/pharmacy.csv": [
        "subject_id,hadm_id,pharmacy_id,poe_id,starttime,stoptime,medication,route,frequency,doses_per_24_hrs",
        "1,10,101,1-1,2180-01-01 10:00:00,2180-01-02 10:00:00,Quetiapine Fumarate,PO,QHS,1",
        "2,20,201,2-1,2180-01-01 10:00:00,2180-01-02 10:00:00,Insulin,SC,TID,3",
        # The MAIN row carries prescriptions' "no NDC" sentinel `0`.
        "2,20,202,2-2,2180-01-01 10:00:00,2180-01-02 10:00:00,Heparin,SC,BID,2",
        "3,30,301,3-1,2180-01-01 10:00:00,2180-01-02 10:00:00,Acetaminophen,PO,Q6H,4",
        # No prescriptions row, so the join leaves `ndc` null.
        "3,30,302,3-2,2180-01-01 10:00:00,2180-01-02 10:00:00,Ondansetron,IV,Q8H,3",
    ],
    "hosp/emar.csv": [
        "subject_id,hadm_id,emar_id,emar_seq,pharmacy_id,charttime,medication,event_txt",
        "1,10,1-1,1,101,2180-01-01 22:00:00,Quetiapine Fumarate,Administered",
        "2,20,2-1,1,201,2180-01-01 12:00:00,Insulin,Administered",
        "2,20,2-2,2,202,2180-01-01 12:00:00,Heparin,Administered",
        "3,30,3-1,1,302,2180-01-01 12:00:00,Ondansetron,Not Given",
    ],
}

#: code -> its NDC parents.
EXPECTED = {
    "MEDICATION//START//Quetiapine Fumarate//00904627761": ["NDC//00904627761"],
    "MEDICATION//END//Quetiapine Fumarate//00904627761": ["NDC//00904627761"],
    "MEDICATION//START//Insulin//00002821501": ["NDC//00002821501"],
    "MEDICATION//END//Insulin//00002821501": ["NDC//00002821501"],
    "MEDICATION//START//Acetaminophen//12345678901": ["NDC//12345678901"],
    "MEDICATION//END//Acetaminophen//12345678901": ["NDC//12345678901"],
    "MEDICATION//START//Heparin//UNK": [],
    "MEDICATION//END//Heparin//UNK": [],
    "MEDICATION//START//Ondansetron//UNK": [],
    "MEDICATION//END//Ondansetron//UNK": [],
    "MEDICATION//Quetiapine Fumarate//Administered//00904627761": ["NDC//00904627761"],
    "MEDICATION//Insulin//Administered//00002821501": ["NDC//00002821501"],
    "MEDICATION//Heparin//Administered//UNK": [],
    "MEDICATION//Ondansetron//Not Given//UNK": [],
}


def _config() -> dict:
    return yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())


@pytest.fixture(scope="module")
def cohort(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("ndc")
    config = _config()
    # The id columns of the tables left out (icustay_id, order_id, link_order_id) are not in this data.
    etl = config["etl"] | {
        "raw_dataset_version": "synthetic",
        "raw_source_id_columns": ["hadm_id", "emar_id", "poe_id"],
    }
    spec = {"etl": etl, **{table: config[table] for table in TABLES}}
    (root / "spec.yaml").write_text(yaml.safe_dump(spec, sort_keys=False))
    for name, lines in RAW.items():
        path = root / "raw" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n")
    out = root / "MEDS_cohort"
    run_script(
        "meds-extract-run",
        f"spec={root / 'spec.yaml'}",
        f"output_dir={out}",
        f"input_dir={root / 'raw'}",
        "do_download=false",
        "skip_stages=[sumeds_enrich]",
    )
    return out


def test_fixture_ndc_is_read_as_a_number(tmp_path: Path):
    """The fixture reproduces the demo: whole-file inference types its `ndc` column Int64."""
    path = tmp_path / "prescriptions.csv"
    path.write_text("\n".join(RAW["hosp/prescriptions.csv"]) + "\n")
    assert pl.read_csv(path, infer_schema_length=None).schema["ndc"] == pl.Int64


def test_codes_carry_11_digit_ndcs(cohort: Path):
    events = pl.read_parquet(sorted((cohort / "data").rglob("*.parquet")))
    in_data = set(events.filter(pl.col("code").str.starts_with("MEDICATION//"))["code"].to_list())
    assert in_data == set(EXPECTED)


def test_ndc_parents_are_11_digits(cohort: Path):
    codes = pl.read_parquet(cohort / "metadata" / "codes.parquet")
    parents = {
        code: [p for p in (parents or []) if p.startswith("NDC//")]
        for code, parents in zip(codes["code"].to_list(), codes["parent_codes"].to_list(), strict=True)
    }
    assert {code: parents.get(code) for code in EXPECTED} == EXPECTED


@pytest.mark.parametrize("table", TABLES)
@pytest.mark.parametrize("dtype", [pl.Int64, pl.String])
def test_ndc_clean_pads_numbers_and_keeps_strings(table: str, dtype: pl.DataType):
    """`ndc_clean` and the parent, from the shipped config, on `ndc` read as a number or as text.

    On text (the full release) an 11-digit NDC and a non-digit value are kept as they are.
    """
    block = _config()[table]
    numbers = [904627761, 2821501, 12345678901, 0, None]
    strings = ["00904627761", "00002821501", "12345678901", "0", None, "ABC-123"]
    raw = pl.DataFrame({"ndc": numbers if dtype == pl.Int64 else strings}, schema={"ndc": dtype})
    raw = raw.with_columns(**Parser.to_polars({"ndc_clean": block["_table"]["cols"]["ndc_clean"]}))
    event = next(v for k, v in block.items() if k != "_table")
    raw = raw.with_columns(**Parser.to_polars({"parent": event["_metadata"]["_self"]["parent_codes"]}))
    expected = ["00904627761", "00002821501", "12345678901", "UNK", "UNK"]
    if dtype == pl.String:
        expected.append("ABC-123")
    assert raw["ndc_clean"].to_list() == expected
    assert raw["parent"].to_list() == [None if n == "UNK" else f"NDC//{n}" for n in expected]
