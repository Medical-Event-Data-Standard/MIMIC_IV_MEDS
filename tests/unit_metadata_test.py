"""Every code with a numeric value carries its unit in `metadata/codes.parquet`.

The ETL runs the shipped config's numeric tables over a few made-up rows (no download): the
unit comes from the code's own unit component (labs, charted items, outputs), from the name of
an OMR measurement, from the per-event unit column of infusions, and, for the infusion weight,
from MIMIC-IV's documentation. On the demo cohort the same column is checked by `e2e_demo_test.py`.
"""

from importlib.resources import files

import polars as pl
import pytest
import yaml
from conftest import run_script

#: The tables whose events carry a numeric value, and the dictionaries their `_metadata` blocks read.
TABLES = ["hosp/labevents", "hosp/omr", "icu/chartevents", "icu/inputevents", "icu/outputevents"]

LAB_MAP = "itemid (omop_source_code),label,omop_concept_name,omop_vocabulary_id,omop_concept_code"
RAW = {
    "hosp/labevents.csv": [
        "subject_id,hadm_id,itemid,charttime,storetime,value,valuenum,valueuom,priority",
        "1,10,50912,2180-01-01 10:00:00,2180-01-01 11:00:00,1.1,1.1,mg/dL,STAT",
        "2,20,50912,2180-01-02 10:00:00,2180-01-02 11:00:00,0.9,0.9,mg/dL,ROUTINE",
        "3,30,50912,2180-01-03 10:00:00,2180-01-03 11:00:00,1.0,1.0,,ROUTINE",
    ],
    "d_labitems_to_loinc.csv": [LAB_MAP, "50912,Creatinine,Creatinine,LOINC,2160-0"],
    "hosp/omr.csv": [
        "subject_id,chartdate,seq_num,result_name,result_value",
        "1,2180-01-01,1,Weight (Lbs),150",
        "2,2180-01-02,1,BMI (kg/m2),26.6",
        "3,2180-01-03,1,Blood Pressure,120/70",
        "4,2180-01-04,1,eGFR,>60",
    ],
    "icu/chartevents.csv": [
        "subject_id,hadm_id,stay_id,charttime,itemid,value,valuenum,valueuom",
        "1,10,100,2180-01-01 10:00:00,220045,80,80,bpm",
        "2,20,200,2180-01-02 10:00:00,220045,90,90,bpm",
    ],
    "icu/d_items.csv": [
        "itemid,label,unitname",
        "220045,Heart Rate,bpm",
        "221906,Norepinephrine,mg",
        "226559,Foley,mL",
    ],
    "meas_chartevents_main.csv": [LAB_MAP, "220045,Heart Rate,Heart rate,LOINC,8867-4"],
    "icu/inputevents.csv": [
        "subject_id,hadm_id,stay_id,starttime,endtime,itemid,amount,amountuom,rate,rateuom,orderid,"
        "linkorderid,ordercategorydescription,patientweight,statusdescription",
        "1,10,100,2180-01-01 10:00:00,2180-01-01 11:00:00,221906,1.0,mg,0.1,mcg/kg/min,1,1,"
        "Continuous Med,70.0,FinishedRunning",
        "2,20,200,2180-01-02 10:00:00,2180-01-02 11:00:00,221906,500,mcg,0.2,mcg/kg/min,2,2,"
        "Continuous Med,80.0,FinishedRunning",
    ],
    "inputevents_to_rxnorm.csv": [LAB_MAP, "221906,Norepinephrine,norepinephrine,RxNorm,7512"],
    "icu/outputevents.csv": [
        "subject_id,hadm_id,stay_id,charttime,itemid,value,valueuom",
        "1,10,100,2180-01-01 10:00:00,226559,200,mL",
    ],
    "outputevents_to_loinc.csv": [LAB_MAP, "226559,Foley,Urine output,LOINC,9187-6"],
}

#: code -> the units its `unit` metadata must hold.
EXPECTED = {
    "LAB//RESULT//50912//mg/dL": ["mg/dL"],
    "LAB//220045//bpm": ["bpm"],
    "SUBJECT_FLUID_OUTPUT//226559//mL": ["mL"],
    "Weight (Lbs)": ["Lbs"],
    "BMI (kg/m2)": ["kg/m2"],
    "INFUSION_START//221906": ["mcg/kg/min"],
    # The unit is not part of this code, so both charted units are kept.
    "INFUSION_END//221906": ["mcg", "mg"],
    # From the MIMIC-IV documentation of inputevents.patientweight.
    "SUBJECT_WEIGHT_AT_INFUSION//KG": ["kg"],
}
#: Codes without a recorded unit, which get none.
NO_UNIT = ["LAB//RESULT//50912//UNK", "Blood Pressure//SYSTOLIC", "eGFR"]


@pytest.fixture(scope="module")
def codes(tmp_path_factory) -> pl.DataFrame:
    root = tmp_path_factory.mktemp("units")
    config = yaml.safe_load(files("MIMIC_IV_MEDS").joinpath("event_configs.yaml").read_text())
    # The id columns of the tables left out (emar_id, poe_id) are not in this data.
    etl = config["etl"] | {
        "raw_dataset_version": "synthetic",
        "raw_source_id_columns": ["hadm_id", "icustay_id", "order_id", "link_order_id"],
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
    return pl.read_parquet(out / "metadata" / "codes.parquet")


def _units(codes: pl.DataFrame) -> dict[str, list[str] | None]:
    # MEDS-Extract writes `_metadata` columns as lists, aggregated per code.
    return dict(zip(codes["code"].to_list(), codes["unit"].to_list(), strict=True))


@pytest.mark.parametrize("code", EXPECTED)
def test_numeric_codes_carry_their_unit(codes: pl.DataFrame, code: str):
    assert sorted(_units(codes)[code] or []) == EXPECTED[code]


@pytest.mark.parametrize("code", NO_UNIT)
def test_codes_without_a_recorded_unit_get_none(codes: pl.DataFrame, code: str):
    assert not _units(codes)[code]
