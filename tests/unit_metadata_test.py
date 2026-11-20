"""Every code with a numeric value carries its one unit in `metadata/codes.parquet`, and only there.

The ETL runs the shipped config's numeric tables over a few made-up rows (no download): the
unit comes from the code's own unit component (labs, charted items, outputs), from the infusion
rows (in the code only for an itemid that mixes units), from the name of an OMR measurement, from
an ICU item's label where it was charted without one, and, for the infusion weight, from
MIMIC-IV's documentation. MEDS-Extract writes it as one String per code; suMEDS's
`sumeds_normalize_units` stage then writes it as UCUM in `unit` and keeps the spelling as recorded
in `unit_source_value`. The events have no `unit` column. On the demo cohort the same columns are
checked by `e2e_demo_test.py`.
"""

from importlib.resources import files
from pathlib import Path

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
        "3,30,300,2180-01-03 10:00:00,226531,150,150,",
    ],
    "icu/d_items.csv": [
        "itemid,label,unitname",
        "220045,Heart Rate,bpm",
        "226531,Admission Weight (lbs.),None",
        "221906,Norepinephrine,mg",
        "225158,NaCl 0.9%,mL",
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
        # A push has an amount but no rate.
        "3,30,300,2180-01-03 10:00:00,2180-01-03 10:01:00,221906,5,mg,,,3,3,Drug Push,90.0,FinishedRunning",
        # Starts of one itemid at rates in two units, and one without a rate; all ends in mL.
        "1,10,100,2180-01-01 12:00:00,2180-01-01 13:00:00,225158,100,mL,100,mL/hour,4,4,"
        "Continuous IV,70.0,FinishedRunning",
        "2,20,200,2180-01-02 12:00:00,2180-01-02 13:00:00,225158,60,mL,1,mL/min,5,5,"
        "Continuous IV,80.0,FinishedRunning",
        "3,30,300,2180-01-03 12:00:00,2180-01-03 12:01:00,225158,250,mL,,,6,6,"
        "Fluid Bolus,90.0,FinishedRunning",
    ],
    "inputevents_to_rxnorm.csv": [
        LAB_MAP,
        "221906,Norepinephrine,norepinephrine,RxNorm,7512",
        "225158,NaCl 0.9%,sodium chloride,RxNorm,9863",
    ],
    "icu/outputevents.csv": [
        "subject_id,hadm_id,stay_id,charttime,itemid,value,valueuom",
        "1,10,100,2180-01-01 10:00:00,226559,200,mL",
    ],
    "outputevents_to_loinc.csv": [LAB_MAP, "226559,Foley,Urine output,LOINC,9187-6"],
}

#: code -> its unit as recorded (`unit_source_value`) and as UCUM (`unit`). Codes keep the recorded
#: spelling (`…//mcg`, `…//mL/hour`); only the `unit` column is UCUM.
EXPECTED = {
    "LAB//RESULT//50912//mg/dL": ("mg/dL", "mg/dL"),
    "ICU_CHART//220045//bpm": ("bpm", "{beats}/min"),
    # Charted without a unit; the item's label states it.
    "ICU_CHART//226531//UNK": ("lbs.", "[lb_av]"),
    "SUBJECT_FLUID_OUTPUT//226559//mL": ("mL", "mL"),
    "Weight (Lbs)": ("Lbs", "[lb_av]"),
    "BMI (kg/m2)": ("kg/m2", "kg/m2"),
    # An itemid whose starts have one rate unit keeps its code; the push without a rate has none.
    "INFUSION_START//221906": ("mcg/kg/min", "ug/kg/min"),
    # Its ends are recorded in two units, which gives two codes, one per unit.
    "INFUSION_END//221906//mg": ("mg", "mg"),
    "INFUSION_END//221906//mcg": ("mcg", "ug"),
    # And the other way round.
    "INFUSION_START//225158//mL/hour": ("mL/hour", "mL/h"),
    "INFUSION_START//225158//mL/min": ("mL/min", "mL/min"),
    "INFUSION_END//225158": ("mL", "mL"),
    # From the MIMIC-IV documentation of inputevents.patientweight, written as UCUM by the ETL.
    "SUBJECT_WEIGHT_AT_INFUSION": ("kg", "kg"),
}
#: Codes without a recorded unit, which get none.
#: The start without a rate of an itemid whose starts mix units is `…//UNK`, which has none.
NO_UNIT = ["LAB//RESULT//50912//UNK", "Blood Pressure//SYSTOLIC", "eGFR", "INFUSION_START//225158//UNK"]


@pytest.fixture(scope="module")
def cohort(tmp_path_factory) -> Path:
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
    return out


@pytest.fixture(scope="module")
def codes(cohort: Path) -> pl.DataFrame:
    return pl.read_parquet(cohort / "metadata" / "codes.parquet")


def _units(codes: pl.DataFrame, column: str = "unit") -> dict[str, str | None]:
    return dict(zip(codes["code"].to_list(), codes[column].to_list(), strict=True))


def test_unit_is_a_string(codes: pl.DataFrame):
    """MEDS-Extract (0.9 and later) writes one String per code, and suMEDS keeps it one."""
    assert codes.schema["unit"] == pl.String
    assert codes.schema["unit_source_value"] in (pl.String, pl.Utf8)


def test_events_have_no_unit(cohort: Path):
    """A code's unit lives in `codes.parquet` only; join the events to it on `code` to get one."""
    shards = sorted((cohort / "data").rglob("*.parquet"))
    assert shards
    for shard in shards:
        assert "unit" not in pl.read_parquet_schema(shard)


def test_only_itemids_that_mix_units_have_it_in_the_code(codes: pl.DataFrame):
    infusions = sorted(c for c in codes["code"].to_list() if c.startswith("INFUSION_"))
    assert infusions == [
        "INFUSION_END//221906",  # the stem row of the two ends below, without events
        "INFUSION_END//221906//mcg",
        "INFUSION_END//221906//mg",
        "INFUSION_END//225158",
        "INFUSION_START//221906",
        "INFUSION_START//225158",  # the stem row
        "INFUSION_START//225158//UNK",
        "INFUSION_START//225158//mL/hour",
        "INFUSION_START//225158//mL/min",
    ]


@pytest.mark.parametrize("code", EXPECTED)
def test_numeric_codes_carry_their_unit(codes: pl.DataFrame, code: str):
    recorded, ucum = EXPECTED[code]
    assert _units(codes, "unit_source_value")[code] == recorded
    assert _units(codes)[code] == ucum


def test_unit_siblings_get_the_code_without_the_unit_as_parent(codes: pl.DataFrame):
    parents = dict(zip(codes["code"].to_list(), codes["parent_codes"].to_list(), strict=True))
    for code in ["INFUSION_END//221906//mg", "INFUSION_END//221906//mcg"]:
        assert parents[code] == ["RxNorm/7512", "INFUSION_END//221906"]
    for code in ["INFUSION_START//225158//mL/hour", "INFUSION_START//225158//mL/min"]:
        assert parents[code] == ["RxNorm/9863", "INFUSION_START//225158"]
    # A start whose unit was not recorded (`UNK`) joins its item's siblings; codes without unit
    # siblings get no extra parent.
    assert parents["INFUSION_START//225158//UNK"] == ["RxNorm/9863", "INFUSION_START//225158"]
    assert parents["INFUSION_START//221906"] == ["RxNorm/7512"]
    # The stem has a row of its own, with what both siblings share and no unit.
    stem = codes.filter(pl.col("code") == "INFUSION_END//221906").to_dicts()
    assert [{k: row[k] for k in ("description", "parent_codes", "unit")} for row in stem] == [
        {"description": "Norepinephrine", "parent_codes": ["RxNorm/7512"], "unit": None}
    ]


@pytest.mark.parametrize("code", NO_UNIT)
def test_codes_without_a_recorded_unit_get_none(codes: pl.DataFrame, code: str):
    assert _units(codes)[code] is None
    assert _units(codes, "unit_source_value")[code] is None
