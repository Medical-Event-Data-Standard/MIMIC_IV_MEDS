"""The `mimic_iv_unit_keys` pre-stage finds the infusion itemids recorded in more than one unit.

Its logic is in the doctest of `itemids_with_unit_in_code`; these tests read the raw table the way
the pre-stage does, from a gzipped csv as PhysioNet ships it and from a directory of parquet shards.
How the config uses the side table is tested in `unit_metadata_test.py`.
"""

import gzip

import polars as pl
import pytest

from MIMIC_IV_MEDS.unit_keys import SIDE_TABLE_NAME, write_unit_keys

ROWS = pl.DataFrame(
    {
        "subject_id": [1, 1, 2, 2, 3],
        "starttime": ["2180-01-01 10:00:00"] * 5,
        "endtime": ["2180-01-01 11:00:00"] * 4 + [None],
        "itemid": [221906, 221906, 225158, 225158, 225158],
        "rate": [0.1, 0.2, 100.0, 1.0, None],
        "rateuom": ["mcg/kg/min", "mcg/kg/min", "mL/hour", "mL/min", None],
        "amount": [1.0, 500.0, 100.0, 60.0, 250.0],
        "amountuom": ["mg", "mcg", "mL", "mL", "L"],
    }
)
#: 221906's ends mix mg and mcg; 225158's starts mix two rate units, and its row in L has no end.
EXPECTED = [(221906, None, True), (225158, True, None)]


@pytest.mark.parametrize("layout", ["csv.gz", "parquet shards"])
def test_write_unit_keys(tmp_path, layout: str):
    raw = tmp_path / "raw"
    (raw / "icu").mkdir(parents=True)
    if layout == "csv.gz":
        with gzip.open(raw / "icu" / "inputevents.csv.gz", "wb") as f:
            ROWS.write_csv(f)
    else:
        (raw / "icu" / "inputevents").mkdir()
        ROWS.head(2).write_parquet(raw / "icu" / "inputevents" / "0.parquet")
        ROWS.tail(3).write_parquet(raw / "icu" / "inputevents" / "1.parquet")
    out = write_unit_keys(raw, tmp_path / "staged")
    assert out == tmp_path / "staged" / f"{SIDE_TABLE_NAME}.parquet"
    table = pl.read_parquet(out)
    assert table.schema == pl.Schema(
        {"itemid": pl.Int64, "start_unit_in_code": pl.Boolean, "end_unit_in_code": pl.Boolean}
    )
    assert table.rows() == EXPECTED
