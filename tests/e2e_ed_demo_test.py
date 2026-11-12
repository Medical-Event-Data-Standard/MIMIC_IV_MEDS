"""`spec=MIMIC-IV-ED dataset_key=demo` end to end, on the public MIMIC-IV demo plus the MIMIC-IV-ED demo.

Unlike MIMIC-Note-Extract, MIMIC-IV-ED has an open-access demo (v2.2, ODbL): 222 ED stays of 64 subjects, all
of them in the MIMIC-IV demo, so the ED events land on existing timelines. This test runs the spec through its
registered name and its `demo` source bucket, which proves the bucket (both PhysioNet demos, the ED one
filtered by its `include` list) and the whole base ETL still work.

The expected ED events are derived independently from the raw ED demo tables below, with polars rather than
MESSY, and compared exactly with what the spec produced: every routing rule (null vitals emit nothing, `pain`
splits into numeric_value or text_value, one event per medication / dispensation, the NDC padding) and every
timing rule (triage at `edstays.intime`, diagnoses and disposition at `edstays.outtime`, the rest at its own
`charttime`) is checked on real rows.
"""

import json
import os
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pyarrow.parquet as pq
import pytest
from conftest import DOWNLOAD_DIR_ENV_VAR, run_script
from meds import CodeMetadataSchema

SPEC = "MIMIC-IV-ED"

TS = "%Y-%m-%d %H:%M:%S"

#: Raw vital-sign column -> code suffix, shared by ed/triage and ed/vitalsign.
VITALS = {
    "temperature": "TEMPERATURE_F",
    "heartrate": "HEART_RATE",
    "resprate": "RESPIRATORY_RATE",
    "o2sat": "O2_SATURATION",
    "sbp": "BP_SYSTOLIC",
    "dbp": "BP_DIASTOLIC",
}

#: The columns compared between the expected and the produced ED events, and their types.
COLUMNS = {
    "subject_id": pl.Int64,
    "ed_stay_id": pl.Int64,
    "hadm_id": pl.Int64,
    "time": pl.Datetime("us"),
    "code": pl.Utf8,
    "numeric_value": pl.Float64,
    "text_value": pl.Utf8,
    "seq_num": pl.Int64,
    "med_rn": pl.Int64,
    "gsn": pl.Int64,
}


def _read(raw: Path, table: str) -> pl.DataFrame:
    return pl.read_csv(raw / "ed" / f"{table}.csv.gz", infer_schema_length=None)


def _frame(df: pl.DataFrame, code: pl.Expr, time: str, **cols: pl.Expr) -> pl.DataFrame:
    """Shape `df` into the compared columns; columns not given are null."""
    given = {
        "subject_id": pl.col("subject_id"),
        "ed_stay_id": pl.col("stay_id"),
        "hadm_id": pl.col("hadm_id"),
        "time": pl.col(time).str.strptime(pl.Datetime("us"), TS),
        "code": code,
        **cols,
    }
    return df.select(given.get(k, pl.lit(None)).cast(t).alias(k) for k, t in COLUMNS.items())


def _measurements(df: pl.DataFrame, context: str, time: str) -> list[pl.DataFrame]:
    """ED//{context}//{vital} for every non-null vital, and the pain split, as the spec should emit them."""
    out = [
        _frame(
            df.filter(pl.col(c).is_not_null()),
            pl.lit(f"ED//{context}//{s}"),
            time,
            numeric_value=pl.col(c).cast(pl.Float64),
        )
        for c, s in VITALS.items()
    ]
    pain = df.filter(pl.col("pain").is_not_null() & (pl.col("pain") != "")).with_columns(
        num=pl.col("pain").str.extract(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*$", 1)
    )
    out.append(
        _frame(
            pain,
            pl.lit(f"ED//{context}//PAIN"),
            time,
            numeric_value=pl.col("num").cast(pl.Float64),
            text_value=pl.when(pl.col("num").is_null()).then(pl.col("pain")),
        )
    )
    return out


def _expected(raw: Path) -> pl.DataFrame:
    stays = _read(raw, "edstays")
    on_stay = stays.select("stay_id", "hadm_id", "intime", "outtime")
    triage = _read(raw, "triage").join(on_stay, on="stay_id")
    vitals = _read(raw, "vitalsign").join(on_stay.select("stay_id", "hadm_id"), on="stay_id")
    dx = _read(raw, "diagnosis").join(on_stay, on="stay_id")
    medrecon = _read(raw, "medrecon").join(on_stay.select("stay_id", "hadm_id"), on="stay_id")
    pyxis = _read(raw, "pyxis").join(on_stay.select("stay_id", "hadm_id"), on="stay_id")
    children = {"triage": triage, "vitalsign": vitals, "diagnosis": dx, "medrecon": medrecon, "pyxis": pyxis}
    for name, df in children.items():
        assert df.height == _read(raw, name).height, f"{name} rows without an ED stay"

    ndc = pl.when(pl.col("ndc") == 0).then(pl.lit("UNK")).otherwise(pl.col("ndc").cast(pl.Utf8).str.zfill(11))
    frames = [
        _frame(stays, pl.format("ED//ARRIVAL//{}", "arrival_transport"), "intime"),
        _frame(stays, pl.format("ED//DISPOSITION//{}", "disposition"), "outtime"),
        *_measurements(triage, "TRIAGE", "intime"),
        _frame(triage.filter(pl.col("acuity").is_not_null()), pl.lit("ED//TRIAGE//ACUITY"), "intime",
               numeric_value=pl.col("acuity").cast(pl.Float64)),
        _frame(triage.filter(pl.col("chiefcomplaint").is_not_null()), pl.lit("ED//TRIAGE//CHIEF_COMPLAINT"),
               "intime", text_value=pl.col("chiefcomplaint")),
        *_measurements(vitals, "VITALSIGN", "charttime"),
        _frame(vitals.filter(pl.col("rhythm").is_not_null()), pl.lit("ED//VITALSIGN//RHYTHM"), "charttime",
               text_value=pl.col("rhythm")),
        _frame(dx, pl.format("ED//DIAGNOSIS//ICD//{}//{}", "icd_version", "icd_code"), "outtime",
               seq_num=pl.col("seq_num")),
        _frame(medrecon.filter(pl.col("etc_rn") == 1), pl.format("ED//MEDRECON//{}//{}", "name", ndc),
               "charttime", gsn=pl.when(pl.col("gsn") != 0).then(pl.col("gsn"))),
        _frame(pyxis.filter(pl.col("gsn_rn") == 1), pl.format("ED//PYXIS//{}", "name"), "charttime",
               med_rn=pl.col("med_rn"), gsn=pl.col("gsn")),
    ]  # fmt: skip
    return pl.concat(frames)


def _rows(df: pl.DataFrame) -> list[tuple]:
    """The compared columns as tuples; numeric_value is float32 in MEDS, hence the rounding."""
    return df.select(list(COLUMNS)).with_columns(pl.col("numeric_value").cast(pl.Float32).round(3)).rows()


def _sorted(rows: Counter) -> list[tuple]:
    return sorted(rows.elements(), key=lambda r: tuple((v is None, "" if v is None else str(v)) for v in r))


def test_e2e_ed_demo():
    with TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        # CI's cached download dir (`MEDS_DEMO_DOWNLOAD_DIR`, shared with conftest's `demo_cohort`): the ED
        # demo lands under its `ed/`, so one cache holds both demos. Unset, both demos go to a temp dir.
        raw = Path(os.environ.get(DOWNLOAD_DIR_ENV_VAR) or tmp / "demo_download").resolve()
        out = tmp / "MEDS_cohort"
        # The spec's own `demo` bucket: the MIMIC-IV demo plus the six ED demo tables. Files that verify
        # against PhysioNet's manifests are skipped, so a warm cache costs only the manifest fetches. As in
        # `demo_cohort`, the suMEDS post-stage is skipped, so this checks the plain ETL output and fetches no
        # ontology data.
        run_script(
            "meds-extract-run", f"spec={SPEC}", f"output_dir={out}", "dataset_key=demo",
            f"download_dest_dir={raw}", "skip_stages=[sumeds_enrich]",
        )  # fmt: skip

        dataset = json.loads((out / "metadata" / "dataset.json").read_text())
        assert dataset["dataset_name"] == SPEC
        assert dataset["dataset_version"].startswith("2.2:")

        data = pl.concat(
            [pl.read_parquet(f) for f in sorted((out / "data").glob("**/*.parquet"))], how="diagonal_relaxed"
        )
        ed = data.filter(pl.col("code").str.starts_with("ED//"))

        # The base ETL is unaffected, and every ED subject is an existing MIMIC-IV demo subject.
        assert data.filter(~pl.col("code").str.starts_with("ED//")).height > 100_000
        demo_subjects = set(pl.read_csv(raw / "hosp" / "patients.csv.gz")["subject_id"])
        assert set(_read(raw, "edstays")["subject_id"]) <= demo_subjects

        # Exactly the expected ED events.
        got, expected = Counter(_rows(ed)), Counter(_rows(_expected(raw)))
        missing, extra = _sorted(expected - got), _sorted(got - expected)
        assert not missing and not extra, (
            f"{len(missing)} expected ED events missing, {len(extra)} unexpected;\n"
            f"first missing: {missing[:3]}\nfirst unexpected: {extra[:3]}"
        )

        # The leakage rule, stated directly: nothing assigned at ED departure appears before it.
        out_at = dict(
            _read(raw, "edstays").select("stay_id", pl.col("outtime").str.strptime(pl.Datetime, TS)).rows()
        )
        late = ed.filter(pl.col("code").str.contains(r"^ED//(DIAGNOSIS|DISPOSITION)//"))
        assert late.height and all(t == out_at[s] for s, t in late.select("ed_stay_id", "time").rows())

        # Every ED code is described once; vitals carry LOINC parents (the ESI has none), medrecon its NDC,
        # diagnoses their ICD.
        codes = pq.read_table(out / "metadata" / "codes.parquet")
        CodeMetadataSchema.validate(codes)
        meta = pl.from_arrow(codes).filter(pl.col("code").str.starts_with("ED//"))
        assert set(meta["code"]) == set(ed["code"])
        assert meta.filter(pl.col("description").is_null()).is_empty()
        assert not meta.filter(pl.col("description").str.contains("\n")).height
        parents = dict(meta.select("code", "parent_codes").iter_rows())
        assert parents["ED//TRIAGE//HEART_RATE"] == ["LOINC/8867-4"]
        assert parents["ED//VITALSIGN//BP_SYSTOLIC"] == ["LOINC/8480-6"]
        assert parents["ED//TRIAGE//ACUITY"] in (None, [])
        assert parents["ED//ARRIVAL//AMBULANCE"] in (None, [])
        ndc_parents = [
            p for c, p in parents.items() if c.startswith("ED//MEDRECON//") and not c.endswith("//UNK")
        ]
        assert ndc_parents and all(len(p) == 1 and len(p[0]) == len("NDC/") + 11 for p in ndc_parents)
        icd = [p for c, p in parents.items() if c.startswith("ED//DIAGNOSIS//ICD//10//")]
        assert icd and all(p and p[0].startswith("ICD10CM/") for p in icd)


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v", "-s"])
