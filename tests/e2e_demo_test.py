"""End-to-end run of the shipped ETL over the public MIMIC-IV demo release.

This distribution ships no Python — it is `event_configs.yaml` plus the entry-point
registration that names it `MIMIC-IV`. So the main thing worth testing is that
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
from pathlib import Path

import pyarrow.parquet as pq
from conftest import SPEC
from meds import CodeMetadataSchema

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

    # Code metadata, as read from disk, is valid MEDS code metadata, and every code is
    # described except `hosp/labevents` items absent from mimic-code's lab map (the 53xxx
    # itemids), which have no other source (see "Code metadata" in the README). ICU chart
    # codes (`ICU_CHART//…`) are described from `icu/d_items` and get no exemption. No
    # description may hold a newline: that is MEDS-Extract joining the output of two
    # `_metadata` blocks for one code.
    codes = pq.read_table(metadata / "codes.parquet")
    CodeMetadataSchema.validate(codes)
    described = dict(zip(codes["code"].to_pylist(), codes["description"].to_pylist(), strict=True))
    unmapped_lab = re.compile(r"^LAB//(RESULT|SPECIMEN_COLLECTED)//53\d{3}//")
    undescribed = sorted(c for c, d in described.items() if d is None and not unmapped_lab.match(c))
    assert not undescribed, f"codes without a description: {undescribed}"
    multi = sorted(c for c, d in described.items() if d is not None and "\n" in d)
    assert not multi, f"codes with several joined descriptions: {multi}"
