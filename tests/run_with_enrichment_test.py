"""`meds-extract-run spec=MIMIC-IV` runs the ETL and then suMEDS enrichment, offline.

The spec declares suMEDS's `sumeds_enrich` stage in `etl.post_stages`, with the rules this package
registers in suMEDS's `sumeds.rules` entry-point group under the same name. The ETL runs from the raw demo
files that the `demo_cohort` build of `conftest.py` downloaded (`do_download=false`), and that build, which
skips the stage, is the plain-ETL baseline the enriched output is compared with. The ontology source is a
tiny pack repository in suMEDS's `sumeds-ontologies/0.1` layout, built here with made-up titles for a few
ICD codes of the demo and their ancestors, and passed to the stage as a run-time override, so the test
reads nothing over the network and does not depend on the published packs. With an empty pack source
instead, the enrichment fails without failing the ETL: the plain `codes.parquet` is kept, the failure is
recorded in `metadata/dataset.json`, and `suMEDS-enrich` on the output enriches it later.
"""

import csv
import hashlib
import io
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import polars as pl
import pyarrow.parquet as pq
import pytest
from conftest import SPEC, run_script
from meds import CodeMetadataSchema, DatasetMetadataSchema

#: vocabulary -> code -> parent (None for a top-level category). Each vocabulary needs a pack, as every
#: ICD rule validates its codes against one; codes of the demo that are not listed stay unenriched.
PACKS = {
    "ICD10CM": {
        "I27": None,
        "I27.2": "I27",
        "I27.20": "I27.2",
        "I50": None,
        "I50.2": "I50",
        "I50.23": "I50.2",
    },
    "ICD9CM": {"401": None, "401.9": "401"},
    "ICD10PCS": {"02HV33Z": None},
    "ICD9Proc": {"96": None, "96.0": "96", "96.04": "96.0"},
}
RELEASE = "synthetic"


def title(vocabulary: str, code: str) -> str:
    return f"Synthetic {vocabulary} title {code}"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.fixture(scope="module")
def ontology_source(tmp_path_factory) -> Path:
    """A local `sumeds-ontologies/0.1` repository holding the synthetic ICD packs."""
    root = tmp_path_factory.mktemp("ontologies")
    columns = ["code", "description", "parent", "first_year", "last_year"]
    vocabularies = {}
    for vocabulary, rows in PACKS.items():
        directory = root / vocabulary / RELEASE
        directory.mkdir(parents=True)
        by_shard: dict[str, list[list[str]]] = {}
        for code, parent in rows.items():
            by_shard.setdefault(code[:1], []).append([code, title(vocabulary, code), parent or "", "", ""])
        shards = {}
        for key, shard_rows in by_shard.items():
            buffer = io.StringIO()
            csv.writer(buffer, lineterminator="\n").writerows([columns, *shard_rows])
            data = buffer.getvalue().encode()
            (directory / f"{key}.csv").write_bytes(data)
            shards[key] = {
                "file": f"{key}.csv",
                "rows": len(shard_rows),
                "bytes": len(data),
                "sha256": _sha256(data),
            }
        manifest = {
            "schema": "sumeds-ontology-pack/0.1",
            "vocabulary": vocabulary,
            "release": RELEASE,
            "columns": columns,
            "fields": {"code": "code", "description": "description", "parent": "parent"},
            "shard_key_length": 1,
            "rows": len(rows),
            "bytes": sum(s["bytes"] for s in shards.values()),
            "shards": shards,
        }
        manifest_bytes = json.dumps(manifest).encode()
        (directory / "manifest.json").write_bytes(manifest_bytes)
        vocabularies[vocabulary] = {
            "manifest": f"{vocabulary}/{RELEASE}/manifest.json",
            "sha256": _sha256(manifest_bytes),
            "release": RELEASE,
            "rows": manifest["rows"],
            "bytes": manifest["bytes"],
        }
    (root / "index.json").write_text(
        json.dumps({"schema": "sumeds-ontologies/0.1", "vocabularies": vocabularies})
    )
    return root


def _run_with_enrichment(raw_dir: Path, ontology_source: Path):
    with TemporaryDirectory() as temp_dir:
        out = Path(temp_dir) / "MEDS_cohort"
        run_script(
            "meds-extract-run",
            f"spec={SPEC}",
            f"output_dir={out}",
            "dataset_key=demo",
            "do_download=false",
            f"input_dir={raw_dir}",
            # Options of the `sumeds_enrich` stage. Overrides reach every stage of the pipeline, so
            # `++` adds the key where a stage does not have it.
            f"overrides=['++stage_cfg.ontology_source={ontology_source}','++stage_cfg.offline=true']",
        )
        yield out


@pytest.fixture(scope="module")
def enriched_cohort(demo_raw_dir: Path, ontology_source: Path):
    yield from _run_with_enrichment(demo_raw_dir, ontology_source)


@pytest.fixture(scope="module")
def soft_failed_cohort(demo_raw_dir: Path, tmp_path_factory):
    """The same run with an empty pack source, as on a site that cannot reach the packs."""
    yield from _run_with_enrichment(demo_raw_dir, tmp_path_factory.mktemp("no_packs"))


def _enrichment_record(cohort: Path) -> dict:
    """The `sumeds_enrichment` record of the cohort's `dataset.json`, which must stay valid MEDS."""
    metadata = json.loads((cohort / "metadata" / "dataset.json").read_text())
    DatasetMetadataSchema.validate(metadata)
    return metadata["sumeds_enrichment"]


def _codes(cohort: Path) -> dict[str, dict]:
    """The rows of `codes.parquet` by code; names maps come back as dicts on any Polars version."""
    table = pq.read_table(cohort / "metadata" / "codes.parquet")
    rows = {}
    for row in table.to_pylist():
        for column, value in row.items():
            if column.endswith("_names") and value is not None:
                row[column] = dict(value)
        rows[row["code"]] = row
    return rows


@pytest.fixture(scope="module")
def plain_codes(demo_cohort: Path) -> dict[str, dict]:
    return _codes(demo_cohort)


@pytest.fixture(scope="module")
def enriched_codes(enriched_cohort: Path) -> dict[str, dict]:
    return _codes(enriched_cohort)


def test_the_plain_build_records_no_enrichment(demo_cohort: Path):
    """`skip_stages=[sumeds_enrich]` leaves the stage out, so the plain build records no enrichment.

    Its codes are the ones a failed enrichment keeps (`test_failed_enrichment_keeps_the_plain_etl_output`).
    """
    assert "sumeds_enrichment" not in json.loads((demo_cohort / "metadata" / "dataset.json").read_text())


def test_codes_parquet_is_replaced_by_valid_meds_metadata(demo_cohort: Path, enriched_cohort: Path):
    metadata = enriched_cohort / "metadata"
    CodeMetadataSchema.validate(pq.read_table(metadata / "codes.parquet"))
    files = {fp.name for fp in metadata.iterdir()}
    assert files == {fp.name for fp in (demo_cohort / "metadata").iterdir()}, "enrichment left a file behind"


def test_data_matches_the_plain_etl(demo_cohort: Path, enriched_cohort: Path):
    key = ["subject_id", "time", "code"]

    def events(cohort: Path) -> pl.DataFrame:
        return pl.concat([pl.read_parquet(fp, columns=key) for fp in (cohort / "data").rglob("*.parquet")])

    assert events(enriched_cohort).sort(key).equals(events(demo_cohort).sort(key))


def test_enrichment_is_non_destructive(plain_codes: dict, enriched_codes: dict):
    """Every code keeps its description, and its parents stay at the head of the enriched list."""
    assert set(enriched_codes) == set(plain_codes)
    changed = []
    for code, plain in plain_codes.items():
        enriched = enriched_codes[code]
        if plain["description"] is not None and enriched["description"] != plain["description"]:
            changed.append(code)
        parents = plain["parent_codes"] or []
        if (enriched["parent_codes"] or [])[: len(parents)] != parents:
            changed.append(code)
    assert not changed, f"enrichment rewrote the ETL's metadata of {sorted(changed)}"


@pytest.mark.parametrize(
    "code,vocabulary,concept_code,ancestors",
    [
        ("DIAGNOSIS//ICD//10//I5023", "ICD10CM", "I50.23", ["I50.2", "I50"]),
        ("DIAGNOSIS//ICD//9//4019", "ICD9CM", "401.9", ["401"]),
        ("PROCEDURE//ICD//10//02HV33Z", "ICD10PCS", "02HV33Z", []),
        ("PROCEDURE//ICD//9//9604", "ICD9Proc", "96.04", ["96.0", "96"]),
    ],
)
def test_icd_codes_get_their_pack_ancestors(
    plain_codes, enriched_codes, code, vocabulary, concept_code, ancestors
):
    """The ETL's own vocabulary reference stays first and is not repeated; the pack ancestors follow."""
    reference = f"{vocabulary}/{concept_code}"
    assert plain_codes[code]["parent_codes"] == [reference]
    enriched = enriched_codes[code]
    added = [f"{vocabulary}//{ancestor}" for ancestor in ancestors]
    assert enriched["parent_codes"] == [reference, *added]
    assert (enriched["vocabulary_id"], enriched["concept_code"]) == (vocabulary, concept_code)
    # suMEDS 0.6 names the entries in a code-to-name map, keyed as spelled in the list.
    names = {reference: title(vocabulary, concept_code)}
    names |= {parent: title(vocabulary, ancestor) for parent, ancestor in zip(added, ancestors, strict=True)}
    assert enriched["parent_names"] == names


def test_icd_codes_get_parents_from_the_same_dataset(enriched_codes):
    """`I2720` gets `I272`, the demo code whose vocabulary code `I27.2` is its nearest pack ancestor."""
    enriched = enriched_codes["DIAGNOSIS//ICD//10//I2720"]
    assert enriched["parent_codes"] == [
        "ICD10CM/I27.20",
        "ICD10CM//I27.2",
        "ICD10CM//I27",
        "DIAGNOSIS//ICD//10//I272",
    ]
    # A same-dataset parent is described in its own row, so it has no entry in the names map.
    assert "DIAGNOSIS//ICD//10//I272" not in enriched["parent_names"]


def test_codes_missing_from_the_packs_are_left_alone(plain_codes, enriched_codes):
    plain, enriched = plain_codes["DIAGNOSIS//ICD//10//I10"], enriched_codes["DIAGNOSIS//ICD//10//I10"]
    assert enriched["parent_codes"] == plain["parent_codes"] == ["ICD10CM/I10"]
    assert enriched["vocabulary_id"] is None and not enriched["parent_names"]


@pytest.mark.parametrize("gender", ["F", "M"])
def test_gender_is_tagged_with_the_omop_gender_code(plain_codes, enriched_codes, gender):
    assert not plain_codes[f"GENDER//{gender}"]["parent_codes"]
    assert enriched_codes[f"GENDER//{gender}"]["parent_codes"] == [f"Gender//{gender}"]


def test_dataset_json_records_the_enrichment(enriched_cohort: Path):
    record = _enrichment_record(enriched_cohort)
    assert record["enriched"] is True
    assert record["rules"] == SPEC
    coverage = record["coverage"]
    assert coverage["with_parent_codes"][1] > coverage["with_parent_codes"][0]


def test_failed_enrichment_keeps_the_plain_etl_output(demo_cohort: Path, soft_failed_cohort: Path):
    codes = Path("metadata", "codes.parquet")
    assert (soft_failed_cohort / codes).read_bytes() == (demo_cohort / codes).read_bytes()
    record = _enrichment_record(soft_failed_cohort)
    assert record["enriched"] is False
    # The pack directory is not recorded, so the retry asks for it again.
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT> --ontology-source <PACKS_DIR>"


def test_failed_enrichment_can_be_retried(
    soft_failed_cohort: Path, ontology_source: Path, enriched_cohort: Path, tmp_path: Path
):
    out = tmp_path / "MEDS_cohort"
    shutil.copytree(soft_failed_cohort / "metadata", out / "metadata")
    run_script("suMEDS-enrich", str(out), "--ontology-source", str(ontology_source), "--offline")
    assert _enrichment_record(out)["enriched"] is True
    codes = Path("metadata", "codes.parquet")
    assert pq.read_table(out / codes).equals(pq.read_table(enriched_cohort / codes))
