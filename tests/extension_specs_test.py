"""The generated extension specs are current, and differ from the base only where they should.

`event_configs_<name>.yaml` is generated from `event_configs.yaml` plus
`extensions/<name>.yaml` by `scripts/build_extension_specs.py` and committed, because the entry
point needs a file in the package. An edit to the base or a fragment without re-running the
script would ship a spec that silently disagrees with the base; this catches it. Needs no data.
"""

import subprocess
import sys
from importlib.metadata import entry_points
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
PKG = REPO / "src" / "MIMIC_IV_MEDS"
SCRIPT = REPO / "scripts" / "build_extension_specs.py"
FRAGMENTS = sorted((PKG / "extensions").glob("*.yaml"))


def test_generated_specs_are_current():
    result = subprocess.run([sys.executable, str(SCRIPT), "--check"], capture_output=True, text=True)
    assert result.returncode == 0, f"{result.stdout}{result.stderr}"


@pytest.mark.parametrize("fragment", FRAGMENTS, ids=lambda p: p.stem)
def test_extension_spec_is_base_plus_fragment(fragment):
    base = yaml.safe_load((PKG / "event_configs.yaml").read_text())
    ext = yaml.safe_load(fragment.read_text())
    spec = yaml.safe_load((PKG / f"event_configs_{fragment.stem}.yaml").read_text())

    tables = {k for k in base if k not in ("etl", "sources")}
    ext_tables = {k for k in ext if k not in ("etl", "sources")}
    assert set(spec) == tables | ext_tables | {"etl", "sources"}
    assert {k: spec[k] for k in tables} == {k: base[k] for k in tables}
    assert {k: spec[k] for k in ext_tables} == {k: ext[k] for k in ext_tables}
    assert spec["etl"] == {**base["etl"], **ext["etl"]}

    # Every bucket the fragment extends starts with the base bucket unchanged; nothing else
    # but `common` and `dataset_version` survives.
    for bucket, entries in ext["sources"].items():
        assert spec["sources"][bucket] == base["sources"][bucket] + entries
    assert set(spec["sources"]) == set(ext["sources"]) | {"common", "dataset_version"}
    assert spec["sources"]["common"] == base["sources"]["common"]


@pytest.mark.parametrize("fragment", FRAGMENTS, ids=lambda p: p.stem)
def test_extension_spec_is_registered(fragment):
    """The generated file is reachable as `spec=<etl.dataset_name>`."""
    name = yaml.safe_load(fragment.read_text())["etl"]["dataset_name"]
    registered = {ep.name: ep.value for ep in entry_points(group="MEDS_extract.pipelines")}
    assert registered.get(name) == f"MIMIC_IV_MEDS:event_configs_{fragment.stem}.yaml", (
        f"{name} is not registered in pyproject.toml (or the package needs reinstalling): {registered}"
    )
