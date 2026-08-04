"""Pins how this package presents itself to ``meds-extract-run``.

The ETL is a registration plus a config file, so these are the offline tests that the
registration works and that the runner derives the right things from it: the entry point
resolves to the bundled MESSY file, the dataset identity and release version come out
right for every sources bucket, and the pipeline the runner synthesizes is the canonical
one with our stage options applied.

All of it is exercised through the public ``MessyConfig`` surface — the same calls
``meds-extract-run`` makes — so these tests fail if either the registration or upstream's
contract moves. The remaining end-to-end behavior (that the pipeline actually runs and
produces a valid cohort) is ``e2e_demo_test.py``'s job.
"""

from importlib.metadata import version
from pathlib import Path

import pytest
from MEDS_extract.config import EtlConfig, MessyConfig

PKG_VERSION = version("MIMIC-IV-MEDS")

#: Buckets in the shipped ``sources:`` block that name a MIMIC-IV release, and the raw
#: version each one must report. ``common`` (the concept maps) is release-independent.
RELEASES = {"dataset": "3.1", "demo": "2.2"}


def test_registered_name_resolves_to_the_bundled_spec(messy, spec_name):
    """``spec=MIMIC-IV`` must resolve to the YAML this distribution ships.

    ``spec_ref`` is the *portable* reference the runner writes into the synthesized
    pipeline's ``MESSY_config_fp``, so it must be the ``pkg://`` form rather than a local
    filesystem path: every stage re-resolves it, potentially on another machine.
    """
    assert messy.registered_name == spec_name
    assert messy.spec_ref == "pkg://MIMIC_IV_MEDS.configs.event_configs.yaml"
    assert messy.dist_version == PKG_VERSION


def test_dataset_identity(messy, spec_name, spec_fp):
    """The dataset name reaching MEDS metadata is ``MIMIC-IV``, from both routes.

    The registry supplies it for ``spec=MIMIC-IV``; the ``etl.dataset_name`` fallback
    supplies it when the same file is resolved by ``pkg://`` reference or path, which has
    no distribution to ask. They must agree.
    """
    assert messy.dataset_name == spec_name
    assert MessyConfig.load(str(spec_fp)).dataset_name == spec_name


@pytest.mark.parametrize(("key", "raw_version"), RELEASES.items())
def test_release_version_is_stamped_per_bucket(messy, key, raw_version):
    """Each bucket stamps its own release version, combined with this ETL's version.

    Demo and full MIMIC-IV are different releases (2.2 vs 3.1), so a single dataset-wide
    version would mislabel one of them — the pre-0.7 runner stamped 3.1 for demo runs. The
    ``<raw release>:<ETL package version>`` shape is what downstream consumers read out of
    ``metadata/dataset.json`` to identify a cohort.
    """
    assert messy.raw_version_for(key) == raw_version
    assert messy.dataset_version_for(key) == f"{raw_version}:{PKG_VERSION}"


def test_synthesized_pipeline_is_canonical_with_our_options(messy, spec_name):
    """The runner's pipeline: canonical stage order, our one stage option, inlined paths.

    Asserting the whole synthesized config (rather than spot-checking keys) is what makes
    this a regression test for the ETL as a whole — any drift in the stage sequence, the
    ``etl:`` options we set, or the identity stamp shows up here as a diff.
    """
    assert messy.pipeline_config(input_dir=Path("/raw"), output_dir=Path("/cohort"), key="demo") == {
        "etl_metadata": {
            "dataset_name": spec_name,
            "dataset_version": f"{RELEASES['demo']}:{PKG_VERSION}",
        },
        "MESSY_config_fp": "pkg://MIMIC_IV_MEDS.configs.event_configs.yaml",
        "input_dir": "/raw",
        "output_dir": "/cohort",
        "shards_map_fp": "/cohort/metadata/.shards.json",
        "stages": [
            "shard_events",
            {"split_and_shard_subjects": {"n_subjects_per_shard": 1000}},
            "convert_to_subject_sharded",
            "convert_to_MEDS_events",
            "merge_to_MEDS_cohort",
            "extract_code_metadata",
            "finalize_MEDS_metadata",
            "finalize_MEDS_data",
        ],
    }
    # The stage sequence above is upstream's canonical one, not a local choice: an
    # upstream stage rename would otherwise look like our config drifting.
    assert [
        next(iter(s)) if isinstance(s, dict) else s
        for s in messy.pipeline_config(input_dir=Path("/raw"), output_dir=Path("/cohort"))["stages"]
    ] == list(EtlConfig.DEFAULT_PIPELINE)


def test_every_event_table_parses(messy, raw_spec):
    """Every table in the spec must parse, and every declared table must be reachable.

    Accessing ``event_tables`` compiles each table's dftly programs — the codes, times,
    joins, and ``_metadata`` blocks — so this is the cheap, offline guard that a config
    edit is at least well-formed. The count check catches the silent failure mode a parse
    check alone would miss: a table that is dropped rather than mis-parsed (e.g. because
    its key was accidentally spelled like a reserved block).
    """
    reserved = {"etl", "sources"}
    declared = [k for k in raw_spec if k not in reserved]

    assert [t.input_prefix for t in messy.event_tables] == declared
    assert all(t.events for t in messy.event_tables)
