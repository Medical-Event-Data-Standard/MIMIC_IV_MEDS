"""Shared fixtures for addressing the one file this package ships.

There is no Python in ``MIMIC_IV_MEDS`` to import a path constant from — deliberately, see
that package's docstring — so the tests address the shipped MESSY config exactly the way
``meds-extract-run`` does: by its registered ``MEDS_extract.pipelines`` name.
"""

from importlib.resources import files

import pytest
from MEDS_extract.config import MessyConfig
from omegaconf import DictConfig, OmegaConf

#: The registered ``MEDS_extract.pipelines`` name — the production spelling of the spec.
SPEC = "MIMIC-IV"

#: Filesystem path to the same file, for assertions about the raw document.
SPEC_FP = files("MIMIC_IV_MEDS.configs").joinpath("event_configs.yaml")

#: Credential interpolations the ``dataset`` sources bucket reads from the environment.
CREDENTIAL_ENV_VARS = ("DATASET_DOWNLOAD_USERNAME", "DATASET_DOWNLOAD_PASSWORD")


@pytest.fixture
def spec_name() -> str:
    """The registered name, as a fixture (test modules are not importable as a package)."""
    return SPEC


@pytest.fixture
def spec_fp():
    """The shipped MESSY config's filesystem path, for the path-resolution route."""
    return SPEC_FP


@pytest.fixture
def messy() -> MessyConfig:
    """The shipped spec, loaded through the registered-name resolution ladder.

    Resolving ``"MIMIC-IV"`` (rather than a path) is what makes this fixture exercise the
    entry-point registration, and it validates every section of the document on load.
    """
    return MessyConfig.load(SPEC)


@pytest.fixture
def raw_spec() -> DictConfig:
    """The shipped MESSY config as raw, *unresolved* OmegaConf.

    Unresolved so that reading it never demands the credential env vars its ``sources:``
    block interpolates. Prefer the ``messy`` fixture whenever a test cares about resolved
    or parsed behavior rather than the literal document.
    """
    return OmegaConf.load(SPEC_FP)


@pytest.fixture
def no_credentials(monkeypatch):
    """An environment with no PhysioNet credentials set."""
    for var in CREDENTIAL_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def fake_credentials(monkeypatch):
    """Syntactically-present PhysioNet credentials, never actually used to authenticate."""
    for var in CREDENTIAL_ENV_VARS:
        monkeypatch.setenv(var, "unused-by-this-test")
