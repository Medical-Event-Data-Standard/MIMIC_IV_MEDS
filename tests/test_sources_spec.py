"""Validates the ``sources:`` block shipped in ``configs/event_configs.yaml``.

These tests are offline: constructing sources and validating explicit-URL manifests does
no network I/O (PhysioNet manifests are fetched lazily, on first ``.files`` access), so
they exercise exactly what must hold before any download starts.

Everything goes through :meth:`MessyConfig.selected_sources`, the same accessor
``meds-extract-download`` uses — including its per-*selected*-bucket interpolation
resolution (mmcdermott/MEDS_extract#151), which is what lets demo and CI runs work with no
PhysioNet credentials in the environment.
"""

import pytest
from MEDS_extract.download import HTTPSource, PhysioNetSource
from omegaconf.errors import InterpolationResolutionError


@pytest.fixture
def sources(messy):
    """A ``key -> constructed sources`` callable that closes every source afterwards."""
    opened = []

    def _open(key: str):
        opened.extend(new := messy.selected_sources(key))
        return new

    yield _open
    for source in opened:
        source.close()


def test_demo_bucket_constructs_without_credentials(no_credentials, sources):
    """The demo (+ common) buckets must construct with no credential env vars set.

    The credential interpolations live on the ``dataset`` bucket, and the download layer
    resolves only the selected bucket — so demo and CI runs never need PhysioNet
    credentials, even though the same file declares them.
    """
    assert [type(s) for s in sources("demo")] == [PhysioNetSource, HTTPSource]


def test_dataset_bucket_requires_credentials(no_credentials, monkeypatch, sources):
    """The credentialed dataset bucket fails fast without credentials, and constructs with them.

    Failing up front with the missing variable named — rather than mid-download — is the intended UX for the
    credentialed path.
    """
    with pytest.raises(InterpolationResolutionError, match="DATASET_DOWNLOAD_USERNAME"):
        sources("dataset")

    monkeypatch.setenv("DATASET_DOWNLOAD_USERNAME", "unused-by-this-test")
    monkeypatch.setenv("DATASET_DOWNLOAD_PASSWORD", "unused-by-this-test")
    assert [type(s) for s in sources("dataset")] == [PhysioNetSource, HTTPSource]


@pytest.mark.parametrize(
    ("key", "expected_base_url"),
    [
        ("demo", "https://physionet.org/files/mimic-iv-demo/2.2/"),
        ("dataset", "https://physionet.org/files/mimiciv/3.1/"),
    ],
)
def test_release_version_reaches_the_download_url(key, expected_base_url, fake_credentials, sources):
    """``sources.dataset_version`` must land in the URL the downloader actually fetches.

    The declared version is the single source of truth for both the download URL and the
    ``etl_metadata.dataset_version`` stamp (pinned in ``test_spec_registration.py``); this
    is the URL half, per bucket, including that demo and full releases stay distinct.
    """
    physionet, *_ = sources(key)
    assert vars(physionet)["_base_url"] == expected_base_url


def test_common_bucket_is_fully_checksum_pinned(raw_spec):
    """Every concept-map URL must carry a pinned ``sha256``.

    GitHub raw publishes no checksum manifest, and the downloader refuses to skip an
    existing file it cannot verify — so an unpinned entry would make every resumed run fail
    with ``FileExistsError``. The ``v2.4.0`` tag is immutable, so pinned hashes are stable.
    Asserted against the raw file (not constructed sources) because it is a claim about
    what is *written* in the spec.
    """
    (common_entry,) = raw_spec.sources.common
    assert common_entry.type == "http"
    assert len(common_entry.urls) == 10
    for entry in common_entry.urls:
        assert set(entry) == {"url", "rel_path", "sha256"}, entry
        assert len(entry.sha256) == 64
        int(entry.sha256, 16)  # well-formed hex
