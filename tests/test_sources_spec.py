"""Validates the ``sources:`` block shipped in ``configs/event_configs.yaml``.

These tests are offline: constructing sources and validating explicit-URL manifests
does no network I/O (PhysioNet manifests are fetched lazily, on first ``.files``
access), so they exercise exactly what must hold before any download starts.

The download CLI resolves interpolations per *selected* bucket (plus ``common``), not
across the whole ``sources:`` subtree (mmcdermott/MEDS_extract#151, fixed upstream) —
the helpers here mirror that behavior.
"""

from omegaconf import OmegaConf
from omegaconf.errors import InterpolationResolutionError

from MIMIC_IV_MEDS import EVENT_CFG


def _bucket_spec(*keys: str) -> dict:
    """Resolve interpolations for only the selected buckets, mirroring the CLI."""
    raw = OmegaConf.load(EVENT_CFG)
    return {"sources": {k: OmegaConf.to_container(raw.sources[k], resolve=True) for k in keys}}


def test_demo_constructs_without_credentials(monkeypatch):
    """The demo (and common) buckets must construct with no credential env vars set — demo and CI runs never
    need PhysioNet credentials."""
    from MEDS_extract.download import HTTPSource, PhysioNetSource, sources_from_spec

    monkeypatch.delenv("DATASET_DOWNLOAD_USERNAME", raising=False)
    monkeypatch.delenv("DATASET_DOWNLOAD_PASSWORD", raising=False)

    sources = sources_from_spec(_bucket_spec("demo", "common"), key="demo")
    try:
        assert [type(s) for s in sources] == [PhysioNetSource, HTTPSource]
    finally:
        for s in sources:
            s.close()


def test_dataset_requires_credentials(monkeypatch):
    """The credentialed dataset bucket fails fast (clear missing-env-var error) without credentials, and
    constructs once they are set — the intended UX for both cases."""
    from MEDS_extract.download import HTTPSource, PhysioNetSource, sources_from_spec

    monkeypatch.delenv("DATASET_DOWNLOAD_USERNAME", raising=False)
    monkeypatch.delenv("DATASET_DOWNLOAD_PASSWORD", raising=False)

    try:
        _bucket_spec("dataset")
    except InterpolationResolutionError as e:
        assert "DATASET_DOWNLOAD_USERNAME" in str(e)
    else:
        raise AssertionError("resolving the dataset bucket without credentials should fail")

    monkeypatch.setenv("DATASET_DOWNLOAD_USERNAME", "someone")
    monkeypatch.setenv("DATASET_DOWNLOAD_PASSWORD", "hunter2")
    sources = sources_from_spec(_bucket_spec("dataset", "common"), key="dataset")
    try:
        assert [type(s) for s in sources] == [PhysioNetSource, HTTPSource]
    finally:
        for s in sources:
            s.close()


def test_common_bucket_is_fully_checksum_pinned():
    """Every concept-map URL must carry a pinned ``sha256``.

    GitHub raw publishes no checksum manifest, and the downloader refuses to skip an
    existing file it cannot verify — so an unpinned entry would make every resumed run
    (``do_download=True`` over an existing ``raw_input_dir``) fail with
    ``FileExistsError``. The ``v2.4.0`` tag is immutable, so pinned hashes are stable.
    """
    (common_entry,) = _bucket_spec("common")["sources"]["common"]
    assert common_entry["type"] == "http"
    urls = common_entry["urls"]
    assert len(urls) == 10
    for entry in urls:
        assert set(entry) == {"url", "rel_path", "sha256"}, entry
        assert len(entry["sha256"]) == 64
        int(entry["sha256"], 16)  # well-formed hex
