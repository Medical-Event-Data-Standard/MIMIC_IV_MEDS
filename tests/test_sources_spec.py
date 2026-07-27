"""Validates the ``sources:`` block shipped in ``configs/event_configs.yaml``.

These tests are offline: constructing sources and validating explicit-URL manifests
does no network I/O (PhysioNet manifests are fetched lazily, on first ``.files``
access), so they exercise exactly what must hold before any download starts.
"""

from omegaconf import OmegaConf

from MIMIC_IV_MEDS import EVENT_CFG


def _resolved_sources_spec() -> dict:
    """Mirror the download CLI: resolve interpolations on only the ``sources:`` subtree."""
    raw = OmegaConf.load(EVENT_CFG)
    return {"sources": OmegaConf.to_container(raw.sources, resolve=True)}


def test_sources_construct_without_credentials(monkeypatch):
    """Every bucket must construct with no credential env vars set.

    The ``,null`` interpolation defaults exist so that demo (and CI) runs never need
    PhysioNet credentials in the environment — the download CLI resolves every bucket's
    interpolations before ``key=`` selection (mmcdermott/MEDS_extract#151), so a bare
    ``${oc.env:...}`` in the ``dataset`` bucket would break ``key=demo`` runs too.
    """
    from MEDS_extract.download import HTTPSource, PhysioNetSource, sources_from_spec

    monkeypatch.delenv("DATASET_DOWNLOAD_USERNAME", raising=False)
    monkeypatch.delenv("DATASET_DOWNLOAD_PASSWORD", raising=False)

    spec = _resolved_sources_spec()
    for key in ("demo", "dataset"):
        sources = sources_from_spec(spec, key=key)
        try:
            assert [type(s) for s in sources] == [PhysioNetSource, HTTPSource], key
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
    spec = _resolved_sources_spec()
    (common_entry,) = spec["sources"]["common"]
    assert common_entry["type"] == "http"
    urls = common_entry["urls"]
    assert len(urls) == 10
    for entry in urls:
        assert set(entry) == {"url", "rel_path", "sha256"}, entry
        assert len(entry["sha256"]) == 64
        int(entry["sha256"], 16)  # well-formed hex
