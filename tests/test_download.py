import hashlib
import logging
import tempfile
from pathlib import Path

import pytest
from omegaconf import DictConfig

from MIMIC_IV_MEDS import download as download_module
from MIMIC_IV_MEDS.download import MockResponse, MockSession, download_data, make_session_with_retries


@pytest.fixture(autouse=True)
def _clear_checksum_cache():
    """Clear the module-level checksum cache between tests to prevent state leakage."""
    download_module._checksum_cache.clear()
    yield
    download_module._checksum_cache.clear()


# Use URLs with enough path segments (>= 4) to trigger checksum validation,
# mirroring PhysioNet-style URL structure.
DEMO_URL = "http://example.com/files/dataset/v1/demo.csv"
COMMON_URL = "http://example.com/files/dataset/v1/common.csv"
CHECKSUM_URL = "http://example.com/files/dataset/v1/SHA256SUMS.txt"


def fake_checksum_content(file_map: dict) -> str:
    lines = []
    for rel_path, content in file_map.items():
        checksum = hashlib.sha256(content.encode()).hexdigest()
        lines.append(f"{checksum} {rel_path}")
    return "\n".join(lines)


@pytest.fixture
def dataset_config():
    return DictConfig({"urls": {"demo": [DEMO_URL], "common": [COMMON_URL]}})


@pytest.fixture
def demo_only_config():
    """Config with only a demo URL (no common), for focused tests."""
    return DictConfig({"urls": {"demo": [DEMO_URL]}})


class TrackingMockSession(MockSession):
    """MockSession that tracks GET call counts per URL."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.get_counts: dict[str, int] = {}

    def get(self, url: str, stream: bool = False, **kwargs):
        self.get_counts[url] = self.get_counts.get(url, 0) + 1
        return super().get(url, stream=stream, **kwargs)


def test_skip_existing_download(caplog, dataset_config):
    """Tests that if a file already exists and its checksum matches the one from SHA256SUMS.txt, the download
    is skipped."""
    caplog.set_level(logging.INFO)
    file_map = {
        "demo.csv": "demo data",
        "common.csv": "common data",
    }
    checksum_txt = fake_checksum_content(file_map)
    return_status = {
        DEMO_URL: 200,
        COMMON_URL: 200,
        CHECKSUM_URL: 200,
    }
    mock_contents = {
        DEMO_URL: file_map["demo.csv"],
        COMMON_URL: file_map["common.csv"],
        CHECKSUM_URL: checksum_txt,
    }
    mock_session = TrackingMockSession(return_contents=mock_contents, return_status=return_status)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        # First download run.
        download_data(
            tmpdir_path,
            dataset_config,
            do_demo=True,
            session_factory=lambda: mock_session,
        )
        demo_file = tmpdir_path / "demo.csv"
        common_file = tmpdir_path / "common.csv"
        assert demo_file.exists()
        assert demo_file.read_text() == file_map["demo.csv"]
        assert common_file.exists()
        assert common_file.read_text() == file_map["common.csv"]

        # Record GET counts after first run.
        demo_gets_after_first = mock_session.get_counts.get(DEMO_URL, 0)
        common_gets_after_first = mock_session.get_counts.get(COMMON_URL, 0)

        caplog.clear()
        # Second run: files exist; should skip downloading if checksum is valid.
        download_data(
            tmpdir_path,
            dataset_config,
            do_demo=True,
            session_factory=lambda: mock_session,
        )

        # Verify that logs mention skipping the download based on valid checksum.
        found_skip_demo = any(
            f"Skipping download, file already exists and valid checksum: {demo_file}" in rec.message
            for rec in caplog.records
        )
        found_skip_common = any(
            f"Skipping download, file already exists and valid checksum: {common_file}" in rec.message
            for rec in caplog.records
        )
        assert found_skip_demo, "Did not find log message for skipping demo.csv"
        assert found_skip_common, "Did not find log message for skipping common.csv"

        # Verify no additional GET requests were made to file URLs (only SHA256SUMS.txt may be re-fetched).
        assert mock_session.get_counts.get(DEMO_URL, 0) == demo_gets_after_first, (
            "demo.csv was re-downloaded despite matching checksum"
        )
        assert mock_session.get_counts.get(COMMON_URL, 0) == common_gets_after_first, (
            "common.csv was re-downloaded despite matching checksum"
        )


def test_redownload_on_checksum_mismatch(caplog, demo_only_config):
    """Tests that if a file exists but its checksum does not match the expected value, then the file is re-
    downloaded."""
    caplog.set_level(logging.INFO)
    correct_content = "correct data"
    file_map = {"demo.csv": correct_content}
    checksum_txt = fake_checksum_content(file_map)
    wrong_content = "wrong data"
    return_status = {
        DEMO_URL: 200,
        CHECKSUM_URL: 200,
    }
    # Prepare two sets of responses: first with wrong content, then with correct content.
    responses = [
        {DEMO_URL: wrong_content, CHECKSUM_URL: checksum_txt},
        {DEMO_URL: correct_content, CHECKSUM_URL: checksum_txt},
    ]

    class SwitchMockSession(TrackingMockSession):
        def get(self, url: str, stream: bool = False, **kwargs):
            self.get_counts[url] = self.get_counts.get(url, 0) + 1
            current = responses[0]
            if url in current:
                contents = current[url]
            else:
                contents = ""
            status = return_status.get(url, 200)
            return MockResponse(status_code=status, contents=contents)

    mock_session = SwitchMockSession(return_contents=responses[0], return_status=return_status)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        # First run: wrong content is downloaded.
        download_data(
            tmpdir_path,
            demo_only_config,
            do_demo=True,
            session_factory=lambda: mock_session,
        )
        demo_file = tmpdir_path / "demo.csv"
        assert demo_file.exists()
        assert demo_file.read_text() == wrong_content

        caplog.clear()
        # Simulate that the source now returns the correct content.
        responses[0] = responses[1]
        download_data(
            tmpdir_path,
            demo_only_config,
            do_demo=True,
            session_factory=lambda: mock_session,
        )
        # Now the file should have been updated to the correct content.
        assert demo_file.read_text() == correct_content
        redownloaded = any("Checksum mismatch for" in rec.message for rec in caplog.records)
        assert redownloaded, "Expected a checksum mismatch message prompting redownload."


def test_parallel_download_produces_same_output_as_sequential():
    """End-to-end check that download_workers > 1 lands the same files with the same contents as the single-
    worker path, including for an authenticated (dict-with-credentials) URL block where each worker session
    must inherit auth + headers from the enumerating session.

    The auth/UA assertion deliberately skips index 0 of the created-sessions list — that's the
    enumerating (master) session, which `download_data` configures with auth + headers itself
    before calling `crawl_and_download`. Including it would mean the test passes even if the
    workers never inherited anything, defeating the point of the check.
    """
    import threading

    file_map = {
        "a.csv": "alpha contents",
        "b.csv": "bravo contents",
        "c.csv": "charlie contents",
        "d.csv": "delta contents",
        "e.csv": "echo contents",
    }
    base = "http://example.com/files/dataset/v1/"
    listing_html = "".join(f"<a href='{base}{name}'>{name}</a>" for name in file_map)
    return_contents = {base: listing_html, **{f"{base}{n}": c for n, c in file_map.items()}}
    return_status = dict.fromkeys(return_contents, 200)

    # Track every session the factory creates, in creation order. The first entry is the
    # enumerating (master) session; subsequent entries are the worker sessions.
    created_sessions: list[MockSession] = []
    lock = threading.Lock()

    class CountingMockSession(MockSession):
        def __init__(self):
            super().__init__(return_contents=return_contents, return_status=return_status)

    def factory():
        s = CountingMockSession()
        with lock:
            created_sessions.append(s)
        return s

    cfg = DictConfig({"urls": {"dataset": [{"url": base, "username": "u", "password": "p"}]}})

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        download_data(
            tmpdir_path,
            cfg,
            do_demo=False,
            session_factory=factory,
            download_workers=4,
        )
        for name, content in file_map.items():
            written = (tmpdir_path / name).read_text()
            assert written == content, f"{name}: expected {content!r}, got {written!r}"

    # Master + workers. With download_workers=4 the queue is pre-filled with 4 worker sessions,
    # so we expect exactly 5 (1 master + 4 workers).
    assert len(created_sessions) == 5, f"expected 1 master + 4 worker sessions, got {len(created_sessions)}"
    # Workers (everything after the master at index 0) must each have inherited the auth tuple
    # and User-Agent header from the master session. Asserting per-worker rather than via a
    # set-membership check catches the case where one worker is correctly configured but
    # others aren't — e.g. a thread-local-set-once-then-reused implementation that races.
    for i, worker in enumerate(created_sessions[1:], start=1):
        assert worker.auth == ("u", "p"), f"worker session #{i} missing auth, got {worker.auth!r}"
        assert worker.headers.get("User-Agent") == "Wget/1.21.1 (linux-gnu)", (
            f"worker session #{i} missing User-Agent, got {worker.headers!r}"
        )


def test_parallel_download_aggregates_failures():
    """When workers > 1, a single failed file should not silently skip the others, and the raised error should
    reference the count of failures plus the first failing URL."""
    base = "http://example.com/files/dataset/v1/"
    good = {base + "good1.csv": "ok1", base + "good2.csv": "ok2"}
    bad_url = base + "bad.csv"
    listing_html = (
        f"<a href='{base}good1.csv'>g1</a><a href='{base}good2.csv'>g2</a><a href='{bad_url}'>bad</a>"
    )
    contents = {base: listing_html, **good, bad_url: "ignored"}
    statuses = {base: 200, base + "good1.csv": 200, base + "good2.csv": 200, bad_url: 503}

    cfg = DictConfig({"urls": {"dataset": [base]}})

    with tempfile.TemporaryDirectory() as tmpdir:
        with pytest.raises(ValueError, match=r"Failed to download data from"):
            download_data(
                Path(tmpdir),
                cfg,
                do_demo=False,
                session_factory=lambda: MockSession(return_contents=contents, return_status=statuses),
                download_workers=3,
            )
        # Successful files are still on disk despite the bad one — that's the whole point of
        # gathering errors after the pool drains rather than short-circuiting.
        assert (Path(tmpdir) / "good1.csv").exists()
        assert (Path(tmpdir) / "good2.csv").exists()


def test_parallel_requires_session_factory():
    """`crawl_and_download(max_workers > 1, session_factory=None)` must raise — silently degrading to
    sequential would mask config bugs (user requests parallelism, sees no speedup, blames PhysioNet)."""
    from MIMIC_IV_MEDS.download import crawl_and_download

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        pytest.raises(ValueError, match=r"session_factory must be provided"),
    ):
        crawl_and_download(
            "http://example.com/",
            Path(tmpdir),
            MockSession(),
            max_workers=4,
            session_factory=None,
        )


@pytest.mark.parametrize(
    "bad_value, match",
    [
        (None, r"must be a positive int"),
        (0, r"must be >= 1"),
        (-3, r"must be >= 1"),
        ("eight", r"must be a positive int"),
        (True, r"must be a positive int"),  # bool is an int subclass; explicitly reject
    ],
)
def test_download_data_rejects_bad_download_workers(bad_value, match, demo_only_config):
    """`download_workers` must be a real positive int.

    Typos / nulls / negatives should fail loudly here instead of silently taking the sequential path inside
    crawl_and_download.
    """
    with (
        tempfile.TemporaryDirectory() as tmpdir,
        pytest.raises(ValueError, match=match),
    ):
        download_data(
            Path(tmpdir),
            demo_only_config,
            do_demo=True,
            session_factory=lambda: MockSession(),
            download_workers=bad_value,
        )


def test_session_factory_failure_during_pool_setup_closes_already_created_sessions():
    """If `session_factory()` raises partway through pre-creating worker sessions, every session created
    before the failure should be closed before `crawl_and_download` re-raises — otherwise we leak the
    underlying sockets (and adapter connection pools)."""
    import threading

    from MIMIC_IV_MEDS.download import crawl_and_download

    closed: list[int] = []
    lock = threading.Lock()
    creation_count = {"n": 0}

    class TrackingMockSession(MockSession):
        def __init__(self, sid: int):
            super().__init__()
            self.sid = sid

        def close(self):
            with lock:
                closed.append(self.sid)

    def factory():
        with lock:
            creation_count["n"] += 1
            n = creation_count["n"]
        # Pre-creation loop calls factory() max_workers times; fail on the 4th call so
        # the first three sessions are orphaned in the no-cleanup version.
        if n == 4:
            raise RuntimeError("simulated factory failure")
        return TrackingMockSession(n)

    with (
        tempfile.TemporaryDirectory() as tmpdir,
        pytest.raises(RuntimeError, match="simulated factory failure"),
    ):
        crawl_and_download(
            "http://example.com/",
            Path(tmpdir),
            MockSession(),  # enumerating session, not from factory
            max_workers=8,
            session_factory=factory,
        )

    # The first three sessions (sids 1, 2, 3) must have been closed before the raise.
    assert sorted(closed) == [1, 2, 3], (
        f"expected sessions 1,2,3 to be closed on factory-failure cleanup, got {closed}"
    )


def test_make_session_with_retries_contract():
    """Regression guard on the retry adapter config — catches silent changes to the retry policy."""
    session = make_session_with_retries()
    for prefix in ("http://", "https://"):
        adapter = session.get_adapter(prefix + "example.com/")
        retry = adapter.max_retries
        assert retry.total == 5, f"total retries for {prefix}"
        assert retry.backoff_factor == 2.0, f"backoff_factor for {prefix}"
        assert set(retry.status_forcelist) == {429, 500, 502, 503, 504}, f"status_forcelist for {prefix}"
        assert "GET" in retry.allowed_methods, f"GET in allowed_methods for {prefix}"
        assert retry.respect_retry_after_header is True, f"respect_retry_after_header for {prefix}"
        assert retry.raise_on_status is False, f"raise_on_status for {prefix}"
