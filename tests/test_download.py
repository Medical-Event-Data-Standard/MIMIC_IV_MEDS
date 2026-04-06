import hashlib
import logging
import tempfile
from pathlib import Path

import pytest
from omegaconf import DictConfig

from MIMIC_IV_MEDS.download import MockResponse, MockSession, download_data

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

    def get(self, url: str, stream: bool = False):
        self.get_counts[url] = self.get_counts.get(url, 0) + 1
        return super().get(url, stream=stream)


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
        assert (
            mock_session.get_counts.get(DEMO_URL, 0) == demo_gets_after_first
        ), "demo.csv was re-downloaded despite matching checksum"
        assert (
            mock_session.get_counts.get(COMMON_URL, 0) == common_gets_after_first
        ), "common.csv was re-downloaded despite matching checksum"


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
        def get(self, url: str, stream: bool = False):
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
