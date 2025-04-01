import hashlib
import logging
import tempfile
from pathlib import Path

import pytest
from omegaconf import DictConfig

from MIMIC_IV_MEDS.download import MockResponse, MockSession, download_data

logging.basicConfig(level=logging.INFO)


def fake_checksum_content(file_map: dict) -> str:
    lines = []
    for rel_path, content in file_map.items():
        checksum = hashlib.sha256(content.encode()).hexdigest()
        lines.append(f"{checksum} {rel_path}")
    return "\n".join(lines)


@pytest.fixture
def dataset_config():
    return DictConfig(
        {"urls": {"demo": ["http://example.com/demo.csv"], "common": ["http://example.com/common.csv"]}}
    )


def test_skip_existing_download(caplog, dataset_config):
    """Tests that if a file already exists and its checksum matches the one from SHA256SUMS.txt, the download
    is skipped."""
    caplog.set_level(logging.INFO)
    # Setup fake file content for demo and common files.
    file_map = {
        "demo.csv": "demo data",
        "common.csv": "common data",
    }
    # Create the fake SHA256SUMS.txt content based on file_map.
    checksum_txt = fake_checksum_content(file_map)
    # Provide proper status codes for all URLs.
    return_status = {
        "http://example.com/demo.csv": 200,
        "http://example.com/common.csv": 200,
        "http://example.com/SHA256SUMS.txt": 200,
    }
    mock_contents = {
        "http://example.com/demo.csv": file_map["demo.csv"],
        "http://example.com/common.csv": file_map["common.csv"],
        "http://example.com/SHA256SUMS.txt": checksum_txt,
    }
    mock_session = MockSession(return_contents=mock_contents, return_status=return_status)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        # First download run.
        download_data(tmpdir_path, dataset_config, do_demo=True, session_factory=lambda: mock_session)
        demo_file = tmpdir_path / "demo.csv"
        common_file = tmpdir_path / "common.csv"
        assert demo_file.exists()
        assert demo_file.read_text() == file_map["demo.csv"]
        assert common_file.exists()
        assert common_file.read_text() == file_map["common.csv"]

        caplog.clear()
        # Second run: files exist; should skip downloading if checksum is valid.
        download_data(tmpdir_path, dataset_config, do_demo=True, session_factory=lambda: mock_session)

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
        # Also, ensure that no new "Downloading" message is logged.
        found_download_demo = any(
            "Downloading http://example.com/demo.csv" in rec.message for rec in caplog.records
        )
        found_download_common = any(
            "Downloading http://example.com/common.csv" in rec.message for rec in caplog.records
        )
        assert not found_download_demo, "Found unexpected download message for demo.csv in second run"
        assert not found_download_common, "Found unexpected download message for common.csv in second run"


def test_redownload_on_checksum_mismatch(caplog, dataset_config):
    """Tests that if a file exists but its checksum does not match the expected value, then the file is re-
    downloaded."""
    caplog.set_level(logging.INFO)
    # Correct content that should eventually appear.
    correct_content = "correct data"
    file_map = {"demo.csv": correct_content}
    checksum_txt = fake_checksum_content(file_map)
    # For the test, simulate an initial wrong content.
    wrong_content = "wrong data"
    return_status = {
        "http://example.com/demo.csv": 200,
        "http://example.com/SHA256SUMS.txt": 200,
    }
    # Prepare two sets of responses: first with wrong content, then with correct content.
    responses = [
        {"http://example.com/demo.csv": wrong_content, "http://example.com/SHA256SUMS.txt": checksum_txt},
        {"http://example.com/demo.csv": correct_content, "http://example.com/SHA256SUMS.txt": checksum_txt},
    ]

    class SwitchMockSession(MockSession):
        def get(self, url: str, stream: bool = False):
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
        download_data(tmpdir_path, dataset_config, do_demo=True, session_factory=lambda: mock_session)
        demo_file = tmpdir_path / "demo.csv"
        assert demo_file.exists()
        assert demo_file.read_text() == wrong_content

        caplog.clear()
        # Simulate that the source now returns the correct content.
        responses[0] = responses[1]
        download_data(tmpdir_path, dataset_config, do_demo=True, session_factory=lambda: mock_session)
        # Now the file should have been updated to the correct content.
        assert demo_file.read_text() == correct_content
        redownloaded = any("Checksum mismatch for" in rec.message for rec in caplog.records)
        assert redownloaded, "Expected a checksum mismatch message prompting redownload."
