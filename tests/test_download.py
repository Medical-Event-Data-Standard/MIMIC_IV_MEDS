import logging
import tempfile
from pathlib import Path

from omegaconf import DictConfig

from MIMIC_IV_MEDS.download import MockSession, download_data

logging.basicConfig(level=logging.INFO)


def test_skip_existing_download(caplog):
    """Tests that existing files are skipped during download."""
    caplog.set_level(logging.INFO)  # Ensure INFO messages are captured

    # Define a simple dataset config
    cfg = DictConfig(
        {
            "urls": {
                "demo": ["http://example.com/demo.csv"],
                "common": ["http://example.com/common.csv"],
            }
        }
    )

    # Configure the mock session to return simple content
    mock_contents = {
        "http://example.com/demo.csv": "demo data",
        "http://example.com/common.csv": "common data",
    }
    mock_session = MockSession(return_contents=mock_contents)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # --- First Run: Download the files ---
        print(f"\n--- Running first download to {tmpdir_path} ---")
        download_data(
            tmpdir_path, cfg, do_demo=True, session_factory=lambda: mock_session
        )

        # Check files exist
        demo_file = tmpdir_path / "demo.csv"
        common_file = tmpdir_path / "common.csv"
        assert demo_file.exists()
        assert demo_file.read_text() == "demo data"
        assert common_file.exists()
        assert common_file.read_text() == "common data"

        # Clear captured logs before the second run
        caplog.clear()

        # --- Second Run: Attempt download again ---
        print(f"\n--- Running second download to {tmpdir_path} (should skip) ---")
        download_data(
            tmpdir_path, cfg, do_demo=True, session_factory=lambda: mock_session
        )

        # --- Verification ---
        # Check that "Skipping download" messages are in the logs
        found_skip_demo = False
        found_skip_common = False
        for record in caplog.records:
            if f"Skipping download, file already exists: {demo_file}" in record.message:
                found_skip_demo = True
            if (
                f"Skipping download, file already exists: {common_file}"
                in record.message
            ):
                found_skip_common = True

        print("\nCaptured logs during second run:")
        print("--------------------------------")
        for record in caplog.records:
            print(f"{record.levelname}: {record.message}")
        print("--------------------------------")

        assert found_skip_demo, "Did not find log message for skipping demo.csv"
        assert found_skip_common, "Did not find log message for skipping common.csv"

        # Optional: Check that download messages are NOT present in the second run logs
        found_download_demo = any(f"Downloading http://example.com/demo.csv" in rec.message for rec in caplog.records)
        found_download_common = any(f"Downloading http://example.com/common.csv" in rec.message for rec in caplog.records)
        assert not found_download_demo, "Found unexpected download message for demo.csv in second run"
        assert not found_download_common, "Found unexpected download message for common.csv in second run"