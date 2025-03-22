import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory


def test_e2e():
    with TemporaryDirectory() as temp_dir:

        root = Path(temp_dir)

        do_overwrite = True
        do_copy = False
        do_demo = True
        do_download = True

        command_parts = [
            "MEDS_extract-MIMIC_IV",
            f"root_output_dir={str(root.resolve())}",
            f"do_download={do_download}",
            f"do_overwrite={do_overwrite}",
            f"do_copy={do_copy}",
            f"do_demo={do_demo}",
        ]

        full_cmd = " ".join(command_parts)
        command_out = subprocess.run(full_cmd, shell=True, capture_output=True)

        stderr = command_out.stderr.decode()
        stdout = command_out.stdout.decode()

        err_message = (
            f"Command failed with return code {command_out.returncode}.\n"
            f"Command stdout:\n{stdout}\n"
            f"Command stderr:\n{stderr}"
        )
        assert command_out.returncode == 0, err_message

        data_path = root / "MEDS_cohort" / "data"
        data_files = list(data_path.glob("*.parquet")) + list(data_path.glob("**/*.parquet"))

        all_files = [x for x in data_path.glob("**/*") if x.is_file()]

        assert len(data_files) > 0, f"No data files found in {data_path}; found {all_files}"

        metadata_path = root / "MEDS_cohort" / "metadata"
        all_files = [x for x in metadata_path.glob("**/*") if x.is_file()]

        dataset_metadata = metadata_path / "dataset.json"
        assert dataset_metadata.exists(), f"Dataset metadata not found in {metadata_path}; found {all_files}"

        codes_metadata = metadata_path / "codes.parquet"
        assert codes_metadata.exists(), f"Codes metadata not found in {metadata_path}; found {all_files}"

        subject_splits = metadata_path / "subject_splits.parquet"
        assert subject_splits.exists(), f"Subject splits not found in {metadata_path}; found {all_files}"
