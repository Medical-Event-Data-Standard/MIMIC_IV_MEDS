"""End-to-end run of the shipped ETL over the public MIMIC-IV demo release.

This is the only test that touches the network: it drives the real ``meds-extract-run``
console script, which downloads the demo bucket from PhysioNet (no credentials needed) and
runs the canonical eight-stage pipeline, then checks the result is a well-formed MEDS
cohort labelled with the right dataset identity.
"""

import json
import subprocess
import sys
import sysconfig
from pathlib import Path
from tempfile import TemporaryDirectory


def _console_script(name: str) -> str:
    """Locate ``name`` in the environment running the tests.

    Resolved against ``sysconfig.get_path("scripts")`` — the directory ``activate`` puts
    on ``PATH`` — rather than trusted to the ambient ``PATH``, so the suite behaves the
    same under ``uv run pytest`` (which activates) and a bare ``pytest`` (which does not).
    """
    exe = Path(sysconfig.get_path("scripts")) / name
    assert exe.exists(), f"{name} is not installed in this environment ({sys.executable})."
    return str(exe)


def _run_etl(output_dir: Path, spec: str) -> None:
    """Run the whole ETL into ``output_dir``, failing with the child's output on error."""
    command = [
        _console_script("meds-extract-run"),
        f"spec={spec}",
        f"output_dir={output_dir.resolve()!s}",
        "download_key=demo",
        "do_overwrite=True",
    ]
    # Output is captured rather than streamed so that a failure's message carries the
    # child's stderr: CI matches network-shaped exception names in it to decide whether a
    # failure is a transient PhysioNet blip worth retrying (see .github/workflows/tests.yaml).
    out = subprocess.run(command, capture_output=True, check=False)
    assert out.returncode == 0, (
        f"{' '.join(command)} failed with return code {out.returncode}.\n"
        f"stdout:\n{out.stdout.decode()}\nstderr:\n{out.stderr.decode()}"
    )


def _validate_meds_dataset(dataset_path: Path, dataset_name: str) -> None:
    """Assert ``dataset_path`` holds a MEDS cohort with our dataset identity stamped on it."""
    data_path = dataset_path / "data"
    metadata_path = dataset_path / "metadata"

    data_files = list(data_path.glob("**/*.parquet"))
    all_data = [x for x in data_path.glob("**/*") if x.is_file()]
    assert len(data_files) > 0, f"No data files found in {data_path}; found {all_data}"

    all_meta_files = [x for x in metadata_path.glob("**/*") if x.is_file()]
    for fname in ("dataset.json", "codes.parquet", "subject_splits.parquet"):
        fpath = metadata_path / fname
        assert fpath.exists(), f"{fname} not found in {metadata_path}; found {all_meta_files}"

    # The identity the runner derives from the registration and the `sources:` block —
    # `2.2` because this is the demo release, not the `3.1` full one.
    dataset_metadata = json.loads((metadata_path / "dataset.json").read_text())
    assert dataset_metadata["dataset_name"] == dataset_name
    assert dataset_metadata["dataset_version"].startswith("2.2:")


def test_e2e_demo(spec_name):
    with TemporaryDirectory() as temp_dir:
        output_dir = Path(temp_dir) / "MEDS_cohort"
        _run_etl(output_dir, spec_name)
        _validate_meds_dataset(output_dir, spec_name)
