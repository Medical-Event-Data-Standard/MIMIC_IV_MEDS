"""End-to-end run of the shipped ETL over the public MIMIC-IV demo release.

This distribution ships no Python — it is `configs/event_configs.yaml` plus the entry-point
registration that names it `MIMIC-IV`. So there is exactly one thing worth testing: that
`meds-extract-run spec=MIMIC-IV` resolves that registration, downloads the demo bucket from
PhysioNet (no credentials needed), runs the pipeline, and leaves a well-formed MEDS cohort
behind. Driving it through the registered name rather than a path is deliberate: it makes
this test cover the registration too.

Whether the *contents* of that cohort have changed is a separate question, answered by the
summary-stats regression check in `.github/workflows/demo-regression.yaml`.
"""

import json
import os
import subprocess
import sys
import sysconfig
from pathlib import Path
from tempfile import TemporaryDirectory

SPEC = "MIMIC-IV"

#: The demo release the spec pins, as it appears in `dataset_version`'s `<release>:<pkg>` stamp.
DEMO_RELEASE = "2.2"


def test_e2e_demo():
    # Everything resolves against `sysconfig`'s script dir — the one `activate` puts on
    # PATH — rather than the ambient PATH, so the test behaves the same under `uv run
    # pytest` (which activates) and a bare `pytest` (which does not). The dir is also
    # *prepended* to the child's PATH, because the runner spawns each stage as
    # `MEDS_transform-stage` by bare name and would otherwise not find it.
    scripts = Path(sysconfig.get_path("scripts"))
    runner = scripts / "meds-extract-run"
    assert runner.exists(), f"meds-extract-run is not installed in this environment ({sys.executable})."
    env = os.environ | {"PATH": f"{scripts}{os.pathsep}{os.environ.get('PATH', '')}"}

    with TemporaryDirectory() as temp_dir:
        out = Path(temp_dir) / "MEDS_cohort"
        command = [str(runner), f"spec={SPEC}", f"output_dir={out.resolve()!s}", "dataset_key=demo"]

        # Captured rather than streamed so a failure's message carries the child's stderr:
        # CI matches network-shaped exception names in it to decide whether a failure is a
        # transient PhysioNet blip worth retrying (see .github/workflows/tests.yaml).
        result = subprocess.run(command, capture_output=True, check=False, env=env)
        assert result.returncode == 0, (
            f"{' '.join(command)} failed with return code {result.returncode}.\n"
            f"stdout:\n{result.stdout.decode()}\nstderr:\n{result.stderr.decode()}"
        )

        data_files = list((out / "data").glob("**/*.parquet"))
        assert data_files, f"No data shards written; {out / 'data'} holds {list((out / 'data').rglob('*'))}"

        metadata = out / "metadata"
        for fname in ("dataset.json", "codes.parquet", "subject_splits.parquet"):
            assert (metadata / fname).exists(), f"{fname} missing from {metadata}"

        # The identity the runner derives from the registration and the `sources:` block.
        dataset_json = json.loads((metadata / "dataset.json").read_text())
        assert dataset_json["dataset_name"] == SPEC
        assert dataset_json["dataset_version"].startswith(f"{DEMO_RELEASE}:")
