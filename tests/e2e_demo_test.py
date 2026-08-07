"""End-to-end run of the shipped ETL over the public MIMIC-IV demo release.

This distribution ships no Python — it is `configs/event_configs.yaml` plus the entry-point
registration that names it `MIMIC-IV`. So there is exactly one thing worth testing: that
`meds-extract-run spec=MIMIC-IV` resolves that registration, downloads the demo bucket from
PhysioNet (no credentials needed), runs the pipeline, and leaves a well-formed MEDS cohort
behind. Driving it through the registered name rather than a path is deliberate: it makes
this test cover the registration too.

Whether the *contents* of that cohort have changed is a separate question, answered by the
summary-stats regression check. That check runs against the cohort *this* test builds
rather than building its own: the extraction downloads from PhysioNet and takes minutes, so
CI does it exactly once, on one Python version, and both checks read the result. Setting
`MEDS_DEMO_OUTPUT_DIR` is how CI asks for the cohort to be left behind; unset (the local
default) it goes to a temporary directory and is cleaned up.
"""

import json
import os
import subprocess
import sys
import sysconfig
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory

SPEC = "MIMIC-IV"

#: The demo release the spec pins, as it appears in `dataset_version`'s `<release>:<pkg>` stamp.
DEMO_RELEASE = "2.2"

#: Set by CI to a path that outlives the test, so the summary-stats step can read the cohort.
OUTPUT_DIR_ENV_VAR = "MEDS_DEMO_OUTPUT_DIR"


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

    preset = os.environ.get(OUTPUT_DIR_ENV_VAR)
    # `nullcontext` rather than a temp dir when CI has named the destination: the cohort must
    # survive the test for the summary-stats step to read it.
    with TemporaryDirectory() if preset is None else nullcontext(preset) as temp_dir:
        out = Path(temp_dir) / "MEDS_cohort"
        # No overwrite flag: CI retries this test on transient PhysioNet errors, and a retry
        # re-entering a partly-filled directory is exactly the resumable case the runner
        # already handles — the downloader skips files it can verify and the pipeline skips
        # stages whose outputs exist.
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
