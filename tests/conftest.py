"""The demo cohort, built once per test session for every test that reads it.

`meds-extract-run spec=MIMIC-IV dataset_key=demo` downloads the public MIMIC-IV demo from PhysioNet (no
credentials needed) and takes minutes, so it runs exactly once: `e2e_demo_test.py` checks the cohort it
leaves behind, and `run_with_enrichment_test.py` runs the ETL again, offline, from the raw files this
build staged. This build skips the spec's `sumeds_enrich` post-stage, so it is the plain ETL output, the
same with or without network access. Setting `MEDS_DEMO_OUTPUT_DIR` is how CI asks for the cohort to be
left behind for the summary-stats check; unset (the local default) it goes to a temporary directory and is
cleaned up.
"""

import os
import subprocess
import sys
import sysconfig
from contextlib import nullcontext
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

SPEC = "MIMIC-IV"

#: Set by CI to a path that outlives the test, so the summary-stats step can read the cohort.
OUTPUT_DIR_ENV_VAR = "MEDS_DEMO_OUTPUT_DIR"

#: Set by CI to a cached directory for the raw demo download, passed to the runner as
#: `download_dest_dir`. The downloader verifies every file against PhysioNet's `SHA256SUMS.txt`
#: and skips the ones that match, so a restored cache turns the download into a manifest fetch
#: while a stale or partial one is repaired rather than trusted. Unset, the raw data lands
#: inside the output tree as before.
DOWNLOAD_DIR_ENV_VAR = "MEDS_DEMO_DOWNLOAD_DIR"


def run_script(name: str, *args: str) -> None:
    """Run the console script `name` installed in this environment, failing with its output on error.

    Everything resolves against `sysconfig`'s script dir — the one `activate` puts on PATH — rather than the
    ambient PATH, so the tests behave the same under `uv run pytest` (which activates) and a bare `pytest`
    (which does not). The dir is also *prepended* to the child's PATH, because the runner spawns each stage as
    `MEDS_transform-stage` by bare name and would otherwise not find it.
    """
    scripts = Path(sysconfig.get_path("scripts"))
    script = scripts / name
    assert script.exists(), f"{name} is not installed in this environment ({sys.executable})."
    env = os.environ | {"PATH": f"{scripts}{os.pathsep}{os.environ.get('PATH', '')}"}
    command = [str(script), *args]

    # Captured rather than streamed so a failure's message carries the child's stderr: CI matches
    # network-shaped exception names in it to decide whether a failure is a transient PhysioNet blip
    # worth retrying (see .github/workflows/tests.yaml).
    result = subprocess.run(command, capture_output=True, check=False, env=env)
    assert result.returncode == 0, (
        f"{' '.join(command)} failed with return code {result.returncode}.\n"
        f"stdout:\n{result.stdout.decode()}\nstderr:\n{result.stderr.decode()}"
    )


@pytest.fixture(scope="session")
def demo_cohort():
    """The MEDS cohort `meds-extract-run spec=MIMIC-IV` builds from the downloaded demo.

    Driving it through the registered name rather than a path is deliberate: it makes the tests cover the
    registration too.
    """
    preset = os.environ.get(OUTPUT_DIR_ENV_VAR)
    # `nullcontext` rather than a temp dir when CI has named the destination: the cohort must
    # survive the test for the summary-stats step to read it.
    with TemporaryDirectory() if preset is None else nullcontext(preset) as temp_dir:
        out = Path(temp_dir) / "MEDS_cohort"
        # No overwrite flag: CI retries this test on transient PhysioNet errors, and a retry
        # re-entering a partly-filled directory is exactly the resumable case the runner
        # already handles — the downloader skips files it can verify and the pipeline skips
        # stages whose outputs exist.
        args = [
            f"spec={SPEC}",
            f"output_dir={out.resolve()!s}",
            "dataset_key=demo",
            "skip_stages=[sumeds_enrich]",
        ]
        if download_dir := os.environ.get(DOWNLOAD_DIR_ENV_VAR):
            args.append(f"download_dest_dir={Path(download_dir).resolve()!s}")
        run_script("meds-extract-run", *args)
        yield out


@pytest.fixture(scope="session")
def demo_raw_dir(demo_cohort: Path) -> Path:
    """The raw demo files the build downloaded: the cached download dir if CI set one, else where the
    runner stages them by default."""
    download_dir = os.environ.get(DOWNLOAD_DIR_ENV_VAR)
    raw = Path(download_dir).resolve() if download_dir else demo_cohort / ".meds_extract_run" / "raw_input"
    assert (raw / "hosp").is_dir(), f"no raw demo files staged under {raw}"
    return raw
