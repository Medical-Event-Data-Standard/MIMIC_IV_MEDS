import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def resolve_console_script(name: str) -> str:
    """Locate a console script that pip / uv installed alongside the running Python.

    `subprocess.run([name, ...])` does a `PATH` lookup. When the user runs the bundled
    `MEDS_extract-MIMIC_IV` directly via its venv path (without first activating the
    venv), the venv's `bin/` is not on `PATH`, and `subprocess` raises
    `FileNotFoundError` even though the script is installed in the same venv as the
    Python interpreter that's calling it. Looking next to `sys.executable` first
    sidesteps the PATH coupling — that's where pip / uv guarantee console scripts land.

    Args:
        name: The console script's basename (e.g. ``"MEDS_transform-pipeline"``).

    Returns:
        Absolute path to the executable.

    Raises:
        FileNotFoundError: If the script is neither next to `sys.executable` nor on PATH.

    Examples:
        Found next to `sys.executable` — works without an activated venv:

        >>> import sys
        >>> resolve_console_script(Path(sys.executable).name)  # the python itself
        ... # doctest: +ELLIPSIS
        '...'

        Falls back to PATH for tools installed elsewhere (e.g. via `apt`):

        >>> resolve_console_script("ls")  # doctest: +ELLIPSIS
        '/.../ls'

        Missing scripts raise with a useful message that names the lookup it tried:

        >>> resolve_console_script("definitely-not-a-real-script-xyz")
        Traceback (most recent call last):
            ...
        FileNotFoundError: 'definitely-not-a-real-script-xyz' not found next to ... or on PATH...
    """
    candidate = Path(sys.executable).parent / name
    if candidate.is_file():
        return str(candidate)
    found = shutil.which(name)
    if found:
        return found
    raise FileNotFoundError(
        f"{name!r} not found next to {sys.executable} or on PATH. "
        f"Reinstall the package that provides it in this environment."
    )


def run_command(
    command_parts: list[str],
    env: dict[str, str] | None = None,
    runner_fn: callable = subprocess.run,
):
    """Runs a command with the specified runner function.

    Args:
        command_parts: A list of the arguments to be run without shell interpretation.
        env: Optional dictionary of extra environment variables to set for the subprocess.
        runner_fn: The function to run the command with (added for dependency injection).

    Raises:
        ValueError: If the command fails

    Examples:
        >>> def fake_succeed(cmd, capture_output, env):
        ...     print(cmd)
        ...     return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")
        >>> def fake_fail(cmd, capture_output, env):
        ...     print(cmd)
        ...     return subprocess.CompletedProcess(args=cmd, returncode=1, stdout=b"", stderr=b"")
        >>> run_command(["echo", "hello"], runner_fn=fake_succeed)
        ['echo', 'hello']
        >>> run_command(["echo", "hello"], runner_fn=fake_fail)
        Traceback (most recent call last):
            ...
        ValueError: Command failed with return code 1.
    """
    logger.info(f"Running command: {command_parts}")
    run_env = {**os.environ, **(env or {})}
    command_out = runner_fn(command_parts, capture_output=True, env=run_env)

    # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging

    stderr = command_out.stderr.decode()
    stdout = command_out.stdout.decode()
    logger.info(f"Command output:\n{stdout}")

    if command_out.returncode != 0:
        logger.error(f"Command failed with return code {command_out.returncode}.")
        logger.error(f"Command stderr:\n{stderr}")
        raise ValueError(f"Command failed with return code {command_out.returncode}.")


def coerce_download_workers(raw: object, *, default: int = 1) -> int:
    """Coerce + validate a raw `download_workers` config value.

    The value comes from Hydra and may be `None` (`download_workers: null` in YAML
    behaves like an unset key, yielding `default`). `bool` is rejected explicitly
    because `int(True) == 1` would silently take the sequential path even though
    `download_workers: true` in YAML is almost certainly a config typo. Fractional
    floats (e.g. `1.9`) are also rejected rather than truncated by `int()` — same
    reasoning, silent truncation hides typos.

    Examples:
        >>> coerce_download_workers(None)
        1
        >>> coerce_download_workers(8)
        8
        >>> coerce_download_workers(2.0)
        2
        >>> coerce_download_workers(True)
        Traceback (most recent call last):
            ...
        ValueError: download_workers must be a positive int, got True (bool)
        >>> coerce_download_workers(1.9)
        Traceback (most recent call last):
            ...
        ValueError: download_workers must be a positive int, got 1.9 (float)
        >>> coerce_download_workers("many")
        Traceback (most recent call last):
            ...
        ValueError: download_workers must be a positive int, got 'many' (str)
        >>> coerce_download_workers(0)
        Traceback (most recent call last):
            ...
        ValueError: download_workers must be a positive int, got 0
    """
    if raw is None:
        return default
    if isinstance(raw, bool):
        raise ValueError(f"download_workers must be a positive int, got {raw!r} (bool)")
    if isinstance(raw, float):
        if not raw.is_integer():
            raise ValueError(f"download_workers must be a positive int, got {raw!r} ({type(raw).__name__})")
        value = int(raw)
    else:
        try:
            value = int(raw)
        except (TypeError, ValueError) as e:
            raise ValueError(
                f"download_workers must be a positive int, got {raw!r} ({type(raw).__name__})"
            ) from e
    if value < 1:
        raise ValueError(f"download_workers must be a positive int, got {value}")
    return value
