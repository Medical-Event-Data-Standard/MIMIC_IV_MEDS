import logging
import os
import shutil
import subprocess
import sys
import sysconfig

logger = logging.getLogger(__name__)


def activation_equivalent_path(path: str | None = None, *, scripts_dir: str | None = None) -> str:
    """Returns ``PATH`` with this environment's scripts directory prepended.

    Prepending the environment's scripts directory (``sysconfig.get_path("scripts")`` —
    the venv's ``bin/``, or ``Scripts\\`` on Windows) is exactly what ``source
    .../activate`` does to ``PATH``. Building that same view explicitly means
    subprocesses resolve console scripts identically whether or not the caller
    activated the environment, without inventing any resolution order beyond the
    standard, documented one.

    Args:
        path: The ``PATH`` value to prepend to. Defaults to ``os.environ["PATH"]``.
        scripts_dir: Override of the scripts directory (for tests). Defaults to
            ``sysconfig.get_path("scripts")``.

    Returns:
        ``scripts_dir`` followed by the entries of ``path``, minus any duplicate
        occurrence of ``scripts_dir`` and any empty entries (a leading/trailing
        separator would otherwise mean "current directory" on POSIX).

    Examples:
        >>> activation_equivalent_path("/usr/bin", scripts_dir="/my/venv/bin")
        '/my/venv/bin:/usr/bin'

        Idempotent — an already-activated ``PATH`` is not double-prepended:

        >>> activation_equivalent_path("/my/venv/bin:/usr/bin", scripts_dir="/my/venv/bin")
        '/my/venv/bin:/usr/bin'

        An empty ``PATH`` yields just the scripts directory:

        >>> activation_equivalent_path("", scripts_dir="/my/venv/bin")
        '/my/venv/bin'
    """
    if path is None:
        path = os.environ.get("PATH", "")
    if scripts_dir is None:
        scripts_dir = sysconfig.get_path("scripts")
    parts = [p for p in path.split(os.pathsep) if p and p != scripts_dir]
    return os.pathsep.join([scripts_dir, *parts])


def resolve_console_script(name: str, *, scripts_dir: str | None = None) -> str:
    """Resolves console script ``name`` exactly as an activated environment would.

    ``subprocess.run([name, ...])`` resolves via the ambient ``PATH``, which omits this
    environment's scripts directory unless the user activated the venv — so running the
    bundled ``MEDS_extract-MIMIC_IV`` by its venv path (cron jobs, Slurm scripts,
    Makefiles) previously died with ``FileNotFoundError`` on this subprocess even
    though the script was installed right beside the interpreter (#51). Resolving with
    ``shutil.which`` against the activation-equivalent ``PATH`` gives the same result
    activation would, with real executable semantics (exec-bit checks on POSIX,
    ``PATHEXT`` on Windows) rather than bespoke file probing.

    Args:
        name: The console script's basename (e.g. ``"MEDS_transform-pipeline"``).
        scripts_dir: Override of the scripts directory (for tests).

    Returns:
        Absolute path to the executable.

    Raises:
        FileNotFoundError: If the script is neither in the scripts directory nor on
            ``PATH``.

    Examples:
        Console scripts installed in this environment resolve without activation:

        >>> from pathlib import Path
        >>> import sysconfig
        >>> exe = resolve_console_script("MEDS_transform-pipeline")
        >>> Path(exe).parent == Path(sysconfig.get_path("scripts"))
        True

        Tools installed elsewhere still resolve through the ambient ``PATH``:

        >>> resolve_console_script("ls")  # doctest: +ELLIPSIS
        '/.../ls'

        Missing scripts raise with a message naming both lookup locations:

        >>> resolve_console_script("definitely-not-a-real-script-xyz")
        Traceback (most recent call last):
            ...
        FileNotFoundError: 'definitely-not-a-real-script-xyz' not found in this environment's scripts...
    """
    found = shutil.which(name, path=activation_equivalent_path(scripts_dir=scripts_dir))
    if found:
        return found
    raise FileNotFoundError(
        f"{name!r} not found in this environment's scripts directory "
        f"({scripts_dir or sysconfig.get_path('scripts')}) or on PATH. "
        f"Reinstall the package that provides it in this environment ({sys.executable})."
    )


def run_command(
    command_parts: list[str],
    env: dict[str, str] | None = None,
    runner_fn: callable = subprocess.run,
):
    """Runs a command with the specified runner function.

    The child's ``PATH`` is upgraded to the activation-equivalent view (see
    :func:`activation_equivalent_path`), so any console scripts the child spawns in
    turn resolve the same way ours do — activated or not.

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
    run_env["PATH"] = activation_equivalent_path(run_env.get("PATH", ""))
    command_out = runner_fn(command_parts, capture_output=True, env=run_env)

    # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging

    stderr = command_out.stderr.decode()
    stdout = command_out.stdout.decode()
    logger.info(f"Command output:\n{stdout}")

    if command_out.returncode != 0:
        logger.error(f"Command failed with return code {command_out.returncode}.")
        logger.error(f"Command stderr:\n{stderr}")
        raise ValueError(f"Command failed with return code {command_out.returncode}.")
