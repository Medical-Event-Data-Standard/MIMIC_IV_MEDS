import logging
import os
import subprocess

logger = logging.getLogger(__name__)


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
