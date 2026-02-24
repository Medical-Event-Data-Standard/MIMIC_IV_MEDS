import logging
import subprocess

logger = logging.getLogger(__name__)


def run_command(
    command_parts: list[str],
    runner_fn: callable = subprocess.run,
):
    """Runs a command with the specified runner function.

    Args:
        command_parts: A list of the arguments to be run as a shell command.
        runner_fn: The function to run the command with (added for dependency injection).

    Raises:
        ValueError: If the command fails

    Examples:
        >>> def fake_shell_succeed(cmd, shell, capture_output):
        ...     print(cmd)
        ...     return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")
        >>> def fake_shell_fail(cmd, shell, capture_output):
        ...     print(cmd)
        ...     return subprocess.CompletedProcess(args=cmd, returncode=1, stdout=b"", stderr=b"")
        >>> run_command(["echo", "hello"], runner_fn=fake_shell_succeed)
        echo hello
        >>> run_command(["echo", "hello"], runner_fn=fake_shell_fail)
        Traceback (most recent call last):
            ...
        ValueError: Command failed with return code 1.
    """
    full_cmd = " ".join(command_parts)
    logger.info(f"Running command: {full_cmd}")
    command_out = runner_fn(full_cmd, shell=True, capture_output=True)

    # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging

    stderr = command_out.stderr.decode()
    stdout = command_out.stdout.decode()
    logger.info(f"Command output:\n{stdout}")

    if command_out.returncode != 0:
        logger.error(f"Command failed with return code {command_out.returncode}.")
        logger.error(f"Command stderr:\n{stderr}")
        raise ValueError(f"Command failed with return code {command_out.returncode}.")
