import logging
import subprocess

from omegaconf import DictConfig

logger = logging.getLogger(__name__)


def run_command(
    command_parts: list[str],
    cfg: DictConfig | None = None,
    runner_fn: callable = subprocess.run,
):
    """Runs a command with the specified runner function.

    Args:
        command_parts: A list of the arguments to be run as a shell command.
        cfg: The configuration for the command.
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
        >>> run_command(["hello"], cfg={"do_overwrite": True}, runner_fn=fake_shell_succeed)
        hello ++do_overwrite=True
        >>> run_command(["hello"], cfg={"do_overwrite": False}, runner_fn=fake_shell_succeed)
        hello ++do_overwrite=False
        >>> run_command(["hello"], cfg={"do_copy": True}, runner_fn=fake_shell_succeed)
        hello ++do_copy=True
        >>> run_command(["hello"], cfg={"do_copy": False}, runner_fn=fake_shell_succeed)
        hello ++do_copy=False
        >>> run_command(["hello"], cfg={"do_profile": True}, runner_fn=fake_shell_succeed)
        hello ++hydra.callbacks.profiler._target_=hydra_profiler.profiler.ProfilerCallback
        >>> run_command(["hello"], cfg={"do_profile": False}, runner_fn=fake_shell_succeed)
        hello
        >>> run_command(["hello"], cfg={"seed": 42}, runner_fn=fake_shell_succeed)
        hello ++seed=42
    """

    if cfg is None:
        do_overwrite = None
        do_copy = None
        do_profile = False
        seed = None
    else:
        do_overwrite = cfg.get("do_overwrite", None)
        do_copy = cfg.get("do_copy", None)
        do_profile = cfg.get("do_profile", False)
        seed = cfg.get("seed", None)

    if do_overwrite is not None:
        command_parts.append(f"++do_overwrite={do_overwrite}")
    if do_copy is not None:
        command_parts.append(f"++do_copy={do_copy}")
    if seed is not None:
        command_parts.append(f"++seed={seed}")
    if do_profile:
        command_parts.append("++hydra.callbacks.profiler._target_=hydra_profiler.profiler.ProfilerCallback")

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
