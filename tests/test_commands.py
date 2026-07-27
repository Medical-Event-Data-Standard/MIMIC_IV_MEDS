"""Tests for `MIMIC_IV_MEDS.commands`.

Coverage focuses on `resolve_console_script` and `activation_equivalent_path` — the
helpers that make subprocess console-script resolution behave exactly as if the
environment were activated, fixing the unactivated-venv invocation failure (#51)
without any bespoke resolution order. Doctests in `commands.py` cover the happy path
and the missing-script error; this file pins the cross-cutting behaviors (activation
precedence, PATH fallback, executable semantics, child-PATH propagation) that a future
refactor might silently break.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sysconfig
from pathlib import Path

import pytest

from MIMIC_IV_MEDS.commands import activation_equivalent_path, resolve_console_script, run_command


def _make_executable(path: Path) -> None:
    path.write_text("#!/usr/bin/env bash\nexit 0\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def test_scripts_dir_wins_over_path(tmp_path, monkeypatch):
    """The environment's scripts dir takes precedence over ambient PATH — exactly the precedence `source
    .../activate` would give, since activation prepends that dir."""
    scripts = tmp_path / "venv" / "bin"
    scripts.mkdir(parents=True)
    venv_script = scripts / "my-script"
    _make_executable(venv_script)

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    _make_executable(other_dir / "my-script")

    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script", scripts_dir=str(scripts))
    assert resolved == str(venv_script), f"expected {venv_script} (activation precedence), got {resolved}"


def test_falls_back_to_ambient_path(tmp_path, monkeypatch):
    """Tools not installed in this environment still resolve through the ambient PATH."""
    scripts = tmp_path / "venv" / "bin"
    scripts.mkdir(parents=True)  # NOTE: no my-script here

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_script = other_dir / "my-script"
    _make_executable(other_script)

    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script", scripts_dir=str(scripts))
    assert resolved == str(other_script), f"expected PATH fallback to {other_script}, got {resolved}"


def test_raises_when_not_found(tmp_path, monkeypatch):
    """Useful error when the script genuinely isn't installed — names both lookup locations so the user knows
    what to check."""
    scripts = tmp_path / "venv" / "bin"
    scripts.mkdir(parents=True)

    monkeypatch.setenv("PATH", "")

    with pytest.raises(FileNotFoundError, match=r"not found in this environment's scripts"):
        resolve_console_script("definitely-not-installed-xyz-script", scripts_dir=str(scripts))


def test_directory_named_like_script_is_skipped(tmp_path, monkeypatch):
    """A directory in the scripts dir sharing the script's name must not satisfy the lookup — `shutil.which`
    applies real executable semantics."""
    scripts = tmp_path / "venv" / "bin"
    scripts.mkdir(parents=True)
    (scripts / "my-script").mkdir()  # a directory, not an executable

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_script = other_dir / "my-script"
    _make_executable(other_script)

    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script", scripts_dir=str(scripts))
    assert resolved == str(other_script)


def test_non_executable_file_is_skipped(tmp_path, monkeypatch):
    """A plain (non-executable) file in the scripts dir must not shadow a real executable on PATH —
    `shutil.which` checks the exec bit, unlike a bare `is_file()` probe."""
    scripts = tmp_path / "venv" / "bin"
    scripts.mkdir(parents=True)
    (scripts / "my-script").write_text("not executable")

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_script = other_dir / "my-script"
    _make_executable(other_script)

    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script", scripts_dir=str(scripts))
    assert resolved == str(other_script)


def test_resolves_real_console_script_without_activation_precedence_leaks():
    """Self-check: the package's own MEDS_transform-pipeline (installed alongside us)
    must resolve into this environment's scripts directory. If this fails, the test
    environment is broken — and so is real-world usage."""
    resolved = resolve_console_script("MEDS_transform-pipeline")
    assert Path(resolved).is_file(), f"resolved to {resolved} which doesn't exist"
    assert Path(resolved).parent == Path(sysconfig.get_path("scripts")), (
        f"resolved {resolved} but this environment's scripts dir is "
        f"{sysconfig.get_path('scripts')} — activation-equivalent precedence didn't fire"
    )


def test_run_command_child_path_is_activation_equivalent(monkeypatch):
    """The child process must see the activation-equivalent PATH so that console scripts *it* spawns resolve
    the same way ours do."""
    seen = {}

    def capture_runner(cmd, capture_output, env):
        seen["env"] = env
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout=b"", stderr=b"")

    monkeypatch.setenv("PATH", "/usr/bin")
    run_command(["echo", "hello"], runner_fn=capture_runner)

    child_path = seen["env"]["PATH"].split(os.pathsep)
    assert child_path[0] == sysconfig.get_path("scripts")
    assert "/usr/bin" in child_path


def test_activation_equivalent_path_never_emits_empty_entries():
    """Empty PATH entries mean "current directory" on POSIX; the helper must never introduce one, whatever the
    input shape."""
    for raw in ("", ":", "/usr/bin:", ":/usr/bin", "/usr/bin::/bin"):
        result = activation_equivalent_path(raw, scripts_dir="/my/venv/bin")
        assert "" not in result.split(os.pathsep), f"empty entry leaked from input {raw!r}: {result}"
