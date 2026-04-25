"""Tests for `MIMIC_IV_MEDS.commands`.

Coverage focuses on `resolve_console_script` — the helper added to fix the case where
the bundled `MEDS_extract-MIMIC_IV` is run from a venv that hasn't been activated, and
the subprocess can't find `MEDS_transform-pipeline` because PATH doesn't include the
venv's `bin/`. Doctests in `commands.py` cover the happy path and the missing-script
error message; this file pins the cross-cutting behaviors (PATH-vs-sys.executable
resolution order, env-var inheritance) that a future refactor might silently break.
"""

from __future__ import annotations

import os
import shutil
import stat
import sys
from pathlib import Path

import pytest

from MIMIC_IV_MEDS.commands import resolve_console_script


def _make_executable(path: Path) -> None:
    path.write_text("#!/usr/bin/env bash\nexit 0\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def test_resolve_console_script_prefers_sys_executable_dir(tmp_path, monkeypatch):
    """When the script exists next to sys.executable, that path wins over PATH — this is the whole point of
    the helper, since PATH might not include the venv."""
    fake_venv_bin = tmp_path / "venv" / "bin"
    fake_venv_bin.mkdir(parents=True)
    fake_python = fake_venv_bin / "python"
    _make_executable(fake_python)
    fake_script = fake_venv_bin / "my-script"
    _make_executable(fake_script)

    # And put a *different* my-script earlier on PATH — if the helper ever reverts to
    # a PATH-first lookup, this test catches it.
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_script = other_dir / "my-script"
    _make_executable(other_script)

    monkeypatch.setattr(sys, "executable", str(fake_python))
    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script")
    assert resolved == str(fake_script), f"expected {fake_script} (next to sys.executable), got {resolved}"


def test_resolve_console_script_falls_back_to_path(tmp_path, monkeypatch):
    """When no sibling script exists, fall back to PATH lookup."""
    fake_venv_bin = tmp_path / "venv" / "bin"
    fake_venv_bin.mkdir(parents=True)
    fake_python = fake_venv_bin / "python"
    _make_executable(fake_python)
    # Note: NO my-script next to fake_python.

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_script = other_dir / "my-script"
    _make_executable(other_script)

    monkeypatch.setattr(sys, "executable", str(fake_python))
    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script")
    assert resolved == str(other_script), f"expected PATH fallback to {other_script}, got {resolved}"


def test_resolve_console_script_raises_when_not_found(tmp_path, monkeypatch):
    """Useful error when the script genuinely isn't installed — surfaces both lookup locations so the user
    knows what to check."""
    fake_venv_bin = tmp_path / "venv" / "bin"
    fake_venv_bin.mkdir(parents=True)
    fake_python = fake_venv_bin / "python"
    _make_executable(fake_python)

    monkeypatch.setattr(sys, "executable", str(fake_python))
    monkeypatch.setenv("PATH", "")  # nothing on PATH

    with pytest.raises(FileNotFoundError, match=r"not found next to .* or on PATH"):
        resolve_console_script("definitely-not-installed-xyz-script")


def test_resolve_console_script_actually_finds_real_console_script():
    """Self-check: the package's own MEDS_transform-pipeline (installed alongside us
    via the test extras) must resolve via the sys.executable path. If this fails, the
    test environment is broken — and so is real-world usage."""
    resolved = resolve_console_script("MEDS_transform-pipeline")
    assert Path(resolved).is_file(), f"resolved to {resolved} which doesn't exist"
    # It must be the one in OUR venv (next to sys.executable), not some random PATH hit.
    expected_dir = Path(sys.executable).parent
    assert Path(resolved).parent == expected_dir, (
        f"resolved {resolved} but sys.executable dir is {expected_dir} — "
        f"the sys.executable-first lookup didn't fire"
    )


def test_resolve_console_script_skips_directories_named_like_scripts(tmp_path, monkeypatch):
    """A directory next to sys.executable that happens to share the script name should not satisfy the lookup
    — `is_file()` rules it out and we fall through to PATH."""
    fake_venv_bin = tmp_path / "venv" / "bin"
    fake_venv_bin.mkdir(parents=True)
    fake_python = fake_venv_bin / "python"
    _make_executable(fake_python)
    # A *directory* with the script's name next to python — NOT a real executable.
    (fake_venv_bin / "my-script").mkdir()

    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_script = other_dir / "my-script"
    _make_executable(other_script)

    monkeypatch.setattr(sys, "executable", str(fake_python))
    monkeypatch.setenv("PATH", f"{other_dir}{os.pathsep}/usr/bin")

    resolved = resolve_console_script("my-script")
    assert resolved == str(other_script), (
        f"expected fallback to {other_script} (the dir-named-my-script next to "
        f"sys.executable should not have matched), got {resolved}"
    )


# Suppress "unused" warnings for the imports we keep for visibility above.
_ = shutil
