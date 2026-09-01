"""Thin subprocess helpers. Every external command (docker, kind, kubectl) goes through here."""
from __future__ import annotations

import shutil
import subprocess
from dataclasses import dataclass


@dataclass
class Result:
    code: int
    out: str
    err: str

    @property
    def ok(self) -> bool:
        return self.code == 0

    def check(self) -> "Result":
        if not self.ok:
            raise RuntimeError(f"command failed ({self.code}): {self.err.strip() or self.out.strip()}")
        return self


def have(binary: str) -> bool:
    return shutil.which(binary) is not None


def run(args: list[str], *, input_text: str | None = None, timeout: int | None = 600,
        cwd: str | None = None) -> Result:
    """Run a command to completion, capturing stdout/stderr."""
    try:
        p = subprocess.run(
            args, input=input_text, capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        return Result(p.returncode, p.stdout, p.stderr)
    except subprocess.TimeoutExpired as e:
        return Result(124, e.stdout or "", f"timed out after {timeout}s")
    except FileNotFoundError:
        return Result(127, "", f"command not found: {args[0]}")


def stream(args: list[str], *, cwd: str | None = None):
    """Run a command, yielding output lines as they arrive (for long builds). Yields ('line'|'exit', value)."""
    proc = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, cwd=cwd,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        yield ("line", line.rstrip("\n"))
    proc.wait()
    yield ("exit", proc.returncode)
