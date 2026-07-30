from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

import pytest

from easybackup import delta
from easybackup.archive import find_tool
from easybackup.delta import (
    apply_delta_patch,
    compress_patch,
    create_delta_patch,
    decompress_patch,
    file_integrity,
    find_xdelta3,
    verify_file,
)
from easybackup.errors import (
    CancelledError,
    StorageError,
    ToolMissingError,
    ValidationError,
)


class _FinishedProcess:
    def wait(self, timeout=None):
        del timeout
        return 0

    def poll(self):
        return 0


class _HangingProcess:
    def __init__(self):
        self.terminated = False
        self.killed = False

    def wait(self, timeout=None):
        del timeout
        if self.terminated or self.killed:
            return -15
        raise subprocess.TimeoutExpired("xdelta3", 0.1)

    def poll(self):
        return -15 if self.terminated or self.killed else None

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


def _tool(tmp_path: Path, name: str = "xdelta3.exe") -> Path:
    executable = tmp_path / name
    executable.write_bytes(b"placeholder")
    return executable


def test_find_xdelta3_uses_explicit_path_and_common_finder(
    tmp_path, monkeypatch
):
    explicit = _tool(tmp_path)
    assert find_xdelta3(explicit) == str(explicit.resolve())

    monkeypatch.setenv("EASYBACKUP_XDELTA3_PATH", str(explicit))
    assert find_xdelta3() == str(explicit.resolve())
    monkeypatch.delenv("EASYBACKUP_XDELTA3_PATH", raising=False)
    monkeypatch.delenv("EASYBACKUP_XDELTA3", raising=False)
    monkeypatch.setattr(delta, "find_tool", lambda name: f"/conda/{name}")
    assert find_xdelta3() == "/conda/xdelta3"


def test_create_patch_passes_paths_as_literal_arguments(
    tmp_path, monkeypatch
):
    executable = _tool(tmp_path)
    base = tmp_path / "base ; untouched.bin"
    current = tmp_path / "new $(literal).bin"
    patch = tmp_path / "patch & literal.delta"
    base.write_bytes(b"old")
    current.write_bytes(b"new-version")
    calls = []

    def fake_popen(arguments, **kwargs):
        calls.append((arguments, kwargs))
        Path(arguments[-1]).write_bytes(b"binary-patch")
        return _FinishedProcess()

    monkeypatch.setattr(delta.subprocess, "Popen", fake_popen)
    integrity = create_delta_patch(
        base, current, patch, executable=executable
    )

    assert patch.read_bytes() == b"binary-patch"
    assert integrity.size == len(b"binary-patch")
    assert integrity.sha256 == hashlib.sha256(b"binary-patch").hexdigest()
    arguments, options = calls[0]
    assert isinstance(arguments, list)
    assert arguments[:5] == [
        str(executable.resolve()),
        "-f",
        "-e",
        "-s",
        str(base.resolve()),
    ]
    assert arguments[5] == str(current.resolve())
    assert Path(arguments[6]).parent == tmp_path
    assert options["shell"] is False
    assert options["stdin"] is subprocess.DEVNULL


def test_apply_patch_validates_reconstructed_file_before_replace(
    tmp_path, monkeypatch
):
    executable = _tool(tmp_path)
    base = tmp_path / "base.bin"
    patch = tmp_path / "change.delta"
    output = tmp_path / "restored.bin"
    base.write_bytes(b"base")
    patch.write_bytes(b"patch")
    output.write_bytes(b"keep-existing")

    def fake_popen(arguments, **kwargs):
        del kwargs
        Path(arguments[-1]).write_bytes(b"reconstructed")
        return _FinishedProcess()

    monkeypatch.setattr(delta.subprocess, "Popen", fake_popen)
    with pytest.raises(ValidationError, match="SHA-256"):
        apply_delta_patch(
            base,
            patch,
            output,
            expected_size=len(b"reconstructed"),
            expected_sha256="0" * 64,
            executable=executable,
        )
    assert output.read_bytes() == b"keep-existing"
    assert not list(tmp_path.glob(f".{output.name}.*.partial"))


def test_apply_patch_returns_verified_integrity(tmp_path, monkeypatch):
    executable = _tool(tmp_path)
    base = tmp_path / "base.bin"
    patch = tmp_path / "change.delta"
    output = tmp_path / "restored.bin"
    payload = b"expected reconstructed payload"
    base.write_bytes(b"base")
    patch.write_bytes(b"patch")

    def fake_popen(arguments, **kwargs):
        del kwargs
        Path(arguments[-1]).write_bytes(payload)
        return _FinishedProcess()

    monkeypatch.setattr(delta.subprocess, "Popen", fake_popen)
    integrity = apply_delta_patch(
        base,
        patch,
        output,
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest().upper(),
        executable=executable,
    )
    assert output.read_bytes() == payload
    assert integrity == file_integrity(output)


def test_running_process_is_terminated_on_cancellation(
    tmp_path, monkeypatch
):
    executable = _tool(tmp_path)
    base = tmp_path / "base.bin"
    current = tmp_path / "current.bin"
    patch = tmp_path / "change.delta"
    base.write_bytes(b"base")
    current.write_bytes(b"current")
    process = _HangingProcess()
    monkeypatch.setattr(
        delta.subprocess, "Popen", lambda *args, **kwargs: process
    )
    checks = 0

    def cancelled():
        nonlocal checks
        checks += 1
        return checks >= 3

    with pytest.raises(CancelledError, match="已取消"):
        create_delta_patch(
            base,
            current,
            patch,
            executable=executable,
            cancelled=cancelled,
        )
    assert process.terminated
    assert not patch.exists()


def test_running_process_is_terminated_on_timeout(
    tmp_path, monkeypatch
):
    executable = _tool(tmp_path)
    base = tmp_path / "base.bin"
    current = tmp_path / "current.bin"
    patch = tmp_path / "change.delta"
    base.write_bytes(b"base")
    current.write_bytes(b"current")
    process = _HangingProcess()
    monkeypatch.setattr(
        delta.subprocess, "Popen", lambda *args, **kwargs: process
    )
    moments = iter((10.0, 12.0))
    monkeypatch.setattr(delta.time, "monotonic", lambda: next(moments))

    with pytest.raises(StorageError, match="超时"):
        create_delta_patch(
            base,
            current,
            patch,
            executable=executable,
            timeout_seconds=1,
        )
    assert process.terminated
    assert not patch.exists()


def test_running_process_is_terminated_when_output_exceeds_limit(
    tmp_path,
    monkeypatch,
):
    executable = _tool(tmp_path)
    base = tmp_path / "base.bin"
    current = tmp_path / "current.bin"
    patch = tmp_path / "change.delta"
    base.write_bytes(b"base")
    current.write_bytes(b"current")
    patch.write_bytes(b"keep-existing")
    process = _HangingProcess()

    def fake_popen(arguments, **kwargs):
        del kwargs
        Path(arguments[-1]).write_bytes(b"x" * 32)
        return process

    monkeypatch.setattr(delta.subprocess, "Popen", fake_popen)
    with pytest.raises(StorageError, match="输出超过允许上限"):
        create_delta_patch(
            base,
            current,
            patch,
            executable=executable,
            max_output_size=8,
        )
    assert process.terminated
    assert patch.read_bytes() == b"keep-existing"


def test_missing_tool_is_reported_without_touching_output(
    tmp_path, monkeypatch
):
    base = tmp_path / "base.bin"
    current = tmp_path / "current.bin"
    patch = tmp_path / "change.delta"
    base.write_bytes(b"base")
    current.write_bytes(b"current")
    patch.write_bytes(b"existing")
    monkeypatch.delenv("EASYBACKUP_XDELTA3_PATH", raising=False)
    monkeypatch.delenv("EASYBACKUP_XDELTA3", raising=False)
    monkeypatch.setattr(delta, "find_tool", lambda name: None)

    with pytest.raises(ToolMissingError, match="xdelta3"):
        create_delta_patch(base, current, patch)
    assert patch.read_bytes() == b"existing"


def test_file_integrity_and_verification_are_reusable(tmp_path):
    value = tmp_path / "value.bin"
    value.write_bytes(b"integrity")
    expected = hashlib.sha256(b"integrity").hexdigest()

    assert verify_file(
        value,
        expected_size=9,
        expected_sha256=expected,
    ).sha256 == expected
    with pytest.raises(ValidationError, match="大小校验"):
        verify_file(value, expected_size=10)


def test_zstd_patch_helpers_use_atomic_outputs(tmp_path, monkeypatch):
    executable = _tool(tmp_path, "zstd.exe")
    patch = tmp_path / "raw.patch"
    compressed = tmp_path / "raw.patch.zst"
    restored = tmp_path / "restored.patch"
    patch.write_bytes(b"patch-payload")
    calls = []

    def fake_popen(arguments, **kwargs):
        del kwargs
        calls.append(arguments)
        source = Path(arguments[-3])
        output = Path(arguments[-1])
        output.write_bytes(source.read_bytes())
        return _FinishedProcess()

    monkeypatch.setattr(delta.subprocess, "Popen", fake_popen)
    compressed_integrity = compress_patch(
        patch, compressed, executable=executable
    )
    restored_integrity = decompress_patch(
        compressed,
        restored,
        expected_size=len(b"patch-payload"),
        expected_sha256=hashlib.sha256(b"patch-payload").hexdigest(),
        executable=executable,
    )

    assert compressed_integrity.size == len(b"patch-payload")
    assert restored_integrity == file_integrity(patch)
    assert restored.read_bytes() == patch.read_bytes()
    assert calls[0][1:4] == ["-T0", "-3", "-q"]
    assert calls[1][1:4] == ["-d", "-q", "-f"]


@pytest.mark.skipif(
    not find_xdelta3(),
    reason="xdelta3 is not installed",
)
def test_real_xdelta3_base_relative_round_trip(tmp_path):
    base = tmp_path / "weekly-base.bin"
    current = tmp_path / "thursday.bin"
    patch = tmp_path / "thursday.delta"
    restored = tmp_path / "restored.bin"
    base.write_bytes((b"A" * 8192) + (b"B" * 8192))
    payload = (b"A" * 8192) + b"changed" + (b"B" * 8185)
    current.write_bytes(payload)

    create_delta_patch(base, current, patch)
    apply_delta_patch(
        base,
        patch,
        restored,
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
    )
    assert restored.read_bytes() == payload


@pytest.mark.skipif(
    not find_tool("zstd"),
    reason="zstd is not installed",
)
def test_real_zstd_patch_round_trip(tmp_path):
    patch = tmp_path / "change.delta"
    compressed = tmp_path / "change.delta.zst"
    restored = tmp_path / "restored.delta"
    payload = b"delta-content" * 1024
    patch.write_bytes(payload)

    compress_patch(patch, compressed, level=5)
    decompress_patch(
        compressed,
        restored,
        expected_size=len(payload),
        expected_sha256=hashlib.sha256(payload).hexdigest(),
    )
    assert restored.read_bytes() == payload
