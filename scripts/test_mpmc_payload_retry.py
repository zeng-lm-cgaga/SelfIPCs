#!/usr/bin/env python3
"""Hold the native consumer to prove a retry keeps one message payload intact."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time

from pressure_process import terminate_process_group


def children_of(pid: int) -> list[int]:
    try:
        return [int(value) for value in Path(f"/proc/{pid}/task/{pid}/children").read_text().split()]
    except (OSError, ValueError):
        return []


def cmdline(pid: int) -> list[str]:
    try:
        return [value for value in Path(f"/proc/{pid}/cmdline").read_bytes().decode().split("\0") if value]
    except (OSError, UnicodeDecodeError):
        return []


def has_role(pid: int, role: str) -> bool:
    args = cmdline(pid)
    try:
        return args[args.index("--role") + 1] == role
    except (ValueError, IndexError):
        return False


def stop_consumer(pid: int, retry_ready: Path, timeout_s: float) -> tuple[int, int]:
    deadline = time.monotonic() + timeout_s
    stopped_pid = None
    while time.monotonic() < deadline:
        for child in children_of(pid):
            if has_role(child, "consumer"):
                try:
                    state = Path(f"/proc/{child}/status").read_text()
                    if not any(line.startswith("State:") and "T" in line.split()[1]
                               for line in state.splitlines()):
                        continue
                    stopped_pid = child
                    if not retry_ready.is_file():
                        continue
                    marker = retry_ready.read_text()
                    if not marker.startswith("RETRY_READY ") or "sequence=1\n" not in marker:
                        continue
                    # The payload exists and QueueFull has actually occurred before this hold.
                    started_ns = time.monotonic_ns()
                    time.sleep(0.15)
                    os.kill(child, signal.SIGCONT)
                    return child, time.monotonic_ns() - started_ns
                except (ProcessLookupError, FileNotFoundError):
                    continue
        time.sleep(0.001)
    if stopped_pid is not None:
        try:
            os.kill(stopped_pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
    raise RuntimeError("stopped consumer and native QueueFull rendezvous were not observed")


def parse_result(text: str) -> dict[str, str]:
    for line in text.splitlines():
        if not line.startswith("RESULT "):
            continue
        fields: dict[str, str] = {}
        for item in line[len("RESULT "):].split():
            if "=" in item:
                key, value = item.split("=", 1)
                fields[key] = value
        return fields
    return {}


def run_case(binary: Path, wait_policy: str, root: Path) -> dict[str, object]:
    case_dir = root / wait_policy
    case_dir.mkdir()
    csv_dir = case_dir / "latency"
    csv_dir.mkdir()
    retry_ready = case_dir / "retry.ready"
    argv = [
        str(binary), "--payload-size", "64", "--producers", "1", "--consumers", "1",
        "--messages", "2", "--capacity", "1", "--runs", "1", "--timeout-ms", "10000",
        "--test-consumer-stop", "--test-retry-ready-file", str(retry_ready),
        "--retry-policy", "yield", "--wait-policy",
        wait_policy, "--out-dir", str(csv_dir),
    ]
    env = os.environ.copy()
    for key in ("EDGE_FAILPOINT", "EDGE_FAILPOINT_MODE", "EDGE_FAILPOINT_COUNT",
                "ASAN_OPTIONS", "UBSAN_OPTIONS", "LD_PRELOAD"):
        env.pop(key, None)
    env["LC_ALL"] = "C"
    stdout_path = case_dir / "stdout.raw"
    stderr_path = case_dir / "stderr.raw"
    process = None
    stopped_pid = -1
    held_ns = 0
    try:
        with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open(
                "w", encoding="utf-8") as stderr:
            process = subprocess.Popen(
                argv, cwd=root, env=env, stdout=stdout, stderr=stderr,
                start_new_session=True, close_fds=True, text=True)
            stopped_pid, held_ns = stop_consumer(process.pid, retry_ready, 5.0)
            try:
                process.wait(timeout=20.0)
            except subprocess.TimeoutExpired:
                terminate_process_group(process, 1.0)
                process.wait(timeout=2.0)
    finally:
        if process is not None:
            terminate_process_group(process, 1.0)
            if process.poll() is None:
                process.wait(timeout=2.0)
    output = stdout_path.read_text(encoding="utf-8")
    result = parse_result(output)
    csv_files = sorted(csv_dir.glob("*.csv"))
    if process.returncode != 0 or not result or len(csv_files) != 1:
        raise RuntimeError(f"{wait_policy}: exit={process.returncode}, result={result}")
    with csv_files[0].open(newline="", encoding="utf-8") as csv_file:
        rows = list(csv.DictReader(csv_file))
    sequence_one = [row for row in rows if row.get("producer") == "0" and row.get("sequence") == "1"]
    if (result.get("correctness") != "PASS" or result.get("queue_full", "0") == "0" or
            len(rows) != 2 or len(sequence_one) != 1 or
            int(sequence_one[0]["latency_ns"]) < 100_000_000):
        raise RuntimeError(f"{wait_policy}: retry evidence failed result={result} rows={rows}")
    return {
        "wait_policy": wait_policy,
        "argv": argv,
        "parent_pid": process.pid,
        "consumer_pid": stopped_pid,
        "held_ms": held_ns / 1_000_000,
        "returncode": process.returncode,
        "result": result,
        "rows": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    args = parser.parse_args()
    binary = args.binary.resolve()
    output_root = args.output_root.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"not an executable: {binary}")
    if not output_root.is_dir() or str(output_root) == "/":
        parser.error("--output-root must be an existing non-root directory")
    root = Path(tempfile.mkdtemp(prefix="edgeruntime-mpmc-payload-retry-", dir=output_root))
    rows = [run_case(binary, policy, root) for policy in ("busy", "blocking")]
    report = {"root": str(root), "cases": rows}
    (root / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
