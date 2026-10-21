#!/usr/bin/env python3
"""Bounded timeout cleanup checks for real benchmark IPC and a small fixture."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import signal
from multiprocessing import shared_memory
import subprocess
import sys
import tempfile
import time

from pressure_process import run_logged_process, _session_groups


def clean_env() -> dict[str, str]:
    env = os.environ.copy()
    for key in ("EDGE_FAILPOINT", "EDGE_FAILPOINT_MODE", "EDGE_FAILPOINT_COUNT",
                "ASAN_OPTIONS", "UBSAN_OPTIONS", "LD_PRELOAD"):
        env.pop(key, None)
    env["LC_ALL"] = "C"
    return env


def pids_from_ready(path: Path) -> tuple[list[int], int | None]:
    pids: list[int] = []
    worker_pgid: int | None = None
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return pids, worker_pgid
    for line in text.splitlines():
        for match in re.finditer(r"(?:pid|worker_parent|worker_pgid)=(\d+)", line):
            value = int(match.group(1))
            if "worker_pgid=" in line and match.group(0).startswith("worker_pgid"):
                worker_pgid = value
            else:
                pids.append(value)
    return pids, worker_pgid


def process_exists(pid: int) -> bool:
    return Path(f"/proc/{pid}").exists()


def group_exists(pgid: int | None) -> bool:
    if pgid is None or pgid <= 0:
        return False
    try:
        os.kill(-pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def parse_cleanup(stdout: str) -> dict[str, str]:
    for line in stdout.splitlines():
        if not line.startswith("CLEANUP "):
            continue
        result: dict[str, str] = {}
        for field in line[len("CLEANUP "):].split():
            if "=" in field:
                key, value = field.split("=", 1)
                result[key] = value
        return result
    return {}


def run_timeout_case(binary: Path, kind: str, root: Path, *, owner: bool,
                     ignore_term: bool) -> dict[str, object]:
    label = f"{kind}-{'owner' if owner else 'native'}-{'kill' if ignore_term else 'term'}"
    case_dir = root / label
    case_dir.mkdir()
    ready = case_dir / "ready.txt"
    sentinel_name = f"edgeruntime_watchdog_sentinel_{os.getpid()}_{time.time_ns()}"
    sentinel = shared_memory.SharedMemory(name=sentinel_name, create=True, size=64)
    sentinel_bytes = bytes((index * 3 + 7) & 0xFF for index in range(64))
    sentinel.buf[:] = sentinel_bytes
    if owner:
        argv = [str(binary), "--kind", kind, "--ready-file", str(ready)]
        if ignore_term:
            argv.append("--ignore-term-worker")
    elif kind == "mpmc":
        argv = [
            str(binary), "--payload-size", "64", "--producers", "1", "--consumers", "1",
            "--messages", "1000000", "--capacity", "1", "--runs", "1",
            "--timeout-ms", "120000", "--retry-policy", "yield", "--wait-policy", "blocking",
            "--test-consumer-hold-ms", "10000", "--ready-file", str(ready),
        ]
        if ignore_term:
            argv.append("--test-ignore-term-consumer")
    else:
        argv = [
            str(binary), "--block-size", "4096", "--block-count", "2", "--producers", "1",
            "--consumers", "1", "--messages", "1000000", "--capacity", "1", "--runs", "1",
            "--timeout-ms", "120000", "--test-consumer-hold-ms", "10000",
            "--ready-file", str(ready),
        ]
        if ignore_term:
            argv.append("--test-ignore-term-consumer")

    try:
        result = run_logged_process(
            argv,
            cwd=root,
            env=clean_env(),
            stdout_path=case_dir / "stdout.raw",
            stderr_path=case_dir / "stderr.raw",
            sample_path=case_dir / "process-samples.jsonl",
            timeout_s=0.4,
            term_grace_s=3.0,
            ready_path=ready,
            ready_roles={"READY"},
            startup_timeout_s=5.0,
        )
        stdout = (case_dir / "stdout.raw").read_text(encoding="utf-8")
        stderr = (case_dir / "stderr.raw").read_text(encoding="utf-8")
        cleanup = parse_cleanup(stdout)
        ready_pids, worker_pgid = pids_from_ready(ready)
        lingering = [pid for pid in sorted(set(result.observed_child_pids + ready_pids))
                     if process_exists(pid)]
        sentinel_ok = bytes(sentinel.buf[:]) == sentinel_bytes
        record = {
            "label": label,
            "argv": argv,
            "cwd": str(root),
            "returncode": result.returncode,
            "timed_out": result.timed_out,
            "term_sent": result.term_sent,
            "kill_sent": result.kill_sent,
            "orphan_group_reaped": result.orphan_group_reaped,
            "observed_pids": result.observed_child_pids,
            "ready_pids": ready_pids,
            "worker_pgid": worker_pgid,
            "worker_group_alive": group_exists(worker_pgid),
            "lingering_pids": lingering,
            "cleanup": cleanup,
            "sentinel_unchanged": sentinel_ok,
            "stderr": stderr,
            "end_epoch_ns": result.end_epoch_ns,
        }
        (case_dir / "result.json").write_text(json.dumps(record, indent=2) + "\n",
                                               encoding="utf-8")
        reopen_values = [value for key, value in cleanup.items() if key.startswith("reopen")]
        cleanup_ok = bool(reopen_values) and all(value == "NotFound" for value in reopen_values)
        if owner:
            cleanup_ok = cleanup_ok and cleanup.get("parent_exit") == "true"
        if (not result.timed_out or result.orphan_group_reaped or lingering or
                group_exists(worker_pgid) or not sentinel_ok or not cleanup_ok):
            print(json.dumps(record, sort_keys=True), file=sys.stderr)
            raise RuntimeError(f"watchdog cleanup failed for {label}")
        return record
    finally:
        sentinel.close()
        sentinel.unlink()


def run_fixture(root: Path) -> dict[str, object]:
    case_dir = root / "python-fixture"
    case_dir.mkdir()
    sentinel = case_dir / "external-sentinel"
    sentinel.write_text("must-survive\n", encoding="utf-8")
    nested = case_dir / "nested.json"
    fixture = f"""import os, signal, time, json
from pathlib import Path
child = os.fork()
if child == 0:
    os.setpgid(0, 0)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    grandchild = os.fork()
    if grandchild != 0:
        Path({str(nested)!r}).write_text(json.dumps(dict(
            child=os.getpid(), grandchild=grandchild, session=os.getsid(0))))
time.sleep(30)
"""
    result = run_logged_process(
        [sys.executable, "-c", fixture],
        cwd=case_dir,
        env=clean_env(),
        stdout_path=case_dir / "stdout.raw",
        stderr_path=case_dir / "stderr.raw",
        sample_path=case_dir / "process-samples.jsonl",
        timeout_s=0.25,
        term_grace_s=0.25,
    )
    record = {
        "returncode": result.returncode,
        "timed_out": result.timed_out,
        "term_sent": result.term_sent,
        "kill_sent": result.kill_sent,
        "orphan_group_reaped": result.orphan_group_reaped,
        "observed_pids": result.observed_child_pids,
        "sentinel_unchanged": sentinel.read_text(encoding="utf-8") == "must-survive\n",
    }
    record["nested"] = json.loads(nested.read_text())
    record["remaining_active_groups"] = _session_groups(record["nested"]["session"])
    (case_dir / "result.json").write_text(json.dumps(record, indent=2) + "\n",
                                           encoding="utf-8")
    if (not result.timed_out or not result.term_sent or not result.kill_sent or
            not result.orphan_group_reaped or not record["sentinel_unchanged"] or
            record["remaining_active_groups"]):
        raise RuntimeError(f"python watchdog fixture failed: {record}")
    return record


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mpmc-binary", type=Path)
    parser.add_argument("--pool-binary", type=Path)
    parser.add_argument("--owner-binary", type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    args = parser.parse_args()
    output_root = args.output_root.resolve()
    if not output_root.is_dir() or str(output_root) in {"/", str(Path.home().resolve())}:
        parser.error("--output-root must be an existing private non-root directory")
    for value in (args.mpmc_binary, args.pool_binary, args.owner_binary):
        if value is not None:
            resolved = value.resolve()
            if not resolved.is_file() or not os.access(resolved, os.X_OK):
                parser.error(f"not an executable: {resolved}")
    run_dir = Path(tempfile.mkdtemp(prefix="edgeruntime-pressure-watchdog-", dir=output_root))
    records: list[dict[str, object]] = [run_fixture(run_dir)]
    if args.mpmc_binary is not None and args.pool_binary is not None:
        for binary, kind in ((args.mpmc_binary.resolve(), "mpmc"),
                             (args.pool_binary.resolve(), "pool")):
            records.append(run_timeout_case(binary, kind, run_dir, owner=False, ignore_term=False))
            records.append(run_timeout_case(binary, kind, run_dir, owner=False, ignore_term=True))
    if args.owner_binary is not None:
        owner_binary = args.owner_binary.resolve()
        for kind in ("mpmc", "pool"):
            records.append(run_timeout_case(owner_binary, kind, run_dir, owner=True, ignore_term=False))
            records.append(run_timeout_case(owner_binary, kind, run_dir, owner=True, ignore_term=True))
    summary = {"run_dir": str(run_dir), "cases": records}
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n",
                                           encoding="utf-8")
    print(json.dumps({"run_dir": str(run_dir), "case_count": len(records)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
