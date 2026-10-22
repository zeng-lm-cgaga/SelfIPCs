#!/usr/bin/env python3
"""Bounded process-group execution for the cross-process pressure drivers."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import signal
import subprocess
import time
from typing import Mapping, Optional, Sequence


@dataclass
class ProcessResult:
    returncode: int
    timed_out: bool
    term_sent: bool
    kill_sent: bool
    orphan_group_reaped: bool
    observed_child_pids: list[int]
    start_epoch_ns: int
    end_epoch_ns: int


def child_snapshot(pid: int) -> list[int]:
    path = Path(f"/proc/{pid}/task/{pid}/children")
    try:
        return [int(value) for value in path.read_text().split()]
    except (OSError, ValueError):
        return []


def affinity_snapshot(pid: int) -> dict:
    started = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
    observation = {"pid": pid, "schema": 2, "begin_ns": started}
    process = Path(f"/proc/{pid}")
    try:
        identity = (process / "stat").read_text().rsplit(")", 1)[1].split()[19]
        argv = (process / "cmdline").read_bytes().decode().split("\0")[:-1]
        executable = os.readlink(process / "exe")
        tids = sorted(int(entry.name) for entry in (process / "task").iterdir())
        observation.update(
            starttime_ticks=int(identity), argv=argv, exe=executable,
            cpus=sorted(os.sched_getaffinity(pid)),
            threads=[{"tid": tid, "cpus": sorted(os.sched_getaffinity(tid))} for tid in tids])
        if (identity != (process / "stat").read_text().rsplit(")", 1)[1].split()[19]
                or argv != (process / "cmdline").read_bytes().decode().split("\0")[:-1]
                or executable != os.readlink(process / "exe")
                or tids != sorted(int(entry.name) for entry in (process / "task").iterdir())):
            observation["error"] = "process identity/exec/thread set changed during snapshot"
    except (OSError, ValueError, IndexError, UnicodeError) as error:
        observation["error"] = f"{type(error).__name__}: {error}"
    observation["end_ns"] = time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW)
    return observation


def process_group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _session_groups(session_id: int) -> dict[int, int]:
    if session_id <= 1 or session_id == os.getsid(0):
        raise ValueError("cleanup requires a dedicated child session")
    groups: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
            fields = stat[stat.rfind(")") + 2:].split()
            if fields[0] not in ("Z", "X") and int(fields[3]) == session_id:
                groups[int(fields[2])] = int(entry.name)
        except (OSError, ValueError, IndexError):
            continue
    return groups


def _signal_session(session_id: int, signum: signal.Signals) -> bool:
    sent = False
    for group, member in _session_groups(session_id).items():
        try:
            if os.getsid(member) == session_id and os.getpgid(member) == group:
                sent = _signal_group(group, signum) or sent
        except ProcessLookupError:
            continue
    return sent


def _wait_session_gone(session_id: int, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while _session_groups(session_id):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


def _signal_group(pgid: int, signum: signal.Signals) -> bool:
    try:
        os.killpg(pgid, signum)
    except ProcessLookupError:
        return False
    return True


def _wait_group_gone(pgid: int, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while process_group_alive(pgid):
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.01)
    return True


def _terminate_group(process: subprocess.Popen[str], pgid: int,
                     term_grace_s: float) -> tuple[bool, bool, bool]:
    """Terminate a session and report TERM/KILL/orphan cleanup actions."""
    _signal_session(pgid, signal.SIGCONT)
    term_sent = _signal_session(pgid, signal.SIGTERM)
    kill_sent = False
    try:
        process.wait(timeout=term_grace_s)
    except subprocess.TimeoutExpired:
        _signal_session(pgid, signal.SIGCONT)
        kill_sent = _signal_session(pgid, signal.SIGKILL)
        process.wait(timeout=term_grace_s)

    orphan_group_reaped = bool(_session_groups(pgid))
    if orphan_group_reaped:
        _signal_session(pgid, signal.SIGCONT)
        _signal_session(pgid, signal.SIGTERM)
        if not _wait_session_gone(pgid, term_grace_s):
            _signal_session(pgid, signal.SIGCONT)
            kill_sent = _signal_session(pgid, signal.SIGKILL) or kill_sent
            if not _wait_session_gone(pgid, term_grace_s):
                raise RuntimeError(f"child session {pgid} survived cleanup")
    return term_sent, kill_sent, orphan_group_reaped


def terminate_process_group(process: subprocess.Popen[str], term_grace_s: float = 1.0) -> tuple[bool, bool, bool]:
    """Best-effort bounded cleanup for a process already started by a caller."""
    if term_grace_s <= 0:
        raise ValueError("term_grace_s must be positive")
    if process.poll() is not None and not _session_groups(process.pid):
        return False, False, False
    return _terminate_group(process, process.pid, term_grace_s)


def _ready_roles(path: Path, roles: set[str]) -> bool:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return False
    return all(role in content for role in roles)


def run_logged_process(
    argv: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str],
    stdout_path: Path,
    stderr_path: Path,
    sample_path: Path,
    timeout_s: float,
    term_grace_s: float = 2.0,
    ready_path: Optional[Path] = None,
    ready_roles: Optional[set[str]] = None,
    startup_timeout_s: float = 10.0,
    capture_affinity: bool = False,
) -> ProcessResult:
    """Run argv in its own process group with bounded cleanup and file-backed output.

    The caller owns the run directory.  This function never removes paths or uses a
    shell.  If ready_roles are supplied, the work timeout starts only after the
    child-written ready file contains every requested role.  Any exception after
    Popen, including output or KeyboardInterrupt failures, enters the same bounded
    process-group cleanup path before being re-raised.
    """
    if timeout_s <= 0 or term_grace_s <= 0 or startup_timeout_s <= 0:
        raise ValueError("timeouts must be positive")
    if ready_roles and ready_path is None:
        raise ValueError("ready_path is required with ready_roles")
    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    stderr_path.parent.mkdir(parents=True, exist_ok=True)
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    start_epoch_ns = time.time_ns()
    observed: set[int] = set()
    process: Optional[subprocess.Popen[str]] = None
    pgid: Optional[int] = None
    timed_out = False
    term_sent = False
    kill_sent = False
    orphan_group_reaped = False
    end_epoch_ns = start_epoch_ns

    try:
        with stdout_path.open("w", encoding="utf-8", buffering=1) as stdout, \
                stderr_path.open("w", encoding="utf-8", buffering=1) as stderr, \
                sample_path.open("w", encoding="utf-8", buffering=1) as samples:
            process = subprocess.Popen(
                list(argv),
                cwd=cwd,
                env=dict(env),
                stdout=stdout,
                stderr=stderr,
                start_new_session=True,
                close_fds=True,
                text=True,
            )
            pgid = process.pid
            observed.add(process.pid)
            start_monotonic = time.monotonic()
            startup_deadline = start_monotonic + startup_timeout_s
            deadline: Optional[float] = None if ready_roles else start_monotonic + timeout_s

            while process.poll() is None:
                children = child_snapshot(process.pid)
                observed.update(children)
                affinity_samples = []
                if capture_affinity:
                    for child in children:
                        affinity_samples.append(affinity_snapshot(child))
                samples.write(json.dumps({
                    "epoch_ns": time.time_ns(),
                    "parent_pid": process.pid,
                    "process_group": pgid,
                    "children": children,
                    "affinities": affinity_samples,
                    "ready": _ready_roles(ready_path, ready_roles)
                    if ready_path is not None and ready_roles else None,
                }) + "\n")
                samples.flush()
                if ready_roles and deadline is None:
                    if _ready_roles(ready_path, ready_roles):
                        deadline = time.monotonic() + timeout_s
                    elif time.monotonic() >= startup_deadline:
                        raise RuntimeError("child IPC ready handshake did not complete")
                if deadline is not None and time.monotonic() >= deadline:
                    timed_out = True
                    term_sent, kill_sent, orphan_group_reaped = _terminate_group(
                        process, pgid, term_grace_s)
                    break
                time.sleep(0.01)

            if process.poll() is None:
                process.wait(timeout=term_grace_s)
            if pgid is not None and _session_groups(pgid):
                extra_term, extra_kill, orphan = _terminate_group(process, pgid, term_grace_s)
                term_sent = term_sent or extra_term
                kill_sent = kill_sent or extra_kill
                orphan_group_reaped = orphan_group_reaped or orphan

            end_epoch_ns = time.time_ns()
            samples.write(json.dumps({
                "epoch_ns": end_epoch_ns,
                "parent_pid": process.pid,
                "process_group": pgid,
                "children": [],
                "returncode": process.returncode,
                "timed_out": timed_out,
                "term_sent": term_sent,
                "kill_sent": kill_sent,
                "orphan_group_reaped": orphan_group_reaped,
            }) + "\n")
            samples.flush()
    except BaseException:
        if process is not None and pgid is not None:
            try:
                _terminate_group(process, pgid, term_grace_s)
            except BaseException:
                pass
        raise

    if process is None or process.returncode is None:
        raise RuntimeError("process did not produce a return code")
    return ProcessResult(
        returncode=process.returncode,
        timed_out=timed_out,
        term_sent=term_sent,
        kill_sent=kill_sent,
        orphan_group_reaped=orphan_group_reaped,
        observed_child_pids=sorted(observed),
        start_epoch_ns=start_epoch_ns,
        end_epoch_ns=end_epoch_ns,
    )
