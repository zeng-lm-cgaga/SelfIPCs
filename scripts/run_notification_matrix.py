#!/usr/bin/env python3
"""Serial finite notification evidence collection; --smoke is not formal evidence.

Formal: 64B/1P1C, 100/1000Hz, same/different CPUs, three alternating
futex/eventfd pairs (24 runs), 1000 publications, first 100 observations warm.
Explicit --backends futex,eventfd,driver selects the 36-run three-backend matrix.
Never deletes resource paths. Worker windows include pacing, payload/checksum,
buffered CSV recording and the producer eventfd service thread (RUSAGE_SELF).
Registration, initial thread enumeration and teardown are outside those windows.
Consumer begins just before signalling ready; producer begins after that gate.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import traceback

from pressure_process import run_logged_process


RULES = {
    "version": 1, "accepted_publications": 1000, "warmup_observations": 100,
    "minimum_postwarm_observations": 900, "maximum_gaps": 0,
    "maximum_duplicates": 0, "maximum_torn": 0, "maximum_timeouts": 0,
    "require_final_sequence_equals_accepted": True, "maximum_queue_overlap": 0,
    "integrity_allows_latest_value_jumps": True, "quantile": "nearest-rank",
    "overlap": "postwarm receive > next accepted publication timestamp",
    "cpu_percent": "100 * (work user_us + system_us) * 1000 / (end_ns - begin_ns)",
    "window": "CLOCK_MONOTONIC_RAW; RUSAGE_SELF all process threads; includes pacing and CSV",
    "lifecycle": "wait4 includes startup, work and teardown; never use as work CPU percent",
}


def parse_backends(value: str) -> tuple[str, ...]:
    requested = value.split(",")
    supported = ("futex", "eventfd", "driver")
    if not requested or len(requested) != len(set(requested)) or any(
            item not in supported for item in requested):
        raise argparse.ArgumentTypeError("backends must be unique names from futex,eventfd,driver")
    return tuple(item for item in supported if item in requested)


def cases(smoke: bool = False, backends: tuple[str, ...] = ("futex", "eventfd")) -> list[dict]:
    planned = []
    for repeat in range(1, 2 if smoke else 4):
        for rate in ([100] if smoke else [100, 1000]):
            for placement in (["same"] if smoke else ["same", "different"]):
                order = backends if repeat % 2 else backends[::-1]
                for notification in order:
                    prefix = "notification-smoke" if smoke else "notification"
                    planned.append({
                        "repeat": repeat, "rate_hz": rate, "placement": placement,
                        "notification": notification, "payload_bytes": 64,
                        "producers": 1, "consumers": 1, "samples": 50 if smoke else 1000,
                        "warmup": 5 if smoke else 100, "max_time_ms": 2000 if smoke else 15000,
                        "run_id": f"{prefix}-shm-{notification}-64B-{placement}-{rate}",
                    })
    return planned


def save(path: Path, value: object, *, immutable: bool = False) -> None:
    with path.open("x" if immutable else "w", encoding="utf-8") as output:
        json.dump(value, output, indent=2, sort_keys=True)
        output.write("\n")
    if immutable:
        path.chmod(0o444)


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def git_output(repo: Path, *arguments: str) -> str:
    return subprocess.check_output(["git", "-C", str(repo), *arguments], text=True)


def source_hashes(repo: Path) -> dict:
    names = git_output(repo, "ls-files", "-z", "--cached", "--others", "--exclude-standard").split("\0")
    return {name: digest(repo / name) for name in names if name and (repo / name).is_file()}


def invocation(case: dict, binaries: Path, output: Path, source: Path,
               cpu_a: int, cpu_b: int, smoke: bool) -> list[str]:
    argv = [str(binaries / "self_ipcs_shm_bench"), "--notification-matrix",
            "--notification", case["notification"], "--only", case["run_id"],
            "--producer", str(binaries / "self_ipcs_shm_bench_producer"),
            "--consumer", str(binaries / "self_ipcs_shm_bench_consumer"),
            "--ctl", str(binaries / "self_ipcs_shm_ctl"), "--out-dir", str(output),
            "--cpu-a", str(cpu_a), "--cpu-b", str(cpu_b), "--git-dir", str(source)]
    if smoke:
        argv.append("--notification-smoke")
    return argv


def owned_residuals(output: Path) -> list[dict]:
    records = []
    for declaration in sorted(output.glob("*/owned-resources.json")):
        owned = json.loads(declaration.read_text())
        paths = []
        for entry in owned["paths"]:
            current = dict(entry)
            try:
                status = Path(entry["path"]).lstat()
                current.update(exists_after=True, device=status.st_dev, inode=status.st_ino,
                               uid=status.st_uid, mode=status.st_mode, size=status.st_size,
                               ctime_ns=status.st_ctime_ns)
            except FileNotFoundError:
                current["exists_after"] = False
            paths.append(current)
        records.append({"channel": owned["channel"], "paths": paths,
                        "action": "reported_only_no_unlink"})
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--git-dir", type=Path, default=Path(__file__).resolve().parents[1],
                        help="real candidate source worktree, not a private harness or metadata path")
    parser.add_argument("--cpu-a", type=int, default=0)
    parser.add_argument("--cpu-b", type=int, default=1)
    parser.add_argument("--smoke", action="store_true", help="two 50-publication checks only")
    parser.add_argument("--backends", type=parse_backends, default=("futex", "eventfd"),
                        help="comma-separated subset of futex,eventfd,driver; missing driver fails its runs")
    args = parser.parse_args()

    def interrupted_by_signal(signum, frame):
        raise KeyboardInterrupt(f"collection interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, interrupted_by_signal)
    source, binaries, output_root = args.git_dir.resolve(), args.binary_dir.resolve(), args.output_root.resolve()
    if not output_root.is_dir() or output_root == Path("/"):
        parser.error("--output-root must be an existing non-root directory")
    if args.cpu_a == args.cpu_b or not {args.cpu_a, args.cpu_b} <= os.sched_getaffinity(0):
        parser.error("two distinct allowed CPUs are required")
    required = [binaries / name for name in ("self_ipcs_shm_bench", "self_ipcs_shm_bench_producer",
                                             "self_ipcs_shm_bench_consumer", "self_ipcs_shm_ctl")]
    for binary in required:
        if not binary.is_file() or not os.access(binary, os.X_OK):
            parser.error(f"missing executable: {binary}")
    cache = binaries.parent / "CMakeCache.txt"
    cache_text = cache.read_text()
    if f"CMAKE_HOME_DIRECTORY:INTERNAL={source}" not in cache_text.splitlines():
        parser.error("--git-dir must identify the source worktree used by this build")
    if not args.smoke:
        for setting in ("CMAKE_BUILD_TYPE:STRING=Release", "SELFIPCS_ENABLE_FAILPOINTS:BOOL=OFF",
                        "SELFIPCS_ENABLE_SANITIZERS:BOOL=OFF"):
            if setting not in cache_text.splitlines():
                parser.error(f"formal collection requires {setting}")
    root = output_root / f"notification-{'smoke' if args.smoke else 'formal'}-{time.time_ns()}-{os.getpid()}"
    root.mkdir()
    planned = cases(args.smoke, args.backends)
    rules = dict(RULES)
    if args.smoke:
        rules.update(accepted_publications=50, warmup_observations=5,
                     minimum_postwarm_observations=45)
    save(root / "plan.json", {"schema": 2, "backends": args.backends, "smoke": args.smoke, "cases": planned,
                             "rules": rules, "cpu_a": args.cpu_a, "cpu_b": args.cpu_b,
                             "serial": True, "watchdog_s": 60}, immutable=True)
    save(root / "plan.sha256.json", {"sha256": digest(root / "plan.json")}, immutable=True)
    manifest = {"source_dir": str(source), "cwd": str(source),
                "head": git_output(source, "rev-parse", "HEAD").strip(),
                "git_metadata_dir": git_output(source, "rev-parse", "--absolute-git-dir").strip(),
                "status": git_output(source, "status", "--porcelain=v1"),
                "source_files": source_hashes(source),
                "allowed_cpus": sorted(os.sched_getaffinity(0)), "environment": "VM_ONLY",
                "binaries": {str(binary): digest(binary) for binary in required},
                "cache_path": str(cache), "cache_sha256": digest(cache),
                "uname": list(os.uname()), "window_contract": rules["window"]}
    for artifact in (cache, binaries.parent / "compile_commands.json"):
        if artifact.is_file():
            (root / artifact.name).write_bytes(artifact.read_bytes())
    archives = sorted((binaries.parent / "src").glob("*.a"))
    manifest["core_archives"] = {str(path): digest(path) for path in archives}
    (root / "source.patch").write_text(git_output(source, "diff", "--binary", "HEAD"))
    manifest["source_patch_sha256"] = digest(root / "source.patch")
    save(root / "manifest.json", manifest, immutable=True)
    env = os.environ.copy()
    for key in list(env):
        if key.startswith(("SELFIPCS_FAILPOINT", "SELF_IPCS_FAILPOINT", "EDGE_FAILPOINT")) or key in (
                "LD_PRELOAD", "ASAN_OPTIONS", "UBSAN_OPTIONS"):
            env.pop(key)
    env["LC_ALL"] = "C"
    print(f"evidence={root}", flush=True)
    outcomes = []
    interrupted = False
    for ordinal, case in enumerate(planned, 1):
        directory = root / f"case-{ordinal:02d}"
        directory.mkdir()
        output = directory / "output"
        argv = invocation(case, binaries, output, source, args.cpu_a, args.cpu_b, args.smoke)
        save(directory / "command.json", {"case": case, "argv": argv, "cwd": str(source),
                                          "environment": {"LC_ALL": "C"}, "start_epoch_ns": time.time_ns()},
             immutable=True)
        try:
            result = run_logged_process(argv, cwd=source, env=env,
                                        stdout_path=directory / "stdout.raw",
                                        stderr_path=directory / "stderr.raw",
                                        sample_path=directory / "process-samples.jsonl",
                                        timeout_s=60, capture_affinity=True)
            outcome = asdict(result)
        except BaseException as error:
            outcome = {"returncode": None, "error": repr(error), "traceback": traceback.format_exc(),
                       "cleanup_unverified": True, "end_epoch_ns": time.time_ns()}
            interrupted = isinstance(error, (KeyboardInterrupt, SystemExit))
        save(directory / "exit.json", outcome)
        try:
            residuals = owned_residuals(output)
        except (OSError, ValueError, KeyError) as error:
            residuals = [{"error": str(error), "action": "reported_only_no_unlink"}]
        save(directory / "owned-residuals.json", residuals)
        outcomes.append({"ordinal": ordinal, "case": case, **outcome})
        save(root / "outcomes.json", outcomes)
        print(f"case={ordinal} notification={case['notification']} exit={outcome['returncode']}", flush=True)
        if interrupted:
            break
    save(root / "source-after.json", {"head": git_output(source, "rev-parse", "HEAD").strip(),
                                      "status": git_output(source, "status", "--porcelain=v1"),
                                      "source_files": source_hashes(source),
                                      "binaries": {str(binary): digest(binary) for binary in required}})
    if interrupted:
        return 130
    return 0 if all(item.get("returncode") == 0 and not item.get("timed_out")
                    and not item.get("orphan_group_reaped") for item in outcomes) else 1


if __name__ == "__main__":
    raise SystemExit(main())
