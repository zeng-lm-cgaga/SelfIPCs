#!/usr/bin/env python3
"""Exercise the standalone benchmark's own worker/descendant watchdog."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time

from pressure_process import _session_groups, run_logged_process


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--all-modes", action="store_true")
    args = parser.parse_args()
    args.output_root.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="bench-worker-deadline-", dir=args.output_root)).resolve()
    driver = args.driver.resolve()
    helper = root / "fault-helper"
    helper.write_text(f"#!{sys.executable}\n" + """import os, time
child = os.fork()
if child == 0:
    time.sleep(120)
    os._exit(0)
print('FAULT_WRAPPER_EXITED child=' + str(child), flush=True)
os._exit(0)
""")
    helper.chmod(0o700)
    specifications = [
        ("futex", "--smoke", "smoke-shm-futex-64B-same-100-controlled", 2000),
        ("eventfd", "--smoke", "smoke-shm-eventfd-64B-same-100-controlled", 2000),
        ("poll", "--evidence", "shm-poll-64B-same-max", 60000),
        ("socket", "--smoke", "smoke-socket-64B-same-100-controlled", 2000),
    ]
    if not args.all_modes:
        specifications = specifications[:1]

    def check(specification: tuple[str, str, str, int]) -> dict[str, object]:
        mode, profile, run_id, work_ms = specification
        case = root / mode
        case.mkdir()
        command = [str(driver), profile, "--only", run_id, "--notification",
                   "eventfd" if mode == "eventfd" else "futex", "--producer", str(helper),
                   "--consumer", str(helper), "--sock", str(helper), "--out-dir",
                   str(case / "driver")]
        (case / "command.json").write_text(json.dumps(command, indent=2) + "\n")
        started = time.monotonic()
        result = run_logged_process(
            command, cwd=case, env=dict(os.environ), stdout_path=case / "stdout.raw",
            stderr_path=case / "stderr.raw", sample_path=case / "processes.jsonl",
            timeout_s=work_ms / 1000 + 22, term_grace_s=1.0)
        elapsed = time.monotonic() - started
        text = "\n".join(path.read_text(errors="replace") for path in case.rglob("*.raw"))
        parent = json.loads((case / "processes.jsonl").read_text().splitlines()[0])["parent_pid"]
        remaining = _session_groups(parent)
        record = dict(mode=mode, command=command, work_ms=work_ms,
                      elapsed_seconds=elapsed, returncode=result.returncode,
                      outer_timeout=result.timed_out, inner_watchdog="WATCHDOG" in text,
                      fault_observed="FAULT_WRAPPER_EXITED" in text,
                      remaining_session_groups=remaining)
        (case / "result.json").write_text(json.dumps(record, indent=2) + "\n")
        return record

    with ThreadPoolExecutor(max_workers=len(specifications)) as executor:
        records = list(executor.map(check, specifications))
    verified = all(record["returncode"] == 1 and not record["outer_timeout"] and
                   record["inner_watchdog"] and record["fault_observed"] and
                   not record["remaining_session_groups"] for record in records)
    report = dict(driver=str(driver), driver_sha256=hashlib.sha256(driver.read_bytes()).hexdigest(),
                  cases=records, verified=verified)
    (root / "verified.json").write_text(json.dumps(report, indent=2) + "\n")
    print(f"{'PASS' if verified else 'FAIL'} internal worker deadlines: {root}")
    return 0 if verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
