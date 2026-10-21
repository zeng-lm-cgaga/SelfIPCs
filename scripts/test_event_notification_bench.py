#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from pressure_process import run_logged_process, terminate_process_group


def check_startup(producer: str, consumer: str, root: Path) -> None:
    case = root / "startup-gate"
    case.mkdir()
    name = f"notify_gate_{os.getpid()}_{time.monotonic_ns()}"
    producer_gate, producer_end = socket.socketpair()
    consumer_gate, consumer_end = socket.socketpair()
    consumer_gate.settimeout(4.0)
    processes = []
    outputs = []
    commands = []
    try:
        for role, binary, endpoint in (("producer", producer, producer_end),
                                       ("consumer", consumer, consumer_end)):
            output = (case / f"{role}.raw").open("wb")
            outputs.append(output)
            command = [binary, "--name", name, "--payload", "64", "--notification", "eventfd",
                       "--ready-fd", str(endpoint.fileno()), "--max-time-ms", "3000"]
            if role == "producer":
                command += ["--samples", "3", "--rate", "100"]
            else:
                command += ["--max-samples", "3", "--csv", str(case / "samples.csv")]
            commands.append(command)
            processes.append(subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                              pass_fds=(endpoint.fileno(),), start_new_session=True))
            endpoint.close()
            if role == "producer":
                deadline = time.monotonic() + 4.0
                while "READY" not in (case / "producer.raw").read_text():
                    if time.monotonic() >= deadline or processes[0].poll() is not None:
                        raise RuntimeError(f"producer did not create channel: {case}")
                    time.sleep(0.005)
        assert consumer_gate.recv(1) == b"R"
        time.sleep(0.1)
        assert "DONE" not in (case / "producer.raw").read_text()
        producer_gate.sendall(b"R")
        assert all(process.wait(timeout=4.0) == 0 for process in processes)
        with (case / "samples.csv").open() as samples:
            assert [int(row[0]) for row in csv.reader(samples)] == [1, 2, 3]
    finally:
        (case / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
        for process in processes:
            terminate_process_group(process)
        for output in outputs:
            output.close()
        for endpoint in (producer_gate, producer_end, consumer_gate, consumer_end):
            endpoint.close()
        control = str(Path(producer).parent / "self_ipcs_shm_ctl")
        cleanup = run_logged_process(
            [control, "remove", name], cwd=case, env=os.environ.copy(),
            stdout_path=case / "cleanup.raw", stderr_path=case / "cleanup.stderr",
            sample_path=case / "cleanup-processes.jsonl", timeout_s=5.0)
        if cleanup.timed_out or cleanup.returncode != 0:
            raise RuntimeError(f"owned channel cleanup failed: {case}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--driver", type=Path, required=True)
    parser.add_argument("--producer", type=Path, required=True)
    parser.add_argument("--consumer", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    root = Path(tempfile.mkdtemp(prefix="notification-bench-", dir=args.output_root))
    counter = 0

    def run(command: list[str], expected: int = 0) -> str:
        nonlocal counter
        counter += 1
        case = root / f"command-{counter}"
        case.mkdir()
        result = run_logged_process(
            command, cwd=case, env=os.environ.copy(), stdout_path=case / "stdout.raw",
            stderr_path=case / "stderr.raw", sample_path=case / "processes.jsonl",
            timeout_s=15.0,
        )
        (case / "command.json").write_text(json.dumps(command) + "\n")
        if result.timed_out or result.returncode != expected:
            raise RuntimeError(f"unexpected result {result}: {case}")
        return (case / "stdout.raw").read_text() + (case / "stderr.raw").read_text()

    driver, producer, consumer = (str(value.resolve()) for value in
                                  (args.driver, args.producer, args.consumer))
    run([driver, "--smoke", "--notification", "invalid"], 2)
    run([producer, "--name", "unused", "--notification", "invalid"], 2)
    run([consumer, "--name", "unused", "--csv", str(root / "unused.csv"),
         "--notification", "invalid"], 2)
    run([consumer, "--name", "unused", "--csv", str(root / "unused.csv"),
         "--mode", "poll", "--notification", "eventfd"], 2)
    listing = run([driver, "--list", "--notification", "eventfd"])
    assert "smoke-shm-eventfd-64B-same-100-controlled" in listing
    assert "smoke-socket-64B-same-100-controlled" in listing
    assert "shm-futex-" not in listing
    check_startup(producer, consumer, root)
    summaries = []
    for notification in ("futex", "eventfd"):
        run_id = f"smoke-shm-{notification}-64B-same-100-controlled"
        output = root / notification
        run([driver, "--smoke", "--only", run_id, "--notification", notification,
             "--producer", producer, "--consumer", consumer, "--out-dir", str(output)])
        summary = json.loads((output / run_id / "summary.json").read_text())
        with (output / run_id / "samples.csv").open() as samples:
            rows = list(csv.DictReader(samples))
        assert summary["notification"] == notification
        assert summary["payload_bytes"] == 64 and summary["rate_hz"] == 100
        assert summary["count"] == len(rows) == 50
        assert summary["warmup_count"] == 5 and summary["measured_count"] == 45
        assert summary["missed_samples"] == summary["torn_reads"] == 0
        assert summary["latency_comparison"]["eligible"]
        assert [int(row["sequence"]) for row in rows] == list(range(1, 51))
        assert all(int(row["missed_samples"]) == 0 for row in rows)
        assert all(int(row["latency_ns"]) == int(row["receive_ns"]) - int(row["publish_ns"])
                   for row in rows)
        latency = sorted(int(row["latency_ns"]) for row in rows[5:])
        for percentile in (50, 95, 99):
            assert summary["latency_ns"][f"p{percentile}"] == latency[
                math.ceil(len(latency) * percentile / 100) - 1]
        summaries.append(summary)
    (root / "verified.json").write_text(json.dumps(summaries, indent=2) + "\n")
    print(f"PASS CLI, paired 50-sample smoke, sequence and quantiles: {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
