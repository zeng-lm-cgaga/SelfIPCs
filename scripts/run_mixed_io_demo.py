#!/usr/bin/env python3

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from pressure_process import run_logged_process


def interfaces():
    return json.loads(subprocess.check_output(["ip", "-j", "link", "show"], text=True))


def main():
    parser = argparse.ArgumentParser(description="Run the demo in an ephemeral user/network namespace")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--duration-ms", type=int, default=300)
    parser.add_argument("--can-batch", type=int, default=8)
    args = parser.parse_args()
    if not 100 <= args.duration_ms <= 5000 or not 1 <= args.can_batch <= 256:
        parser.error("duration must be 100..5000ms and CAN batch 1..256")
    binary = args.binary.resolve(strict=True)
    if not args.output_root.is_dir() or not os.access(binary, os.X_OK):
        parser.error("output root must exist and binary must be executable")
    for executable in ("unshare", "ip", "sh"):
        if not shutil.which(executable):
            parser.error(f"missing required executable: {executable}")
    output = Path(tempfile.mkdtemp(prefix="selfipcs-mixed-io-", dir=args.output_root)).resolve()
    interface = "smx" + str(os.getpid())
    script = ('set -eu; ip link add "$1" type vcan; ip link set "$1" up; '
              'ip -details link show "$1"; readlink /proc/self/ns/net; '
              'exec "$2" --interface "$1" --duration-ms "$3" --can-batch "$4"')
    command = ["unshare", "--user", "--map-current-user", "--net", "--keep-caps",
               "sh", "-c", script, "selfipcs-mixed-io", interface, str(binary),
               str(args.duration_ms), str(args.can_batch)]
    before = interfaces()
    record = {"command": command, "cwd": str(output), "interface": interface,
              "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "host_netns": os.readlink("/proc/self/ns/net"), "host_interfaces_before": before,
              "scope": "minimal functional demo; formal fairness/idle/scenario acceptance separate",
              "budget_seconds": 20}
    (output / "plan.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"OUTPUT={output}", flush=True)
    result = run_logged_process(command, cwd=output, env=dict(os.environ),
                                stdout_path=output / "stdout.log", stderr_path=output / "stderr.log",
                                sample_path=output / "processes.jsonl", timeout_s=20)
    after = interfaces()
    record.update(result=asdict(result), host_interfaces_after=after,
                  host_interface_identities_unchanged=sorted(
                      (entry["ifindex"], entry["ifname"]) for entry in before)
                  == sorted((entry["ifindex"], entry["ifname"]) for entry in after))
    (output / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record["result"]), flush=True)
    return 0 if result.returncode == 0 and not result.timed_out else 1


if __name__ == "__main__":
    raise SystemExit(main())
