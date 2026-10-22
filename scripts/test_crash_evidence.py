#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True

from pressure_process import run_logged_process


def verify_snapshot(directory, tag):
    values = dict(line.split("=", 1) for line in (directory / (tag + ".txt")).read_text().splitlines())
    name = values["name"]
    if not name.startswith("/edgeruntime.") or "/" in name[1:]:
        raise RuntimeError("unexpected retained object name")
    path = Path("/dev/shm") / name[1:]
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (info.st_dev, info.st_ino, info.st_size) != tuple(int(values[key]) for key in ("dev", "ino", "size")):
            raise RuntimeError("retained object identity changed")
        current = stream.read()
    snapshot = (directory / (tag + ".bin")).read_bytes()
    if current != snapshot:
        raise RuntimeError("retained object bytes differ from snapshot")
    return dict(path=str(path), inode=info.st_ino, size=info.st_size,
                sha256=hashlib.sha256(current).hexdigest())


def main():
    parser = argparse.ArgumentParser(description="Check real C12/C13 fault evidence retention without cleanup")
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    args.output_root.mkdir(parents=True, exist_ok=True)
    output = Path(tempfile.mkdtemp(prefix="retention-", dir=args.output_root))
    command = [str(args.matrix.resolve()), "--only", "C12,C13", "--out-dir", str(output)]
    result = run_logged_process(command, cwd=Path.cwd(), env=dict(os.environ),
                                stdout_path=output / "stdout.log", stderr_path=output / "stderr.log",
                                sample_path=output / "processes.jsonl", timeout_s=45,
                                capture_affinity=True)
    record = dict(command=command, **vars(result))
    (output / "command.json").write_text(json.dumps(record, indent=2) + "\n")
    if result.returncode != 0 or result.timed_out:
        raise RuntimeError("matrix failed; preserve output: " + str(output))
    runs = list(output.glob("run_*"))
    if len(runs) != 1:
        raise RuntimeError("expected exactly one matrix run")
    retained = []
    for case in ("C12", "C13"):
        directory = runs[0] / case
        if (directory / "result.txt").read_text() != "PASS\n":
            raise RuntimeError("missing real passing case: " + case)
        retained.append(verify_snapshot(directory, "retained-object"))
    retained.append(verify_snapshot(runs[0] / "C13", "creator-before-aba"))
    if retained[1]["inode"] == retained[2]["inode"]:
        raise RuntimeError("ABA fixture did not replace the original inode")
    (output / "verified-retained-objects.json").write_text(json.dumps(retained, indent=2) + "\n")
    print("PASS C12/C13 retained snapshots and live inodes; evidence=" + str(output))


if __name__ == "__main__":
    main()
