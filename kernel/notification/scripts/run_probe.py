#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import subprocess


def main() -> int:
    parser = argparse.ArgumentParser(description="Bounded, unprivileged probe of an already loaded device")
    parser.add_argument("--probe", type=Path, required=True)
    parser.add_argument("--device", type=Path, default=Path("/dev/self_ipcs_notification"))
    args = parser.parse_args()
    if not args.device.exists():
        print(f"SKIP: {args.device} absent; no module loaded; no runtime pass")
        return 77
    command = [str(args.probe.resolve()), "--device", str(args.device)]
    process = subprocess.Popen(command, start_new_session=True)
    try:
        return process.wait(timeout=25)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        print("FAIL: probe exceeded 25 seconds; process group killed")
        return 124


if __name__ == "__main__":
    raise SystemExit(main())
