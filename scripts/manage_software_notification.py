#!/usr/bin/env python3

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess

NAME = "self_ipcs_notification"
MODULE = Path("/sys/module") / NAME
HEADERS = Path("/lib/modules") / os.uname().release / "build"


def execute(command: list[str], timeout: float) -> dict:
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        return {"command": command, "returncode": process.returncode,
                "stdout": stdout, "stderr": stderr, "timed_out": False}
    except subprocess.TimeoutExpired:
        for action in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, action)
            except ProcessLookupError:
                pass
            try:
                stdout, stderr = process.communicate(timeout=1)
                return {"command": command, "returncode": 124, "child_returncode": process.returncode,
                        "stdout": stdout, "stderr": stderr, "timed_out": True,
                        "cleanup_complete": True}
            except subprocess.TimeoutExpired:
                pass
        return {"command": command, "returncode": 124, "timed_out": True,
                "cleanup_complete": False, "unreaped_pid": process.pid}


def tool(name: str) -> str:
    found = shutil.which(name)
    if not found:
        raise FileNotFoundError(errno.ENOENT, f"required tool {name} absent")
    return found


def status() -> dict:
    loaded = MODULE.is_dir()
    references = int((MODULE / "refcnt").read_text().strip()) if loaded else None
    return {"running_kernel": os.uname().release, "module": NAME, "loaded": loaded,
            "refcnt": references, "device_exists": Path("/dev", NAME).exists(),
            "headers": str(HEADERS), "headers_present": (HEADERS / "Makefile").is_file(),
            "euid": os.geteuid()}


def require_privilege() -> None:
    if os.geteuid() != 0:
        raise PermissionError(errno.EPERM, "explicit module management requires an authorized root operator")


def inspect_module(module: Path, timeout: float) -> dict:
    if not module.is_absolute() or module.is_symlink() or not module.is_file() or module.suffix != ".ko":
        raise ValueError("--module must be an absolute, regular .ko path (no symlink leaf)")
    release = os.uname().release
    header = HEADERS / "include/generated/utsrelease.h"
    if not (HEADERS / "Makefile").is_file() or not header.is_file():
        raise FileNotFoundError(errno.ENOENT, f"matching running-kernel headers absent: {HEADERS}")
    if not re.search(r'^#define UTS_RELEASE "' + re.escape(release) + r'"$', header.read_text(), re.M):
        raise ValueError("header release differs from running kernel")
    details = {}
    for field in ("name", "vermagic"):
        result = execute([tool("modinfo"), "-F", field, str(module)], timeout)
        details[field] = result
        if result["returncode"]:
            raise RuntimeError(f"modinfo {field} failed: {json.dumps(result)}")
    if details["name"]["stdout"].strip() != NAME:
        raise ValueError("module name mismatch")
    if details["vermagic"]["stdout"].split()[:1] != [release]:
        raise ValueError("module vermagic differs from running kernel")
    details["path"] = str(module)
    details["sha256"] = hashlib.sha256(module.read_bytes()).hexdigest()
    return details


def manage(action: str, module: Path | None, timeout: float) -> dict:
    before = status()
    record = {"action": action, "before": before, "executed": False, "returncode": 0}
    if action == "status":
        return record
    if action == "load":
        if before["loaded"]:
            raise FileExistsError(errno.EEXIST, "module already loaded; no replacement attempted")
        if module is None:
            raise ValueError("load requires --module")
        record["module_info"] = inspect_module(module, timeout)
        require_privilege()
        command = [tool("insmod"), str(module)]
    else:
        if not before["loaded"]:
            raise FileNotFoundError(errno.ENOENT, "module is not loaded")
        if before["refcnt"] is None or before["refcnt"] != 0:
            raise OSError(errno.EBUSY, "active/unknown references; normal unload refused")
        require_privilege()
        command = [tool("rmmod"), NAME]
    record["execution"] = execute(command, timeout)
    record["executed"] = True
    record["returncode"] = record["execution"]["returncode"]
    record["after"] = status()
    return record


def main() -> int:
    parser = argparse.ArgumentParser(description="Explicit module status/load/normal unload; never sudo or change permissions")
    parser.add_argument("action", choices=("status", "load", "unload"))
    parser.add_argument("--module", type=Path)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if not 0 < args.timeout <= 30:
        parser.error("--timeout must be in (0, 30]")
    if args.action != "load" and args.module is not None:
        parser.error("--module is only valid for load")
    try:
        record = manage(args.action, args.module, args.timeout)
    except (OSError, ValueError, RuntimeError) as error:
        record = {"action": args.action, "executed": False, "returncode": 1,
                  "error": str(error), "errno": getattr(error, "errno", None)}
    print(json.dumps(record, indent=2))
    return record["returncode"]


if __name__ == "__main__":
    raise SystemExit(main())
