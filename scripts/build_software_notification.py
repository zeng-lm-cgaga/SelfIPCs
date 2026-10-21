#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import tempfile


def compiler_identity(command: str, log: Path) -> dict[str, str]:
    executable = Path(shutil.which(command) or command).resolve()
    return {
        "requested": command, "executable": str(executable),
        "sha256": hashlib.sha256(executable.read_bytes()).hexdigest(),
        "version": log.read_text().strip(),
    }


def run(command: list[str], output: Path, label: str) -> None:
    print(shlex.join(command), flush=True)
    (output / f"{label}.command.json").write_text(json.dumps(command, indent=2) + "\n")
    with (output / f"{label}.log").open("w") as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            status = process.wait(timeout=180)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise RuntimeError(f"{label} exceeded 180 seconds; see {log.name}") from None
    if status:
        raise RuntimeError(f"{label} exited {status}; see {output / f'{label}.log'}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build only; never load modules or change permissions")
    parser.add_argument("--kernel-build", type=Path,
                        default=Path("/lib/modules") / os.uname().release / "build")
    parser.add_argument("--output-root", type=Path, default=Path(tempfile.gettempdir()))
    parser.add_argument("--userspace-only", action="store_true")
    parser.add_argument("--cc", default="cc")
    parser.add_argument("--kernel-cc", help="Optional kernel compiler override; no kernel flags are removed")
    parser.add_argument("--make", default="make")
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 64:
        parser.error("--jobs must be in [1, 64]")
    if not args.output_root.is_dir():
        parser.error("--output-root must be an existing directory")
    if not args.userspace_only and not (args.kernel_build / "Makefile").is_file():
        parser.error(f"kernel build tree missing: {args.kernel_build}")
    source = Path(__file__).resolve().parents[1] / "kernel" / "notification"
    output = Path(tempfile.mkdtemp(prefix="selfipcs-software-notification-", dir=args.output_root))
    output = output.resolve()
    print(f"OUTPUT={output}", flush=True)
    stage = output / "module"
    stage.mkdir()
    inputs = ("Kbuild", "Makefile", "src/self_ipcs_notification.c",
              "include/uapi/self_ipcs_notification.h", "probe/include/notification_probe.h",
              "probe/src/main.c", "probe/src/notification_probe.c")
    for relative in inputs:
        destination = stage / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, destination)
    release_header = args.kernel_build / "include/generated/utsrelease.h"
    release_match = re.search(r'#define UTS_RELEASE "([^"]+)"', release_header.read_text()) \
        if release_header.is_file() else None
    metadata = {
        "source": str(source), "kernel_build": str(args.kernel_build.resolve()),
        "running_kernel": os.uname().release, "module_built": False, "probe_built": False,
        "device_opened": False, "module_loaded": False, "status": "building",
        "kernel_cc": args.kernel_cc,
        "header_release": release_match.group(1) if release_match else None,
        "options": {
            "userspace_only": args.userspace_only, "jobs": args.jobs, "cc": args.cc,
            "kernel_cc": args.kernel_cc, "make": args.make,
            "kernel_flags": ["W=1", "KCFLAGS=-Werror"],
        },
        "builder_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "source_sha256": {
            relative: hashlib.sha256((stage / relative).read_bytes()).hexdigest()
            for relative in inputs
        },
        "kernel_files_sha256": {
            relative: hashlib.sha256((args.kernel_build / relative).read_bytes()).hexdigest()
            for relative in (".config", "Makefile", "Module.symvers", "include/generated/autoconf.h",
                             "include/generated/compile.h", "include/generated/utsrelease.h")
            if (args.kernel_build / relative).is_file()
        },
    }
    (output / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
    try:
        run([args.cc, "--version"], output, "probe-compiler")
        metadata["probe_compiler"] = compiler_identity(args.cc, output / "probe-compiler.log")
        if args.kernel_cc and not args.userspace_only:
            run([args.kernel_cc, "--version"], output, "kernel-compiler")
            metadata["kernel_compiler"] = compiler_identity(args.kernel_cc,
                                                            output / "kernel-compiler.log")
        run([args.cc, "-std=c11", "-O2", "-Wall", "-Wextra", "-Wpedantic", "-Wconversion",
             "-Wshadow", "-Werror", "-I", str(stage / "include/uapi"),
             "-I", str(stage / "probe/include"), str(stage / "probe/src/main.c"),
             str(stage / "probe/src/notification_probe.c"), "-o",
             str(output / "self_ipcs_notification_probe")], output, "probe-build")
        metadata["probe_built"] = True
        metadata["probe_sha256"] = hashlib.sha256(
            (output / "self_ipcs_notification_probe").read_bytes()).hexdigest()
        run([str(output / "self_ipcs_notification_probe"), "--abi-check"], output, "uapi-check")
        if not args.userspace_only:
            command = [args.make, "-C", str(args.kernel_build.resolve()), f"M={stage}",
                       f"-j{args.jobs}", "W=1", "KCFLAGS=-Werror"]
            if args.kernel_cc:
                command.append(f"CC={args.kernel_cc}")
            run(command + ["modules"], output, "module-build")
            metadata["module_built"] = True
            module = stage / "self_ipcs_notification.ko"
            metadata["module_path"] = str(module)
            metadata["module_sha256"] = hashlib.sha256(module.read_bytes()).hexdigest()
    except (OSError, RuntimeError) as error:
        metadata["status"] = "failed"
        (output / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
        print(f"FAIL: {error}")
        return 1
    metadata["status"] = "passed"
    (output / "build.json").write_text(json.dumps(metadata, indent=2) + "\n")
    print(f"PASS build/ABI only; runtime NOT validated. Artifacts: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
