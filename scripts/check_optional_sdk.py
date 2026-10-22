#!/usr/bin/env python3

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

from pressure_process import run_logged_process


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(block)
    return checksum.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description="Check relocated installed SDK consumers only")
    parser.add_argument("--prefix", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--with-async", action="store_true")
    parser.add_argument("--sanitizers", action="store_true")
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    prefix = args.prefix.resolve()
    if not args.output_root.is_dir() or not (prefix / "lib/cmake/SelfIPCs").is_dir():
        parser.error("existing output root and installed prefix required")
    root = args.output_root.resolve() / f"installed-sdk-{time.time_ns()}-{os.getpid()}"
    root.mkdir()
    installed = root / "sdk"
    shutil.copytree(prefix, installed, symlinks=True)
    private_headers = [str(path) for path in (installed / "include").rglob("*")
                       if path.is_file() and "detail" in path.relative_to(installed / "include").parts]
    if private_headers:
        raise RuntimeError(f"private detail headers installed: {private_headers}")
    for config in (installed / "lib/cmake").rglob("*.cmake"):
        if str(source) in config.read_text():
            raise RuntimeError(f"source/build path in installed package: {config}")
    manifest = {"source": str(source), "original_prefix": str(prefix),
                "relocated_prefix": str(installed), "async": args.with_async,
                "sanitizers": args.sanitizers, "uname": list(os.uname()),
                "installed_files": {str(path.relative_to(installed)): digest(path)
                                    for path in installed.rglob("*") if path.is_file()}}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    if args.sanitizers:
        environment.update(ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                           UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1")
    outcomes = []

    def execute(directory: Path, label: str, command: list[str], timeout: int) -> None:
        identity = {"argv": command, "cwd": str(directory), "timeout_s": timeout,
                    "environment": {key: environment[key] for key in environment
                                    if key in ("ASAN_OPTIONS", "UBSAN_OPTIONS")}}
        (directory / f"{label}.command.json").write_text(json.dumps(identity, indent=2) + "\n")
        result = run_logged_process(command, cwd=directory, env=environment,
                                    stdout_path=directory / f"{label}.stdout",
                                    stderr_path=directory / f"{label}.stderr",
                                    sample_path=directory / f"{label}.processes.jsonl",
                                    timeout_s=timeout, capture_affinity=True)
        entry = {**identity, **asdict(result)}
        outcomes.append(entry)
        (directory / f"{label}.result.json").write_text(json.dumps(entry, indent=2) + "\n")
        (root / "outcomes.json").write_text(json.dumps(outcomes, indent=2) + "\n")
        if result.returncode or result.timed_out or result.orphan_group_reaped:
            raise RuntimeError(f"{label} failed: {entry}")

    examples = ["consume_demo", "queue_pool_demo", "notification_demo"]
    if args.with_async:
        examples.append("async_consume_demo")
    for example in examples:
        for legacy in (False, True):
            directory = root / f"{example}-{'legacy' if legacy else 'canonical'}"
            directory.mkdir()
            shutil.copytree(source / "examples" / example, directory / "source")
            command = ["cmake", "-S", "source", "-B", "build", "-G", "Unix Makefiles",
                       "-DCMAKE_BUILD_TYPE=Release", "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
                       f"-DCMAKE_PREFIX_PATH={installed}",
                       f"-DSelfIPCs_DIR={installed / 'lib/cmake/SelfIPCs'}",
                       f"-DEdgeRuntime_DIR={installed / 'lib/cmake/EdgeRuntime'}",
                       "-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF",
                       "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF",
                       f"-DSELFIPCS_DEMO_USE_LEGACY={'ON' if legacy else 'OFF'}"]
            if args.sanitizers:
                command += ["-DSELFIPCS_DEMO_ENABLE_SANITIZERS=ON",
                            "-DCMAKE_CXX_FLAGS=-fno-omit-frame-pointer -fsanitize=address,undefined",
                            "-DCMAKE_EXE_LINKER_FLAGS=-fsanitize=address,undefined"]
            execute(directory, "configure", command, 60)
            execute(directory, "build", ["cmake", "--build", "build", "-j2"], 120)
            commands = json.loads((directory / "build/compile_commands.json").read_text())
            for invocation in commands:
                if str(source) in invocation["command"]:
                    raise RuntimeError("consumer compiled against repository source/build")
            link = (directory / f"build/CMakeFiles/{example}.dir/link.txt").read_text()
            if str(installed / "lib/libself_ipcs.a") not in link:
                raise RuntimeError("consumer did not link relocated core archive")
            if str(source) in link:
                raise RuntimeError("consumer linked repository source/build")
            execute(directory, "run", [str(directory / "build" / example)], 20)
    (root / "result.json").write_text(json.dumps({"passed": True,
        "configurations": len(examples) * 2, "driver_execution": "separate privileged gate",
        "lsan": "disabled" if args.sanitizers else "not instrumented"}, indent=2) + "\n")
    print(json.dumps({"evidence": str(root), "configurations": len(examples) * 2, "passed": True}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
