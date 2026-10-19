#!/usr/bin/env python3
"""Safe, reproducible release-install and downstream-consumer audit."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Iterable, Optional

from self_ipcs_compat import default_build_dir, resolve_binary, validate_release_cache


class AuditFailure(RuntimeError):
	"""A required audit stage failed."""


def repo_root() -> pathlib.Path:
	return pathlib.Path(__file__).resolve().parent.parent


def is_protected(path: pathlib.Path, root: pathlib.Path) -> bool:
	resolved = path.resolve(strict=False)
	home = pathlib.Path.home().resolve()
	protected = {pathlib.Path(resolved.anchor), home, root.resolve()}
	if resolved in protected:
		return True
	try:
		root.resolve().relative_to(resolved)
		return True
	except ValueError:
		return False


def has_symlink_component(path: pathlib.Path) -> bool:
	current = pathlib.Path(path.anchor) if path.is_absolute() else pathlib.Path.cwd()
	parts = path.parts[1:] if path.is_absolute() else path.parts
	for part in parts:
		current /= part
		try:
			if current.is_symlink():
				return True
		except OSError:
			return True
	return False


def validate_output_base(value: Optional[str], root: pathlib.Path) -> pathlib.Path:
	if value is None:
		candidate = root / "evidence" / "bugs" / "ci-runs"
	else:
		if value == "":
			raise ValueError("--out-dir must not be empty")
		candidate = pathlib.Path(value).expanduser()
		if not candidate.is_absolute():
			candidate = root / candidate
	if has_symlink_component(candidate):
		raise ValueError("--out-dir contains a symlink component")
	if is_protected(candidate, root):
		raise ValueError("--out-dir is a protected root, home, repository, or ancestor")
	if candidate.exists() and not candidate.is_dir():
		raise ValueError("--out-dir exists and is not a directory")
	return candidate


def make_run_dir(base: pathlib.Path) -> pathlib.Path:
	base.mkdir(parents=True, exist_ok=True)
	if has_symlink_component(base) or not base.is_dir():
		raise ValueError("output base changed to a symlink or non-directory")
	return pathlib.Path(tempfile.mkdtemp(prefix="run-", dir=str(base))).resolve()


def command_text(args: Iterable[str]) -> str:
	return " ".join(subprocess.list2cmdline([arg]) for arg in args)


def run_command(label: str, args: list[str], cwd: pathlib.Path, log_path: pathlib.Path,
					timeout: float, env: Optional[dict[str, str]] = None) -> None:
	merged_env = os.environ.copy()
	if env is not None:
		merged_env.update(env)
	line = f"\n[command] cwd={cwd} timeout={timeout}s\n[command] {command_text(args)}\n"
	with log_path.open("a", encoding="utf-8") as log:
		log.write(f"\n[stage] {label}\n{line}")
		log.flush()
		print(f"[ci] {label}: {command_text(args)}", flush=True)
		process = subprocess.Popen(
			args,
			cwd=str(cwd),
			env=merged_env,
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			text=True,
			start_new_session=True,
		)
		try:
			output, _ = process.communicate(timeout=timeout)
		except subprocess.TimeoutExpired as exc:
			os.killpg(process.pid, signal.SIGKILL)
			output, _ = process.communicate()
			log.write(output)
			log.write(f"\n[exit] {label}=TIMEOUT\n")
			raise AuditFailure(f"{label} timed out after {timeout}s") from exc
		log.write(output)
		log.write(f"\n[exit] {label}={process.returncode}\n")
		if output:
			print(output, end="")
		if process.returncode != 0:
			raise AuditFailure(f"{label} exited with {process.returncode}")


def assert_release_configuration(build_dir: pathlib.Path, log_path: pathlib.Path) -> None:
	try:
		cache = validate_release_cache(build_dir / "CMakeCache.txt", require_testing_off=True,
									  require_optimization=False)
	except ValueError as error:
		raise AuditFailure(str(error)) from error
	expected = {
		"CMAKE_BUILD_TYPE": "Release",
		"BUILD_TESTING": "OFF",
	}
	for key, value in expected.items():
		if cache.get(key) != value:
			raise AuditFailure(f"Release cache {key}={cache.get(key)!r}, expected {value!r}")
	compile_commands = build_dir / "compile_commands.json"
	if compile_commands.exists():
		text = compile_commands.read_text(encoding="utf-8")
		if "-fsanitize=" in text or any(flag in text for flag in
										("SELFIPCS_ENABLE_FAILPOINTS", "EDGERUNTIME_ENABLE_FAILPOINTS")):
			raise AuditFailure("Release compile commands contain sanitizer or failpoint flags")
	with log_path.open("a", encoding="utf-8") as log:
		flags = {key: value for key, value in cache.items()
				 if key.startswith(("SELFIPCS_", "EDGERUNTIME_"))}
		log.write(f"\n[release-cache] {expected | flags}\n")


def assert_install(prefix: pathlib.Path, root: pathlib.Path) -> None:
	required = [
		prefix / "lib" / "libself_ipcs.a",
		prefix / "lib" / "cmake" / "SelfIPCs" / "SelfIPCsConfig.cmake",
		prefix / "lib" / "cmake" / "SelfIPCs" / "SelfIPCsTargets.cmake",
		prefix / "include" / "self_ipcs" / "queue" / "mpmc_queue.hpp",
		prefix / "include" / "self_ipcs" / "buffer" / "shared_buffer_pool.hpp",
	]
	for path in required:
		if not path.is_file():
			raise AuditFailure(f"missing installed artifact: {path}")
	if any((prefix / "include" / namespace / "detail").exists()
		   for namespace in ("self_ipcs", "edge_runtime")):
		raise AuditFailure("internal detail headers leaked into the install")
	for path in prefix.rglob("*"):
		if path.is_file():
			text = path.read_text(encoding="utf-8", errors="ignore")
			if str(root / "include") in text or str(root / "src") in text:
				raise AuditFailure(f"installed export embeds repository source path: {path}")


def assert_downstream_sources(build_dir: pathlib.Path, prefix: pathlib.Path,
							root: pathlib.Path) -> None:
	compile_commands = build_dir / "compile_commands.json"
	if not compile_commands.is_file():
		raise AuditFailure("downstream compile_commands.json was not generated")
	text = compile_commands.read_text(encoding="utf-8")
	for forbidden in (str(root / "include"), str(root / "src"), str(root / "build")):
		if forbidden in text:
			raise AuditFailure(f"downstream compile command uses repository path: {forbidden}")
	if str(prefix / "include") not in text:
		raise AuditFailure("downstream compile command does not use the install prefix")
	link_file = build_dir / "CMakeFiles" / "consume_demo.dir" / "link.txt"
	if not link_file.is_file() or str(prefix / "lib" / "libself_ipcs.a") not in link_file.read_text(
		encoding="utf-8"
	):
		raise AuditFailure("downstream link command does not use installed libself_ipcs.a")


def run_safety_self_test(root: pathlib.Path) -> None:
	for invalid in ("/", str(pathlib.Path.home()), str(root), str(root.parent)):
		result = subprocess.run(
			[sys.executable, str(pathlib.Path(__file__).resolve()), "--validate-only",
			 "--out-dir", invalid],
			cwd="/tmp",
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			text=True,
		)
		if result.returncode != 2:
			raise AuditFailure(f"unsafe path accepted: {invalid!r}: {result.stdout}")
	with tempfile.TemporaryDirectory(prefix="selfipcs-audit-safety-") as temp:
		base = pathlib.Path(temp) / "out"
		base.mkdir()
		sentinel = base / "sentinel"
		sentinel.write_text("keep\n", encoding="utf-8")
		first = make_run_dir(base)
		second = make_run_dir(base)
		if first == second or sentinel.read_text(encoding="utf-8") != "keep\n":
			raise AuditFailure("safe output self-test did not preserve sentinel or uniqueness")
		outside = pathlib.Path(temp) / "outside"
		outside.mkdir()
		link = base / "escape"
		link.symlink_to(outside, target_is_directory=True)
		try:
			validate_output_base(str(link), root)
		except ValueError:
			pass
		else:
			raise AuditFailure("symlink escape self-test was accepted")
	for argv in (
		["--unknown"],
		["--out-dir"],
	):
		result = subprocess.run(
			[sys.executable, str(pathlib.Path(__file__).resolve()), *argv],
			cwd="/tmp",
			stdout=subprocess.PIPE,
			stderr=subprocess.STDOUT,
			text=True,
		)
		if result.returncode != 2:
			raise AuditFailure(f"invalid CLI form did not return 2: {argv}")
	print("[ci] safety self-test: protected paths, symlink, sentinel, and CLI guards passed")


def run_audit(args: argparse.Namespace, root: pathlib.Path) -> pathlib.Path:
	base = validate_output_base(args.out_dir, root)
	run_dir = make_run_dir(base)
	log_path = run_dir / "ci.log"
	log_path.write_text(f"repo={root}\nrun={run_dir}\nstarted={time.time()}\n", encoding="utf-8")
	scratch_dir = pathlib.Path(tempfile.mkdtemp(prefix="selfipcs-ci-", dir="/tmp")).resolve()
	with log_path.open("a", encoding="utf-8") as log:
		log.write(f"scratch={scratch_dir}\n")
	print(f"[ci] run directory: {run_dir}")
	print(f"[ci] private scratch: {scratch_dir}")
	run_safety_self_test(root)

	for preset in ("dev-debug", "asan-ubsan"):
		run_command(f"{preset} configure", ["cmake", "--preset", preset], root, log_path, 300)
		run_command(f"{preset} build", ["cmake", "--build", "--preset", preset], root, log_path, 900)
		try:
			run_command(
				f"{preset} ctest",
				["ctest", "--preset", preset, "--output-on-failure", "--no-tests=error",
				 "--output-junit", str(run_dir / f"{preset}.xml")],
				root, log_path, 900,
			)
		finally:
			last_test = default_build_dir(root, preset) / "Testing" / "Temporary" / "LastTest.log"
			if last_test.is_file():
				shutil.copyfile(last_test, run_dir / f"{preset}-LastTest.log")

	if args.full:
		tools = default_build_dir(root, "dev-debug") / "tools"
		run_command(
			"full crash matrix C01-C23",
			[str(resolve_binary(tools, "edge_crash_matrix")),
			 "--producer", str(resolve_binary(tools, "edge_shm_producer")),
			 "--consumer", str(resolve_binary(tools, "edge_shm_consumer")),
			 "--ctl", str(resolve_binary(tools, "edge_shm_ctl")),
			 "--supervisor", str(resolve_binary(tools, "edge_shm_supervisor")),
			 "--out-dir", str(run_dir / "crash")],
			root, log_path, 300,
		)

	release_build = scratch_dir / "build" / "selfipcs-release"
	install_prefix = scratch_dir / "install"
	downstream_source = scratch_dir / "downstream-source"
	downstream_build = scratch_dir / "downstream-build"
	run_command(
		"release configure",
		[
			"cmake",
			"-S",
			str(root),
			"-B",
			str(release_build),
			"-G",
			"Unix Makefiles",
			"-DCMAKE_BUILD_TYPE=Release",
			"-DBUILD_TESTING=OFF",
			"-DSELFIPCS_ENABLE_FAILPOINTS=OFF",
			"-DSELFIPCS_ENABLE_SANITIZERS=OFF",
			"-DSELFIPCS_WARNINGS_AS_ERRORS=ON",
			"-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
		],
		root,
		log_path,
		300,
	)
	assert_release_configuration(release_build, log_path)
	run_command("release build", ["cmake", "--build", str(release_build), "--parallel", "2"], root,
				 log_path, 900)
	run_command(
			"release install",
			["cmake", "--install", str(release_build), "--prefix", str(install_prefix)],
			root,
			log_path,
			300,
		)
	assert_install(install_prefix, root)
	shutil.copytree(root / "examples" / "consume_demo", downstream_source)
	run_command(
		"downstream configure",
		[
			"cmake",
			"-S",
			str(downstream_source),
			"-B",
			str(downstream_build),
			"-G",
			"Unix Makefiles",
			"-DCMAKE_BUILD_TYPE=Release",
			f"-DCMAKE_PREFIX_PATH={install_prefix}",
			f"-DSelfIPCs_DIR={install_prefix / 'lib' / 'cmake' / 'SelfIPCs'}",
			"-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF",
			"-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF",
			"-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
		],
		root,
		log_path,
		300,
	)
	run_command("downstream build", ["cmake", "--build", str(downstream_build), "--verbose"], root,
				 log_path, 600)
	assert_downstream_sources(downstream_build, install_prefix, root)
	run_command("downstream run", [str(downstream_build / "consume_demo")], root, log_path, 30)

	if args.package:
		from release_package import create_assets, extract_asset

		packages = base / "packages"
		create_assets(root, install_prefix, release_build, packages, full=args.full)
		relocated = extract_asset(root, packages, scratch_dir / "relocated")
		relocated_build = scratch_dir / "relocated-consumer"
		run_command("relocated package consumer configure", [
			"cmake", "-S", str(downstream_source), "-B", str(relocated_build),
			"-DCMAKE_BUILD_TYPE=Release", f"-DCMAKE_PREFIX_PATH={relocated}",
			f"-DSelfIPCs_DIR={relocated / 'lib/cmake/SelfIPCs'}",
			"-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF", "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF",
			"-DCMAKE_EXPORT_COMPILE_COMMANDS=ON"], root, log_path, 300)
		run_command("relocated package consumer build", ["cmake", "--build", str(relocated_build)],
					root, log_path, 300)
		assert_install(relocated, root)
		assert_downstream_sources(relocated_build, relocated, root)
		run_command("relocated package consumer run", [str(relocated_build / "consume_demo")],
					root, log_path, 30)
		for legacy_first in ("OFF", "ON"):
			compat_source = scratch_dir / f"compat-source-{legacy_first}"
			shutil.copytree(root / "examples/compat_demo", compat_source)
			compat_build = scratch_dir / f"compat-build-{legacy_first}"
			run_command(f"compat {legacy_first} configure", [
				"cmake", "-S", str(compat_source), "-B", str(compat_build),
				f"-DCMAKE_PREFIX_PATH={relocated}",
				f"-DSelfIPCs_DIR={relocated / 'lib/cmake/SelfIPCs'}",
				f"-DEdgeRuntime_DIR={relocated / 'lib/cmake/EdgeRuntime'}",
				f"-DSELFIPCS_COMPAT_LEGACY_FIRST={legacy_first}",
				"-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF", "-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF"],
				root, log_path, 300)
			run_command(f"compat {legacy_first} build", ["cmake", "--build", str(compat_build)],
						root, log_path, 300)
			run_command(f"compat {legacy_first} run", ["ctest", "--test-dir", str(compat_build),
				"--output-on-failure", "--no-tests=error"], root, log_path, 30)
		source = extract_asset(root, packages, scratch_dir / "unpacked-source", source=True)
		source_build = scratch_dir / "source-build"
		run_command("source archive configure", ["cmake", "-S", str(source), "-B", str(source_build),
			"-DCMAKE_BUILD_TYPE=Release", "-DBUILD_TESTING=OFF", "-DSELFIPCS_ENABLE_FAILPOINTS=OFF",
			"-DSELFIPCS_ENABLE_SANITIZERS=OFF"], root, log_path, 300)
		run_command("source archive build", ["cmake", "--build", str(source_build), "--parallel", "2"],
					root, log_path, 600)
		print(f"[ci] release assets: {packages}")

	(run_dir / "summary.json").write_text(json.dumps({
		"success": True, "full_crash_matrix": args.full, "packaged": args.package,
		"install_prefix": str(install_prefix), "release_build": str(release_build),
	}, indent=2) + "\n", encoding="utf-8")

	if not log_path.is_file():
		raise AuditFailure("audit log disappeared")
	print(f"[ci] transcript: {log_path}")
	print(f"[ci] install prefix: {install_prefix}")
	print(f"[ci] installed package: {install_prefix / 'lib' / 'cmake' / 'SelfIPCs'}")
	print(f"[ci] private scratch retained: {scratch_dir}")
	return run_dir


def main() -> int:
	parser = argparse.ArgumentParser(allow_abbrev=False)
	parser.add_argument("--out-dir", default=None)
	parser.add_argument("--self-test-safety", action="store_true")
	parser.add_argument("--validate-only", action="store_true")
	parser.add_argument("--full", action="store_true", help="also execute the complete crash matrix")
	parser.add_argument("--package", action="store_true", help="package and test relocatable release assets")
	args = parser.parse_args()
	root = repo_root()
	try:
		if args.validate_only:
			validate_output_base(args.out_dir, root)
			return 0
		if args.self_test_safety:
			run_safety_self_test(root)
			return 0
		run_audit(args, root)
		return 0
	except ValueError as error:
		print(f"[ci] ERROR: {error}", file=sys.stderr)
		return 2
	except (AuditFailure, OSError) as error:
		print(f"[ci] ERROR: {error}", file=sys.stderr)
		return 1


if __name__ == "__main__":
	raise SystemExit(main())
