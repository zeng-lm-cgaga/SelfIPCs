#!/usr/bin/env python3
"""Focused regression tests for the formal matrix and independent CSV verifier."""

from __future__ import annotations

import argparse
from contextlib import redirect_stderr
import copy
from dataclasses import asdict
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from pressure_process import affinity_snapshot, run_logged_process
from release_audit import (AuditFailure, assert_downstream_sources, assert_install,
                           assert_release_configuration)
from run_formal_bench import cases, digest, invocation, main as runner_main, message_budget
from self_ipcs_compat import (artifact_names, build_target, default_build_dir, resolve_archive,
                             resolve_binary, validate_release_cache)
from verify_formal_bench import check_delivery, check_manifest, check_plans, quantiles


BINARY_DIR = None
OUTPUT_ROOT = Path("/tmp")


class FormalBenchTest(unittest.TestCase):
    def test_platform_probe_name_mapping(self):
        for name in ("edge_runtime_platform_probe", "self_ipcs_platform_probe"):
            self.assertEqual(artifact_names(name),
                             ("self_ipcs_platform_probe", "edge_runtime_platform_probe"))

    def write_release_cache(self, path, prefixes):
        lines = ["CMAKE_BUILD_TYPE:STRING=Release", "CMAKE_CXX_FLAGS_RELEASE:STRING=-O3 -DNDEBUG",
                 "BUILD_TESTING:BOOL=OFF"]
        for prefix in prefixes:
            lines.extend(f"{prefix}_{suffix}:BOOL={value}" for suffix, value in (
                ("WARNINGS_AS_ERRORS", "ON"), ("ENABLE_SANITIZERS", "OFF"),
                ("ENABLE_FAILPOINTS", "OFF")))
        path.write_text("\n".join(lines) + "\n")

    def make_manifest(self, root, kind, legacy, placement="same"):
        binary_dir = root / "build" / "tools"
        binary_dir.mkdir(parents=True)
        for name in ("shm_bench", "shm_bench_producer", "shm_bench_consumer", "shm_bench_sock",
                     "shm_ctl", "mpmc_bench", "buffer_pool_bench"):
            binary = binary_dir / f"{'edge_' if legacy else 'self_ipcs_'}{name}"
            binary.write_bytes(b"metadata fixture; never executed")
            binary.chmod(0o700)
        case = cases(kind, 1)[0]
        if kind == "spsc":
            case["placement"] = placement
        command = root / "evidence" / "case-01" / "command.json"
        command.parent.mkdir(parents=True)
        cache = command.parent.parent / "CMakeCache.txt"
        self.write_release_cache(cache, ("EDGERUNTIME" if legacy else "SELFIPCS",))
        argv = invocation(case, binary_dir, command.parent / "output", 2, 3, root)
        metadata = {"case": case, "argv": argv,
                    "source": {"binaries": {argv[0]: digest(Path(argv[0]))},
                               "cache_sha256": digest(cache), "cpu_a": 2, "cpu_b": 3}}
        if kind == "spsc":
            samples = []
            for prefix in ("edge_", "self_ipcs_"):
                for role in ("producer", "consumer"):
                    name = (f"{prefix}shm_bench_{role}" if prefix == "edge_"
                            else f"{prefix}shm_bench_sock")
                    cpu = 3 if placement == "different" and role == "consumer" else 2
                    samples.append({"argv": [name, "--role", role], "cpus": [cpu]})
            (command.parent / "process-samples.jsonl").write_text(
                json.dumps({"affinities": samples}) + "\n")
        command.write_text(json.dumps(metadata))
        return command, metadata

    def test_binary_and_archive_prefer_canonical_with_legacy_fallback(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            for name in ("shm_bench", "shm_bench_producer", "shm_bench_consumer", "shm_bench_sock",
                         "shm_ctl", "mpmc_bench", "buffer_pool_bench", "crash_matrix"):
                with self.subTest(tool=name):
                    canonical, legacy = artifact_names(f"edge_{name}")
                    old = root / legacy
                    old.write_bytes(b"legacy fixture")
                    old.chmod(0o700)
                    self.assertEqual(resolve_binary(root, canonical), old)
                    new = root / canonical
                    new.write_bytes(b"canonical fixture")
                    new.chmod(0o600)
                    self.assertEqual(resolve_binary(root, canonical), old)
                    new.chmod(0o700)
                    self.assertEqual(resolve_binary(root, legacy), new)
                    self.assertEqual(resolve_binary(root, canonical), new)
            (root / "src").mkdir()
            old_archive = root / "src" / "libedge_runtime.a"
            old_archive.write_bytes(b"legacy archive")
            self.assertEqual(resolve_archive(root), old_archive)
            new_archive = root / "src" / "libself_ipcs.a"
            new_archive.write_bytes(b"canonical archive")
            self.assertEqual(resolve_archive(root), new_archive)
            with self.assertRaises(FileNotFoundError):
                resolve_binary(root, "self_ipcs_missing")

    def test_build_directories_and_targets_support_both_generations(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            for preset, suffix in (("dev-debug", "debug"), ("asan-ubsan", "asan-ubsan"),
                                   ("release", "release")):
                canonical = root / "build" / f"selfipcs-{suffix}"
                legacy = root / "build" / preset
                self.assertEqual(default_build_dir(root, preset), canonical)
                legacy.mkdir(parents=True)
                self.assertEqual(default_build_dir(root, preset), legacy)
                canonical.mkdir()
                self.assertEqual(default_build_dir(root, preset), canonical)
            targets = canonical / "CMakeFiles" / "TargetDirectories.txt"
            targets.parent.mkdir()
            for name in ("self_ipcs", "self_ipcs_mpmc_bench", "self_ipcs_buffer_pool_bench",
                         "self_ipcs_mpmc_wait_child", "self_ipcs_shared_buffer_pool_child"):
                canonical_name, legacy_name = artifact_names(name)
                targets.write_text(str(canonical / f"{legacy_name}.dir") + "\n")
                self.assertEqual(build_target(canonical, name), legacy_name)
                with targets.open("a") as output:
                    output.write(str(canonical / f"{canonical_name}.dir") + "\n")
                self.assertEqual(build_target(canonical, name), canonical_name)
            with self.assertRaises(ValueError):
                build_target(canonical, "self_ipcs_missing")

    def test_manifest_accepts_new_legacy_and_mixed_affinity_names(self):
        for legacy in (False, True):
            for kind, placement in (("spsc", "same"), ("spsc", "different"),
                                    ("mpmc", "same"), ("pool", "same")):
                with self.subTest(legacy=legacy, kind=kind, placement=placement):
                    with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
                        command, metadata = self.make_manifest(Path(directory), kind, legacy,
                                                               placement)
                        check_manifest(command, metadata)
                        if kind == "spsc":
                            samples = command.parent / "process-samples.jsonl"
                            observed = json.loads(samples.read_text())
                            observed["affinities"][-1]["cpus"] = [4]
                            samples.write_text(json.dumps(observed) + "\n")
                            with self.assertRaisesRegex(ValueError, "affinity snapshots"):
                                check_manifest(command, metadata)

    def schema2_affinity_samples(self, command):
        samples = json.loads((command.parent / "process-samples.jsonl").read_text())["affinities"]
        for pid, sample in enumerate(samples, 100):
            sample.update(schema=2, pid=pid, begin_ns=1000, end_ns=1100, starttime_ticks=10,
                          exe=str(Path(sample["argv"][0]).resolve()),
                          threads=[dict(tid=pid, cpus=list(sample["cpus"]))])
        return samples

    def test_manifest_retains_real_reaped_child_errors_with_all_four_roles(self):
        child = subprocess.Popen(["/bin/true"])
        self.assertEqual(child.wait(timeout=5), 0)
        self.assertFalse(Path(f"/proc/{child.pid}").exists(), "PID reused; do not guess")
        error = affinity_snapshot(child.pid)
        self.assertIn("error", error)
        self.assertNotIn("argv", error)
        self.assertNotIn("cpus", error)
        for legacy in (False, True):
            for placement in ("same", "different"):
                for schema in (1, 2):
                    with self.subTest(legacy=legacy, placement=placement, schema=schema):
                        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
                            command, metadata = self.make_manifest(Path(directory), "spsc", legacy, placement)
                            path = command.parent / "process-samples.jsonl"
                            if schema == 2:
                                path.write_text(json.dumps(dict(affinities=self.schema2_affinity_samples(command))) + "\n")
                            with path.open("a") as stream:
                                stream.write(json.dumps(dict(affinities=[error])) + "\n")
                            original = path.read_bytes()
                            check_manifest(command, metadata)
                            self.assertEqual(path.read_bytes(), original)

    def test_manifest_ignores_incomplete_extra_samples_without_counting_them(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            command, metadata = self.make_manifest(Path(directory), "spsc", False)
            path = command.parent / "process-samples.jsonl"
            valid = self.schema2_affinity_samples(command)
            invalid = [None, {}, dict(error="exited"), dict(schema=2),
                       dict(schema=2, argv=valid[0]["argv"]), dict(argv=123, cpus=[2]),
                       dict(argv=["self_ipcs_shm_bench_sock", "--role"], cpus=[2]),
                       dict(argv=["self_ipcs_shm_bench_sock", "--role", "unknown"], cpus=[2])]
            path.write_text(json.dumps(dict(affinities=valid + invalid)) + "\n")
            original = path.read_bytes()
            check_manifest(command, metadata)
            self.assertEqual(path.read_bytes(), original)

    def test_manifest_requires_each_transport_role_despite_partial_or_error_records(self):
        for legacy in (False, True):
            for placement in ("same", "different"):
                with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
                    command, metadata = self.make_manifest(Path(directory), "spsc", legacy, placement)
                    path = command.parent / "process-samples.jsonl"
                    valid = self.schema2_affinity_samples(command)
                    for ordinal, sample in enumerate(valid):
                        for replacement in (None, dict(sample, error="process exited"),
                                            dict(schema=2, argv=sample["argv"], cpus=sample["cpus"]),
                                            dict(sample, cpus=[999999])):
                            with self.subTest(legacy=legacy, placement=placement, role=ordinal,
                                              replacement=replacement):
                                changed = copy.deepcopy(valid)
                                changed[ordinal] = replacement
                                path.write_text(json.dumps(dict(affinities=changed)) + "\n")
                                with self.assertRaisesRegex(ValueError, "missing actual affinity snapshots.*unusable snapshots="):
                                    check_manifest(command, metadata)
                    path.write_text(json.dumps(dict(affinities=[dict(sample, error="unreadable") for sample in valid])) + "\n")
                    with self.assertRaisesRegex(ValueError, "missing actual affinity snapshots"):
                        check_manifest(command, metadata)

    def test_manifest_schema2_missing_fields_do_not_become_affinity_evidence(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            command, metadata = self.make_manifest(Path(directory), "spsc", False, "different")
            path = command.parent / "process-samples.jsonl"
            valid = self.schema2_affinity_samples(command)
            for field in ("argv", "cpus", "pid", "begin_ns", "end_ns", "starttime_ticks", "exe", "threads"):
                with self.subTest(field=field):
                    changed = copy.deepcopy(valid)
                    del changed[0][field]
                    path.write_text(json.dumps(dict(affinities=changed)) + "\n")
                    with self.assertRaisesRegex(ValueError, "missing actual affinity snapshots"):
                        check_manifest(command, metadata)

    def test_manifest_schema2_invalid_identity_threads_and_socket_role_are_rejected(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            command, metadata = self.make_manifest(Path(directory), "spsc", True, "different")
            path = command.parent / "process-samples.jsonl"
            valid = self.schema2_affinity_samples(command)
            invalid = [dict(schema=3), dict(schema=True), dict(pid=0), dict(starttime_ticks=True),
                       dict(begin_ns=1200), dict(exe="relative/path"), dict(exe="/another/binary"),
                       dict(threads=[]), dict(threads=[dict(tid=100, cpus=[3])]),
                       dict(threads=[dict(tid=101, cpus=[2])]),
                       dict(threads=[dict(tid=100, cpus=[2]), dict(tid=100, cpus=[2])])]
            for change in invalid:
                with self.subTest(change=change):
                    changed = copy.deepcopy(valid)
                    changed[0].update(change)
                    path.write_text(json.dumps(dict(affinities=changed)) + "\n")
                    with self.assertRaisesRegex(ValueError, "missing actual affinity snapshots"):
                        check_manifest(command, metadata)
            for argv in (["self_ipcs_shm_bench_sock", "--role"],
                         ["self_ipcs_shm_bench_sock", "--role", "unknown"]):
                changed = copy.deepcopy(valid)
                changed[-1] = dict(argv=argv, cpus=[3])
                path.write_text(json.dumps(dict(affinities=changed)) + "\n")
                with self.assertRaisesRegex(ValueError, "socket_self_ipcs_shm_bench_consumer"):
                    check_manifest(command, metadata)

    def test_installed_package_and_default_consumer_use_self_ipcs_archive(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            repo = root / "repo"
            prefix = root / "install"
            for relative in ("lib/libself_ipcs.a", "lib/cmake/SelfIPCs/SelfIPCsConfig.cmake",
                             "lib/cmake/SelfIPCs/SelfIPCsTargets.cmake",
                             "include/self_ipcs/queue/mpmc_queue.hpp",
                             "include/self_ipcs/buffer/shared_buffer_pool.hpp"):
                artifact = prefix / relative
                artifact.parent.mkdir(parents=True, exist_ok=True)
                artifact.write_text("installed metadata fixture\n")
            assert_install(prefix, repo)
            downstream = root / "downstream"
            link = downstream / "CMakeFiles" / "consume_demo.dir" / "link.txt"
            link.parent.mkdir(parents=True)
            (downstream / "compile_commands.json").write_text(
                json.dumps([{"command": f"g++ -I{prefix / 'include'} main.cpp"}]))
            link.write_text(f"g++ main.o {prefix / 'lib' / 'libself_ipcs.a'} -o consume_demo\n")
            assert_downstream_sources(downstream, prefix, repo)
            link.write_text(f"g++ main.o {prefix / 'lib' / 'libedge_runtime.a'} -o consume_demo\n")
            with self.assertRaisesRegex(AuditFailure, "libself_ipcs.a"):
                assert_downstream_sources(downstream, prefix, repo)
            (prefix / "include" / "self_ipcs" / "detail").mkdir()
            with self.assertRaisesRegex(AuditFailure, "internal detail headers"):
                assert_install(prefix, repo)

    def test_release_cache_accepts_either_namespace_or_matching_aliases(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            cache = root / "CMakeCache.txt"
            for prefixes in (("SELFIPCS",), ("EDGERUNTIME",), ("SELFIPCS", "EDGERUNTIME")):
                with self.subTest(prefixes=prefixes):
                    self.write_release_cache(cache, prefixes)
                    validate_release_cache(cache)
                    assert_release_configuration(root, root / "audit.log")
            for suffix in ("ENABLE_SANITIZERS", "ENABLE_FAILPOINTS"):
                self.write_release_cache(cache, ("SELFIPCS",))
                original = cache.read_text()
                for text in (original.replace(f"SELFIPCS_{suffix}:BOOL=OFF", ""),
                             original.replace(f"SELFIPCS_{suffix}:BOOL=OFF",
                                              f"SELFIPCS_{suffix}:BOOL=ON")):
                    cache.write_text(text)
                    with self.assertRaisesRegex(ValueError, "Release configuration"):
                        validate_release_cache(cache)

    def test_conflicting_cache_aliases_fail_runner_verifier_and_audit(self):
        for suffix in ("WARNINGS_AS_ERRORS", "ENABLE_SANITIZERS", "ENABLE_FAILPOINTS"):
            for canonical_value, legacy_value in (("ON", "OFF"), ("OFF", "ON")):
                with self.subTest(suffix=suffix, values=(canonical_value, legacy_value)):
                    with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
                        root = Path(directory)
                        command, metadata = self.make_manifest(root, "mpmc", False)
                        cache = command.parent.parent / "CMakeCache.txt"
                        self.write_release_cache(cache, ("SELFIPCS", "EDGERUNTIME"))
                        lines = [line for line in cache.read_text().splitlines()
                                 if not line.startswith((f"SELFIPCS_{suffix}:", f"EDGERUNTIME_{suffix}:"))]
                        lines.extend((f"SELFIPCS_{suffix}:BOOL={canonical_value}",
                                      f"EDGERUNTIME_{suffix}:BOOL={legacy_value}"))
                        cache.write_text("\n".join(lines) + "\n")
                        metadata["source"]["cache_sha256"] = digest(cache)
                        with self.assertRaisesRegex(ValueError, "conflicting cache flags"):
                            validate_release_cache(cache)
                        with self.assertRaisesRegex(ValueError, "conflicting cache flags"):
                            check_manifest(command, metadata)
                        with self.assertRaisesRegex(AuditFailure, "conflicting cache flags"):
                            assert_release_configuration(cache.parent, root / "audit.log")
                        argv = ["run_formal_bench.py", "--section", "mpmc", "--binary-dir",
                                str(cache.parent / "tools"), "--output-root", str(root)]
                        stderr = io.StringIO()
                        with patch("sys.argv", argv), patch("run_formal_bench.os.sched_getaffinity",
                                                            return_value={0, 1}), \
                                patch("run_formal_bench.run_logged_process") as workload, \
                                redirect_stderr(stderr):
                            with self.assertRaises(SystemExit) as stopped:
                                runner_main()
                        self.assertEqual(stopped.exception.code, 2)
                        self.assertIn("conflicting cache flags", stderr.getvalue())
                        workload.assert_not_called()
                        self.assertFalse((root / "audit.log").exists())

    def check_matrix(self, section, matrix, require_suite=False):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            (root / "planned.json").write_text(json.dumps({"section": section, "cases": matrix}))
            (root / "outcomes.json").write_text(json.dumps([{} for case in matrix]))
            for ordinal, case in enumerate(matrix, 1):
                command = root / f"case-{ordinal:02d}/command.json"
                command.parent.mkdir()
                command.write_text(json.dumps({"case": case}))
            return [error for check in check_plans(root, require_suite) for error in check["errors"]]

    def test_specification_accepts_real_matrix_and_local_sections(self):
        for section in ("spsc", "mpmc", "pool", "sustained"):
            with self.subTest(section=section):
                self.assertEqual(self.check_matrix(section, cases(section, 1600000)), [])
        for messages in (1, 123, 2000000):
            self.assertEqual(self.check_matrix("sustained", cases("sustained", messages)), [])

    def test_local_section_cannot_claim_suite(self):
        self.assertTrue(self.check_matrix("spsc", cases("spsc", 1), require_suite=True))
        self.assertTrue(self.check_matrix("unknown", cases("mpmc", 1)))

    def test_consistent_8k_replacement_fails(self):
        matrix = cases("mpmc", 1)
        for case in matrix:
            if case["payload"] == 4096:
                case["payload"] = 8192
        self.assertTrue(self.check_matrix("mpmc", matrix))

    def test_same_count_replacement_and_missing_supplement_fail(self):
        for section in ("spsc", "mpmc", "pool"):
            with self.subTest(section=section):
                matrix = cases(section, 1)
                matrix[-1] = copy.deepcopy(matrix[0])
                self.assertTrue(self.check_matrix(section, matrix))
        matrix = cases("mpmc", 1)
        for case in matrix[:3]:
            case["producers"] = 2
            case["consumers"] = 2
        self.assertTrue(self.check_matrix("mpmc", matrix))

    def test_duplicate_and_abnormal_repeat_fail(self):
        for repeat in (0, 4, True, "1"):
            matrix = cases("pool", 1)
            matrix[0]["repeat"] = repeat
            self.assertTrue(self.check_matrix("pool", matrix))
        matrix = cases("mpmc", 1)
        matrix[1] = copy.deepcopy(matrix[0])
        self.assertTrue(self.check_matrix("mpmc", matrix))

    def test_sustained_bounds_and_configuration_fail(self):
        for messages in (0, 2000001, True, "123"):
            self.assertTrue(self.check_matrix("sustained", cases("sustained", messages)))
        matrix = cases("sustained", 123)
        matrix[0]["consumers"] = 4
        self.assertTrue(self.check_matrix("sustained", matrix))

    def test_finite_matrix(self):
        self.assertEqual(len(cases("spsc", 1)), 6)
        matrix = cases("mpmc", 1)
        self.assertEqual(len(matrix), 26)
        self.assertTrue(all(case["messages"] == 20000 for case in matrix))
        self.assertEqual(len(cases("pool", 1)), 13)
        self.assertEqual(cases("sustained", 123)[0]["messages"], 123)
        self.assertEqual(message_budget(cases("mpmc", 1))["total_messages"], 3100000)
        self.assertEqual(message_budget(cases("pool", 1))["total_messages"], 2408000)
        with self.assertRaises(ValueError):
            message_budget(cases("sustained", 2000001))

    def test_missing_planned_cases_fail(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            root = Path(directory)
            (root / "planned.json").write_text(json.dumps({"section": "spsc", "cases": cases("spsc", 1)}))
            checks = check_plans(root)
            self.assertTrue(checks[0]["errors"])

    def test_nearest_rank(self):
        self.assertEqual(quantiles([100, 1, 50, 2]),
                         {"p50_ns": 2, "p95_ns": 100, "p99_ns": 100, "max_ns": 100})
        with self.assertRaises(ValueError):
            quantiles([])

    def test_sha256_on_python310(self):
        with tempfile.TemporaryDirectory(dir=OUTPUT_ROOT) as directory:
            path = Path(directory) / "input"
            path.write_bytes(b"formal benchmark")
            self.assertEqual(digest(path), hashlib.sha256(b"formal benchmark").hexdigest())

    def test_worker_retention_and_independent_negative_checks(self):
        if BINARY_DIR is None:
            self.skipTest("--binary-dir enables native fork/exec coverage")
        root = Path(tempfile.mkdtemp(prefix="selfipcs-formal-regression-", dir=OUTPUT_ROOT))
        for kind, tool, arguments in (
                ("mpmc", "self_ipcs_mpmc_bench", ["--payload-size", "64"]),
                ("pool", "self_ipcs_buffer_pool_bench", ["--block-size", "4096", "--block-count", "4"])):
            case_dir = root / kind
            case_dir.mkdir()
            output = case_dir / "output"
            argv = [str(resolve_binary(BINARY_DIR, tool)), *arguments, "--producers", "2", "--consumers", "2",
                    "--messages", "32", "--capacity", "2", "--timeout-ms", "5000",
                    "--out-dir", str(output), "--keep-worker-reports"]
            result = run_logged_process(argv, cwd=case_dir, env=os.environ.copy(),
                                        stdout_path=case_dir / "stdout.raw",
                                        stderr_path=case_dir / "stderr.raw",
                                        sample_path=case_dir / "process-samples.jsonl", timeout_s=15)
            (case_dir / "exit.json").write_text(json.dumps(asdict(result)))
            case = {"kind": kind, "payload": 64 if kind == "mpmc" else 4096,
                    "producers": 2, "consumers": 2, "messages": 32, "blocks": 4,
                    "wait": "busy", "capacity": 2}
            verified = check_delivery(case_dir, case)
            self.assertTrue(verified["valid"])
            self.assertEqual(len(verified["workers"]), 4)
            original = Path(verified["csv"]).read_text()
            lines = original.splitlines()
            lines[2] = lines[1]
            Path(verified["csv"]).write_text("\n".join(lines) + "\n")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                check_delivery(case_dir, case)
            Path(verified["csv"]).write_text(original)
            worker = Path(verified["workers"][0]["path"])
            original_worker = worker.read_bytes()
            worker.write_bytes(original_worker[:-1])
            with self.assertRaisesRegex(ValueError, "invalid worker report"):
                check_delivery(case_dir, case)
            worker.write_bytes(original_worker)
            wrong_case = dict(case, payload=16384 if kind == "pool" else 4096)
            with self.assertRaisesRegex(ValueError, "configuration"):
                check_delivery(case_dir, wrong_case)
        print(f"native regression evidence={root}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary-dir", type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    args, remaining = parser.parse_known_args()
    BINARY_DIR = args.binary_dir.resolve() if args.binary_dir else None
    OUTPUT_ROOT = args.output_root.resolve()
    unittest.main(argv=[__file__, *remaining])
