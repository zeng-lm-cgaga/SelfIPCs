#!/usr/bin/env python3
"""Small deterministic verifier negatives; optional genuine two-run IPC smoke.

CTest integration (inside the existing Python3_Interpreter_FOUND block):
  add_test(NAME notification_matrix_regression
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/test_notification_matrix.py
      --binary-dir $<TARGET_FILE_DIR:edge_shm_bench> --output-root ${CMAKE_BINARY_DIR})
  set_tests_properties(notification_matrix_regression PROPERTIES TIMEOUT 60
    LABELS benchmark ENVIRONMENT PYTHONDONTWRITEBYTECODE=1)
The driver, producer, consumer and ctl targets must be built first.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import pressure_process
import run_notification_matrix as runner
import verify_notification_matrix as verifier


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value) + "\n")


def fixture(directory: Path, *, jump: bool = False, initial_jump: bool = False,
            backend: str = "futex", backend_identity: bool = False) -> dict:
    case = verifier.normative_cases(True, (backend,))[0]
    all_sequences = list(range(1, 51))
    sequences = [value for value in all_sequences if not (jump and value == 10)
                 and not (initial_jump and value == 1)]
    rows = []
    previous = 0
    for sequence in sequences:
        publish = 1_000_000_000 + (sequence - 1) * 10_000_000
        rows.append([sequence, 1, 64, publish, publish + 20_000, 20_000,
                     sequence - previous - 1 if previous else 0, 600, 600])
        previous = sequence
    with (directory / "samples.csv").open("w") as output:
        writer = csv.writer(output)
        writer.writerow(verifier.FIELDS)
        writer.writerows(rows)
    with (directory / "samples.raw.csv").open("w") as output:
        csv.writer(output).writerows(row[:7] for row in rows)
    with (directory / "publications.csv").open("w") as output:
        writer = csv.writer(output)
        writer.writerow(["attempt", "sequence", "publish_ns", "accepted"])
        writer.writerows([sequence, sequence, 1_000_000_000 + (sequence - 1) * 10_000_000, 1]
                         for sequence in all_sequences)
    argv = {}
    missed = sum(row[6] for row in rows)
    for role, pid in (("producer", 100), ("consumer", 200)):
        argv[role] = ["taskset", "-c", "0", f"/fixture/{role}", "--name", "fixture",
                      "--payload", "64", "--notification", backend, "--rate", "100",
                      "--max-time-ms", "2000", "--checksum", "1", "--ready-fd", "3"]
        argv[role] += ["--samples", "50"] if role == "producer" else [
            "--max-samples", "50", "--final-sequence", "50", "--mode", "futex"]
        begin = dict(role=role, pid=pid, monotonic_raw_ns=999_000_000,
                     realtime_ns=1_700_000_000_999_000_000, user_us=100, system_us=100,
                     voluntary_csw=1, involuntary_csw=0, minor_faults=1, major_faults=0,
                     maxrss_kb=100, input_blocks=0, output_blocks=0, signals=0,
                     swaps=0, messages_sent=0, messages_received=0)
        end = dict(begin, monotonic_raw_ns=1_500_000_000, realtime_ns=1_700_000_001_500_000_000,
                   user_us=200, system_us=200, voluntary_csw=51)
        metrics = dict(version=1, role=role, pid=pid, notification=backend, payload_bytes=64,
                       target=50, rate_hz=100, clock="CLOCK_MONOTONIC_RAW", rusage_scope="RUSAGE_SELF",
                       valid=True, begin_ns=begin["monotonic_raw_ns"], end_ns=end["monotonic_raw_ns"],
                       begin_realtime_ns=begin["realtime_ns"], end_realtime_ns=end["realtime_ns"],
                       user_us=100, system_us=100, voluntary_csw=50, involuntary_csw=0,
                       attempted=50 if role == "producer" else 0, accepted=50 if role == "producer" else 0,
                       consumed=len(rows) if role == "consumer" else 0,
                       unique_consumed=len(rows) if role == "consumer" else 0,
                       final_sequence=50, missed=missed if role == "consumer" else 0,
                       duplicate=0, torn=0, timeout=0, wait_timeouts=0,
                       thread_count_begin=1, thread_count_end=1,
                       process_allowed_cpus_begin=[0], process_allowed_cpus_end=[0],
                       threads_begin=[dict(tid=pid, cpus=[0])], threads_end=[dict(tid=pid, cpus=[0])])
        metrics.update({key: 0 for key in verifier.EXTRA_USAGE})
        if role == "producer" and backend in ("eventfd", "driver"):
            for phase in ("begin", "end"):
                metrics[f"thread_count_{phase}"] = 2
                metrics[f"threads_{phase}"] = [dict(tid=pid, cpus=[0]), dict(tid=pid + 1, cpus=[0])]
        metrics["process_maxrss_kb_at_end"] = 100
        lifecycle = dict(scope="wait4_process_lifecycle", exit_code=0, signal=0, timed_out=False,
                         exec_ok=True, wall_us=600000, user_us=300, system_us=300,
                         voluntary_csw=52, involuntary_csw=0, minor_faults=2, major_faults=0,
                         maxrss_kb=100, input_blocks=0, output_blocks=0, signals=0,
                         swaps=0, messages_sent=0, messages_received=0)
        raw = "WORK_BEGIN " + json.dumps(begin) + "\nWORK_END " + json.dumps(end)
        if backend_identity:
            raw += f"\nNOTIFICATION_BACKEND requested={backend} actual={backend}"
        raw += "\nWORK_METRICS " + json.dumps(metrics) + "\n"
        raw += "DONE published=50 cpu_us=200\n" if role == "producer" else (
            f"SUMMARY reads={len(rows)} torn=0 missed_total={missed} last_seq=50 "
            "elapsed_ms=501 cpu_us=200 ended=samples\n")
        (directory / f"{role}.raw").write_text(raw)
        write_json(directory / f"{role}.lifecycle.json", lifecycle)
    write_json(directory / "workers.argv.json", argv)
    write_json(directory / "owned-resources.json", {"channel": "fixture", "paths": []})
    latencies = [row[5] for row in rows[5:]]
    summary = dict(run_id=case["run_id"], transport="shm", notification=backend, mode=backend,
                   payload_bytes=64, placement="same", rate_hz=100, count=len(rows), warmup_count=5,
                   measured_count=len(latencies), missed_samples=missed, torn_reads=0,
                   latency_ns=dict(verifier.quantiles(latencies), min=min(latencies), max=max(latencies),
                                   mean=statistics.mean(latencies), stddev=statistics.pstdev(latencies)),
                   producer_cpu_ms=0.6, consumer_cpu_ms=0.6,
                   latency_comparison=dict(eligible=not (jump or initial_jump), queue_overlap_count=0))
    write_json(directory / "summary.json", summary)
    return case


class NotificationVerifierTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="notification-verifier-fixture-")
        self.root = Path(self.temporary.name)
        self.case = fixture(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def verify(self):
        return verifier.verify_run(self.root, self.case, 0, 1)

    def test_valid_fixture_and_nearest_rank(self):
        self.assertTrue(self.verify()["eligible"])
        self.assertEqual(verifier.quantiles(list(range(1, 11))), dict(p50=5, p95=10, p99=10))

    def test_plan_is_independently_normative_and_alternates(self):
        self.assertEqual(runner.cases(), verifier.normative_cases(False))
        self.assertEqual(len(runner.cases()), 24)
        self.assertEqual(runner.RULES, verifier.RULES)
        self.assertEqual(runner.cases()[8]["notification"], "eventfd")
        self.assertEqual(runner.cases()[16]["notification"], "futex")

    def test_three_backend_plan_and_explicit_subset(self):
        for smoke in (False, True):
            for backends in (("futex", "eventfd", "driver"), ("driver",), ("futex", "driver")):
                self.assertEqual(runner.cases(smoke, backends), verifier.normative_cases(smoke, backends))
        planned = runner.cases(False, ("futex", "eventfd", "driver"))
        self.assertEqual(len(planned), 36)
        self.assertEqual([case["notification"] for case in planned[:3]], ["futex", "eventfd", "driver"])
        self.assertEqual([case["notification"] for case in planned[12:15]], ["driver", "eventfd", "futex"])

    def test_backend_selectors_reject_empty_unknown_duplicate(self):
        for value in ("", "eventfd,driver,driver", "driver,unknown", "futex,"):
            with self.assertRaises(argparse.ArgumentTypeError):
                runner.parse_backends(value)
            with self.assertRaisesRegex(ValueError, "invalid backend selection"):
                verifier.normative_backends(value.split(","))
        self.assertEqual(runner.parse_backends("driver,futex"), ("futex", "driver"))

    def test_driver_plan_cannot_substitute_eventfd(self):
        planned = verifier.normative_cases(False, ("futex", "eventfd", "driver"))
        planned[2] = dict(planned[1])
        write_json(self.root / "plan.json", dict(smoke=False, schema=2,
                   backends=["futex", "eventfd", "driver"], cases=planned))
        write_json(self.root / "manifest.json", {})
        with self.assertRaisesRegex(ValueError, "normative matrix config mismatch"):
            verifier.verify(self.root)

    def test_explicit_scope_rejects_default_pair_as_three_backend(self):
        write_json(self.root / "plan.json", dict(smoke=False, schema=1))
        write_json(self.root / "manifest.json", {})
        with self.assertRaisesRegex(ValueError, "requested backend scope mismatch"):
            verifier.verify(self.root, expected_backends=("futex", "eventfd", "driver"))

    def test_driver_requires_actual_backend_identity(self):
        case = fixture(self.root, backend="driver")
        with self.assertRaisesRegex(ValueError, "actual backend mismatch or missing"):
            verifier.verify_run(self.root, case, 0, 1)
        case = fixture(self.root, backend="driver", backend_identity=True)
        self.assertTrue(verifier.verify_run(self.root, case, 0, 1)["eligible"])
        path = self.root / "consumer.raw"
        path.write_text(path.read_text().replace("actual=driver", "actual=eventfd"))
        with self.assertRaisesRegex(ValueError, "actual backend mismatch or missing"):
            verifier.verify_run(self.root, case, 0, 1)

    def test_new_plan_requires_identity_but_legacy_raw_is_supported(self):
        self.assertTrue(self.verify()["eligible"])
        with self.assertRaisesRegex(ValueError, "actual backend mismatch or missing"):
            verifier.verify_run(self.root, self.case, 0, 1, require_backend_identity=True)
        case = fixture(self.root, backend_identity=True)
        self.assertTrue(verifier.verify_run(self.root, case, 0, 1, require_backend_identity=True)["eligible"])

    def test_driver_service_thread_missing_rejected(self):
        case = fixture(self.root, backend="driver", backend_identity=True)
        path = self.root / "producer.raw"
        text = path.read_text()
        metrics = verifier.one_record(text, "WORK_METRICS")
        changed = copy.deepcopy(metrics)
        for phase in ("begin", "end"):
            changed[f"threads_{phase}"] = changed[f"threads_{phase}"][:1]
            changed[f"thread_count_{phase}"] = 1
        path.write_text(text.replace("WORK_METRICS " + json.dumps(metrics),
                                     "WORK_METRICS " + json.dumps(changed)))
        with self.assertRaisesRegex(ValueError, "service thread unaccounted"):
            verifier.verify_run(self.root, case, 0, 1)

    def test_modified_metrics_rejected(self):
        path = self.root / "producer.raw"
        text = path.read_text()
        metrics = verifier.one_record(text, "WORK_METRICS")
        modified = dict(metrics, user_us=metrics["user_us"] + 1)
        path.write_text(text.replace("WORK_METRICS " + json.dumps(metrics),
                                     "WORK_METRICS " + json.dumps(modified)))
        with self.assertRaisesRegex(ValueError, "modified work metrics"):
            self.verify()

    def test_modified_quantile_rejected(self):
        path = self.root / "summary.json"
        summary = json.loads(path.read_text())
        summary["latency_ns"]["p99"] += 1
        write_json(path, summary)
        with self.assertRaisesRegex(ValueError, "quantile mismatch"):
            self.verify()

    def test_modified_config_rejected(self):
        case = copy.deepcopy(self.case)
        case["rate_hz"] = 1000
        with self.assertRaisesRegex(ValueError, "config mismatch"):
            verifier.verify_run(self.root, case, 0, 1)

    def test_modified_or_duplicated_plan_rejected_without_trusting_digest(self):
        for duplicate in (False, True):
            plan = dict(smoke=False, schema=1, cases=verifier.normative_cases(False))
            if duplicate:
                plan["cases"][1] = dict(plan["cases"][0])
            else:
                plan["cases"][0]["samples"] = 999
            write_json(self.root / "plan.json", plan)
            write_json(self.root / "manifest.json", {})
            with self.assertRaisesRegex(ValueError, "normative matrix config mismatch"):
                verifier.verify(self.root)

    def test_reordered_pair_plan_rejected(self):
        planned = verifier.normative_cases(False)
        planned[8], planned[9] = planned[9], planned[8]
        write_json(self.root / "plan.json", dict(smoke=False, schema=1, cases=planned))
        write_json(self.root / "manifest.json", {})
        with self.assertRaisesRegex(ValueError, "normative matrix config mismatch"):
            verifier.verify(self.root)

    def test_duplicate_in_both_csvs_rejected(self):
        final = self.root / "samples.csv"
        lines = final.read_text().splitlines()
        lines[11] = lines[10]
        final.write_text("\n".join(lines) + "\n")
        raw = self.root / "samples.raw.csv"
        lines = raw.read_text().splitlines()
        lines[10] = lines[9]
        raw.write_text("\n".join(lines) + "\n")
        with self.assertRaisesRegex(ValueError, "duplicate or reordered"):
            self.verify()

    def test_latest_jump_is_intact_but_ineligible(self):
        fixture(self.root, jump=True)
        result = self.verify()
        self.assertTrue(result["integrity"])
        self.assertFalse(result["eligible"])
        self.assertEqual(result["gaps"], 1)

    def test_initial_jump_is_not_library_missed_but_disqualifies(self):
        fixture(self.root, initial_jump=True)
        result = self.verify()
        self.assertTrue(result["integrity"])
        self.assertFalse(result["eligible"])
        self.assertEqual(result["gaps"], 1)
        self.assertEqual(result["missed_samples"], 0)

    def test_service_thread_missing_rejected(self):
        case = dict(self.case, notification="eventfd")
        for role in ("producer", "consumer"):
            path = self.root / f"{role}.raw"
            path.write_text(path.read_text().replace('"notification": "futex"', '"notification": "eventfd"'))
        with self.assertRaisesRegex(ValueError, "service thread unaccounted"):
            verifier.verify_run(self.root, case, 0, 1)

    def test_reported_torn_is_integrity_failure_not_discarded(self):
        path = self.root / "consumer.raw"
        text = path.read_text()
        metrics = verifier.one_record(text, "WORK_METRICS")
        modified = dict(metrics, torn=1)
        path.write_text(text.replace("WORK_METRICS " + json.dumps(metrics),
                                     "WORK_METRICS " + json.dumps(modified)).replace("torn=0", "torn=1"))
        summary = json.loads((self.root / "summary.json").read_text())
        summary["torn_reads"] = 1
        write_json(self.root / "summary.json", summary)
        result = self.verify()
        self.assertFalse(result["integrity"])
        self.assertFalse(result["eligible"])
        self.assertEqual(result["torn"], 1)


class RunnerAffinityTests(unittest.TestCase):
    def setUp(self):
        self.worker = dict(pid=100, begin_ns=1000, end_ns=2000)
        self.argv = ["taskset", "-c", "0", "/fixture/producer", "--notification", "eventfd"]
        self.observation = dict(schema=2, pid=100, begin_ns=1200, end_ns=1300,
                                starttime_ticks=10, argv=self.argv[3:], exe=self.argv[3],
                                cpus=[0], threads=[dict(tid=100, cpus=[0]), dict(tid=101, cpus=[0])])

    def verify(self, observations, *, schema=2):
        return verifier.verify_runner_affinity([dict(affinities=observations)], self.worker, 0,
                                               self.argv, schema=schema, minimum_threads=2)

    def test_startup_teardown_and_boundary_are_not_work(self):
        startup = dict(self.observation, begin_ns=500, end_ns=600, cpus=[0, 1], argv=["taskset"])
        teardown = dict(self.observation, begin_ns=2100, end_ns=2200, error="process exited")
        boundary = dict(self.observation, begin_ns=900, end_ns=1100, cpus=[0, 1])
        self.assertEqual(self.verify([startup, self.observation, teardown, boundary]),
                         dict(schema=2, startup=1, work=1, teardown=1, boundary=1))
        legacy = [{key: value for key, value in item.items() if key != "schema"}
                  for item in (startup, self.observation)]
        with self.assertRaisesRegex(ValueError, "runner /proc process affinity mismatch"):
            self.verify(legacy, schema=1)

    def test_new_samples_cannot_omit_schema_declaration(self):
        with self.assertRaisesRegex(ValueError, "missing affinity sample schema declaration"):
            self.verify([self.observation], schema=1)

    def test_missing_or_boundary_only_work_is_unverifiable(self):
        for observations in ([], [dict(self.observation, pid=999)],
                             [dict(self.observation, begin_ns=900, end_ns=1100)],
                             [dict(self.observation, begin_ns=1900, end_ns=2100)]):
            with self.subTest(observations=observations):
                with self.assertRaisesRegex(ValueError, "no verifiable in-work affinity observation"):
                    self.verify(observations)

    def test_real_work_process_mismatch_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "work process affinity mismatch"):
            self.verify([self.observation, dict(self.observation, cpus=[0, 1])])

    def test_real_service_thread_mismatch_or_missing_is_rejected(self):
        changed = dict(self.observation, threads=[dict(tid=100, cpus=[0]), dict(tid=101, cpus=[1])])
        with self.assertRaisesRegex(ValueError, "work thread affinity mismatch"):
            self.verify([changed])
        with self.assertRaisesRegex(ValueError, "work thread set incomplete"):
            self.verify([dict(self.observation, threads=[dict(tid=100, cpus=[0])])])

    def test_identity_exec_schema_and_unreadable_work_are_rejected(self):
        changes = [(dict(starttime_ticks=11), "process identity changed"),
                   (dict(exe="/fixture/other"), "worker executable/argv mismatch"),
                   (dict(argv=["taskset"]), "worker executable/argv mismatch"),
                   (dict(schema=1), "affinity sample schema mismatch"),
                   (dict(error="PermissionError"), "work affinity unverifiable"),
                   (dict(begin_ns=1400, end_ns=1300), "invalid affinity sample interval")]
        for change, message in changes:
            with self.subTest(change=change):
                with self.assertRaisesRegex(ValueError, message):
                    self.verify([self.observation, dict(self.observation, **change)])

    def test_snapshot_preserves_unreadable_error_and_interval(self):
        with mock.patch.object(pressure_process.os, "sched_getaffinity", side_effect=ProcessLookupError()):
            observation = pressure_process.affinity_snapshot(os.getpid())
        self.assertIn("ProcessLookupError", observation["error"])
        self.assertLessEqual(observation["begin_ns"], observation["end_ns"])
        self.assertEqual(observation["pid"], os.getpid())

    def test_real_fork_exec_startup_and_work_gates(self):
        cpu = min(os.sched_getaffinity(0))
        work = '''import json, os, signal, time
print('WORK_BEGIN ' + json.dumps(dict(pid=os.getpid(), begin_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW))), flush=True)
os.kill(os.getpid(), signal.SIGSTOP)
print('WORK_END ' + json.dumps(dict(end_ns=time.clock_gettime_ns(time.CLOCK_MONOTONIC_RAW))), flush=True)
'''
        work_argv = [sys.executable, "-c", work]
        startup = ("import os, signal; os.kill(os.getpid(), signal.SIGSTOP); "
                   + "os.execv('/usr/bin/taskset', " + repr(["taskset", "-c", str(cpu), *work_argv]) + ")")
        driver = ("import subprocess; raise SystemExit(subprocess.call("
                  + repr([sys.executable, "-c", startup]) + "))")
        released = set()
        snapshot = pressure_process.affinity_snapshot

        def capture_and_release(pid):
            observation = snapshot(pid)
            stage = "work" if observation.get("argv") == work_argv else "startup"
            if stage not in released and 'State:\tT' in Path(f"/proc/{pid}/status").read_text():
                released.add(stage)
                os.kill(pid, signal.SIGCONT)
            return observation

        with tempfile.TemporaryDirectory(prefix="notification-affinity-gates-") as temporary:
            root = Path(temporary)
            with mock.patch.object(pressure_process, "affinity_snapshot", side_effect=capture_and_release):
                result = pressure_process.run_logged_process(
                    [sys.executable, "-c", driver], cwd=root, env=os.environ.copy(),
                    stdout_path=root / "stdout", stderr_path=root / "stderr",
                    sample_path=root / "samples.jsonl", timeout_s=5, term_grace_s=1, capture_affinity=True)
            self.assertEqual(result.returncode, 0)
            self.assertFalse(result.timed_out or result.orphan_group_reaped)
            self.assertEqual(released, {"startup", "work"})
            begin = verifier.one_record((root / "stdout").read_text(), "WORK_BEGIN")
            end = verifier.one_record((root / "stdout").read_text(), "WORK_END")
            worker = dict(begin, **end)
            samples = [json.loads(line) for line in (root / "samples.jsonl").read_text().splitlines()]
            counts = verifier.verify_runner_affinity(samples, worker, cpu, ["taskset", "-c", str(cpu), *work_argv], schema=2)
            self.assertGreaterEqual(counts["startup"], 1)
            self.assertGreaterEqual(counts["work"], 1)
            if len(os.sched_getaffinity(0)) > 1:
                legacy = [dict(sample, affinities=[{key: value for key, value in item.items() if key != "schema"}
                                                  for item in sample.get("affinities", [])]) for sample in samples]
                with self.assertRaisesRegex(ValueError, "runner /proc process affinity mismatch"):
                    verifier.verify_runner_affinity(legacy, worker, cpu,
                                                    ["taskset", "-c", str(cpu), *work_argv], schema=1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary-dir", type=Path)
    parser.add_argument("--output-root", type=Path, default=Path("/tmp"))
    args = parser.parse_args()
    suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(test_case)
                               for test_case in (NotificationVerifierTests, RunnerAffinityTests))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    if args.binary_dir:
        allowed = sorted(os.sched_getaffinity(0))
        if len(allowed) < 2:
            parser.error("genuine smoke needs two allowed CPUs")
        command = [sys.executable, str(Path(runner.__file__).resolve()), "--smoke",
                   "--binary-dir", str(args.binary_dir.resolve()), "--output-root", str(args.output_root.resolve()),
                   "--cpu-a", str(allowed[0]), "--cpu-b", str(allowed[1])]
        completed = subprocess.run(command, text=True, capture_output=True, timeout=50)
        print(completed.stdout, end="")
        print(completed.stderr, end="", file=sys.stderr)
        if completed.returncode:
            return completed.returncode
        evidence = Path(next(line.partition("=")[2] for line in completed.stdout.splitlines()
                             if line.startswith("evidence=")))
        verified = verifier.verify(evidence, allow_smoke=True)
        write_json(evidence / "verified.json", verified)
        if not verified["integrity"] or not verified["all_eligible"]:
            print(json.dumps(verified, indent=2))
            return 1
        print(f"PASS genuine fork/exec futex/eventfd smoke; evidence={evidence}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
