#!/usr/bin/env python3
"""Independent notification evidence verifier (no runner imports).

The normative matrix/rules below are intentionally independent of plan.json and
summary.json. Latest-value jumps are valid integrity, but disqualify comparison.
Failed/ineligible runs stay in the report. Exit zero means intact evidence, not
qualified performance; use --require-eligible for that stronger assertion.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re
import statistics


RULES = {
    "version": 1, "accepted_publications": 1000, "warmup_observations": 100,
    "minimum_postwarm_observations": 900, "maximum_gaps": 0,
    "maximum_duplicates": 0, "maximum_torn": 0, "maximum_timeouts": 0,
    "require_final_sequence_equals_accepted": True, "maximum_queue_overlap": 0,
    "integrity_allows_latest_value_jumps": True, "quantile": "nearest-rank",
    "overlap": "postwarm receive > next accepted publication timestamp",
    "cpu_percent": "100 * (work user_us + system_us) * 1000 / (end_ns - begin_ns)",
    "window": "CLOCK_MONOTONIC_RAW; RUSAGE_SELF all process threads; includes pacing and CSV",
    "lifecycle": "wait4 includes startup, work and teardown; never use as work CPU percent",
}
FIELDS = ["sequence", "generation", "payload_bytes", "publish_ns", "receive_ns", "latency_ns",
          "missed_samples", "producer_cpu", "consumer_cpu"]
USAGE = ("user_us", "system_us", "voluntary_csw", "involuntary_csw")
EXTRA_USAGE = ("minor_faults", "major_faults", "input_blocks", "output_blocks", "signals",
               "swaps", "messages_sent", "messages_received")


def normative_backends(value) -> tuple[str, ...]:
    require(isinstance(value, list) and bool(value), "invalid backend selection")
    require(all(isinstance(item, str) for item in value), "invalid backend selection")
    require(value == [name for name in ("futex", "eventfd", "driver") if name in value],
            "invalid backend selection")
    return tuple(value)


def normative_cases(smoke: bool, backends: tuple[str, ...] = ("futex", "eventfd")) -> list[dict]:
    result = []
    for repeat in range(1, 2 if smoke else 4):
        for rate in ((100,) if smoke else (100, 1000)):
            for placement in (("same",) if smoke else ("same", "different")):
                for mode in (backends if repeat != 2 else backends[::-1]):
                    prefix = "notification-smoke" if smoke else "notification"
                    result.append(dict(repeat=repeat, rate_hz=rate, placement=placement,
                                       notification=mode, payload_bytes=64, producers=1, consumers=1,
                                       samples=50 if smoke else 1000, warmup=5 if smoke else 100,
                                       max_time_ms=2000 if smoke else 15000,
                                       run_id=f"{prefix}-shm-{mode}-64B-{placement}-{rate}"))
    return result


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def load(path: Path):
    return json.loads(path.read_text())


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def one_record(text: str, marker: str) -> dict:
    records = [json.loads(line[len(marker) + 1:]) for line in text.splitlines()
               if line.startswith(marker + " ")]
    require(len(records) == 1, f"expected exactly one {marker}")
    return records[0]


def argument(argv: list[str], name: str) -> str:
    require(argv.count(name) == 1, f"missing/duplicate argument {name}")
    return argv[argv.index(name) + 1]


def quantiles(values: list[int]) -> dict:
    ordered = sorted(values)
    return {f"p{percent}": ordered[(len(ordered) * percent + 99) // 100 - 1] if ordered else 0
            for percent in (50, 95, 99)}


def verify_worker(directory: Path, role: str, case: dict, cpu: int, argv: list[str],
                  *, require_backend_identity: bool = False) -> dict:
    text = (directory / f"{role}.raw").read_text()
    backend_records = [line for line in text.splitlines() if line.startswith("NOTIFICATION_BACKEND ")]
    if require_backend_identity or case["notification"] == "driver" or backend_records:
        require(backend_records == [f"NOTIFICATION_BACKEND requested={case['notification']} "
                                    f"actual={case['notification']}"], "actual backend mismatch or missing")
    metrics = one_record(text, "WORK_METRICS")
    begin, end = one_record(text, "WORK_BEGIN"), one_record(text, "WORK_END")
    lifecycle = load(directory / f"{role}.lifecycle.json")
    require(metrics["version"] == 1 and metrics["valid"] is True, "invalid worker metric version/rusage")
    require(metrics["role"] == begin["role"] == end["role"] == role, "worker role mismatch")
    require(metrics["pid"] == begin["pid"] == end["pid"] > 0, "worker PID mismatch")
    for key, expected in (("notification", case["notification"]), ("target", case["samples"]),
                          ("payload_bytes", 64), ("rate_hz", case["rate_hz"]),
                          ("clock", "CLOCK_MONOTONIC_RAW"), ("rusage_scope", "RUSAGE_SELF")):
        require(metrics[key] == expected, f"worker config mismatch: {key}")
    for phase, marker in (("begin", begin), ("end", end)):
        require(metrics[f"{phase}_ns"] == marker["monotonic_raw_ns"], "raw marker mismatch")
        require(metrics[f"{phase}_realtime_ns"] == marker["realtime_ns"] > 0, "realtime marker mismatch")
        threads = metrics[f"threads_{phase}"]
        require(metrics[f"process_allowed_cpus_{phase}"] == [cpu], "process actual affinity mismatch")
        require(metrics[f"thread_count_{phase}"] == len(threads) >= 1, "native thread count mismatch")
        require(len({thread["tid"] for thread in threads}) == len(threads), "duplicate TID")
        require(metrics["pid"] in {thread["tid"] for thread in threads}, "missing main thread")
        require(all(thread["cpus"] == [cpu] for thread in threads), "thread actual affinity mismatch")
        if role == "producer" and case["notification"] in ("eventfd", "driver"):
            require(len(threads) >= 2, "notification producer service thread unaccounted")
    duration = end["monotonic_raw_ns"] - begin["monotonic_raw_ns"]
    require(duration > 0 and end["realtime_ns"] >= begin["realtime_ns"], "invalid work interval")
    for key in USAGE + EXTRA_USAGE:
        require(metrics[key] == end[key] - begin[key] >= 0, f"modified work metrics: {key}")
        require(lifecycle[key] >= end[key], f"lifecycle excludes work: {key}")
    require(lifecycle["maxrss_kb"] >= end["maxrss_kb"] >= begin["maxrss_kb"], "invalid RSS")
    require(metrics["process_maxrss_kb_at_end"] == end["maxrss_kb"], "modified peak RSS metric")
    require(lifecycle["scope"] == "wait4_process_lifecycle", "wrong lifecycle scope")
    require(argv[:2] == ["taskset", "-c"] and argv[2] == str(cpu), "worker affinity argv mismatch")
    for option, expected in (("--payload", "64"), ("--notification", case["notification"]),
                             ("--max-time-ms", str(case["max_time_ms"])), ("--checksum", "1"),
                             ("--rate", str(case["rate_hz"]))):
        require(argument(argv, option) == expected, f"worker argument mismatch: {option}")
    require(int(argument(argv, "--ready-fd")) >= 0, "missing real startup gate")
    count_option = "--samples" if role == "producer" else "--max-samples"
    require(argument(argv, count_option) == str(case["samples"]), "worker sample config mismatch")
    if role == "consumer":
        require(argument(argv, "--final-sequence") == str(case["samples"]), "missing finite completion")
        require(argument(argv, "--mode") == "futex", "not blocking notification mode")
    require(lifecycle["wall_us"] * 1000 >= duration, "lifecycle shorter than work")
    metrics["cpu_percent"] = 100000 * (metrics["user_us"] + metrics["system_us"]) / duration
    return dict(metrics=metrics, lifecycle=lifecycle, raw=text)


def verify_run(directory: Path, case: dict, cpu_a: int, cpu_b: int,
               *, require_backend_identity: bool = False) -> dict:
    workers = load(directory / "workers.argv.json")
    producer = verify_worker(directory, "producer", case, cpu_a, workers["producer"],
                             require_backend_identity=require_backend_identity)
    consumer = verify_worker(directory, "consumer", case,
                             cpu_a if case["placement"] == "same" else cpu_b, workers["consumer"],
                             require_backend_identity=require_backend_identity)
    published, consumed = producer["metrics"], consumer["metrics"]
    require(published["pid"] != consumed["pid"], "workers are not separate processes")
    owned = load(directory / "owned-resources.json")
    require(argument(workers["producer"], "--name") == argument(workers["consumer"], "--name")
            == owned["channel"], "owned channel mismatch")
    with (directory / "publications.csv").open() as source:
        reader = csv.DictReader(source)
        require(reader.fieldnames == ["attempt", "sequence", "publish_ns", "accepted"], "bad publication CSV")
        publications = [{key: int(value) for key, value in row.items()} for row in reader]
    accepted = {}
    previous_publish = 0
    for ordinal, row in enumerate(publications, 1):
        require(row["attempt"] == ordinal and row["sequence"] == len(accepted) + 1, "publication identity")
        require(row["accepted"] in (0, 1), "invalid acceptance status")
        require(previous_publish < row["publish_ns"], "nonmonotonic publish timestamp")
        require(published["begin_ns"] <= row["publish_ns"] <= published["end_ns"], "publish outside work")
        previous_publish = row["publish_ns"]
        if row["accepted"]:
            accepted[row["sequence"]] = row["publish_ns"]
    require(published["attempted"] == len(publications), "attempted count mismatch")
    require(published["accepted"] == published["final_sequence"] == len(accepted), "accepted count mismatch")
    with (directory / "samples.csv").open() as source:
        reader = csv.DictReader(source)
        require(reader.fieldnames == FIELDS, "sample CSV schema mismatch")
        rows = [{key: int(value) for key, value in row.items()} for row in reader]
    with (directory / "samples.raw.csv").open() as source:
        raw_rows = [[int(value) for value in row] for row in csv.reader(source)]
    require(raw_rows == [[row[key] for key in FIELDS[:7]] for row in rows], "worker/final CSV mismatch")
    previous_seq, previous_receive, missed, duplicate, initial_gap = 0, 0, 0, 0, 0
    seen, generations = set(), set()
    for row in rows:
        sequence = row["sequence"]
        duplicate += int(sequence in seen)
        require(sequence > previous_seq, "duplicate or reordered sequence")
        gap = sequence - previous_seq - 1 if previous_seq else 0
        if not previous_seq:
            initial_gap = sequence - 1
        require(row["missed_samples"] == gap, "missed counter mismatch")
        missed += gap
        seen.add(sequence)
        generations.add(row["generation"])
        require(row["generation"] > 0 and row["payload_bytes"] == 64, "payload/generation mismatch")
        require(accepted.get(sequence) == row["publish_ns"], "sample not in accepted publication ledger")
        require(row["receive_ns"] >= row["publish_ns"] and row["receive_ns"] > previous_receive,
                "invalid receive timestamp")
        require(row["latency_ns"] == row["receive_ns"] - row["publish_ns"], "modified latency")
        require(consumed["begin_ns"] <= row["receive_ns"] <= consumed["end_ns"], "receive outside work")
        for role, record in (("producer", producer), ("consumer", consumer)):
            life = record["lifecycle"]
            require(row[f"{role}_cpu"] == life["user_us"] + life["system_us"], "CSV lifecycle CPU mismatch")
        previous_seq, previous_receive = sequence, row["receive_ns"]
    require(len(generations) <= 1, "generation changed during run")
    for key, expected in (("consumed", len(rows)), ("unique_consumed", len(seen)),
                          ("final_sequence", previous_seq), ("missed", missed), ("duplicate", duplicate)):
        require(consumed[key] == expected, f"consumer count mismatch: {key}")
    for key in ("consumed", "unique_consumed", "missed", "duplicate", "torn", "wait_timeouts"):
        require(published[key] == 0, f"invalid producer counter {key}")
    require(consumed["attempted"] == consumed["accepted"] == 0, "invalid consumer publish counters")
    done = re.findall(r"^DONE published=(\d+) cpu_us=(\d+)$", producer["raw"], re.MULTILINE)
    require(len(done) == 1 and int(done[0][0]) == len(accepted), "DONE count mismatch")
    summaries = re.findall(r"^SUMMARY reads=(\d+) torn=(\d+) missed_total=(\d+) last_seq=(\d+) "
                           r"elapsed_ms=(\d+) cpu_us=(\d+) ended=(\w+)$", consumer["raw"], re.MULTILINE)
    require(len(summaries) == 1, "consumer SUMMARY missing/duplicate")
    marker = summaries[0]
    require([int(value) for value in marker[:4]] == [len(rows), consumed["torn"], missed, previous_seq],
            "SUMMARY count mismatch")
    warmup = min(case["warmup"], len(rows))
    measured = rows[warmup:]
    overlap = sum(row["receive_ns"] > accepted[row["sequence"] + 1]
                  for row in measured if row["sequence"] + 1 in accepted)
    stats = quantiles([row["latency_ns"] for row in measured])
    summary = load(directory / "summary.json")
    for key, expected in (("run_id", case["run_id"]), ("transport", "shm"),
                          ("notification", case["notification"]), ("mode", case["notification"]),
                          ("payload_bytes", 64), ("placement", case["placement"]),
                          ("rate_hz", case["rate_hz"]), ("count", len(rows)),
                          ("warmup_count", warmup), ("measured_count", len(measured)),
                          ("missed_samples", missed), ("torn_reads", consumed["torn"])):
        require(summary[key] == expected, f"summary mismatch: {key}")
    for key, expected in stats.items():
        require(summary["latency_ns"][key] == expected, f"quantile mismatch: {key}")
    latencies = [row["latency_ns"] for row in measured]
    require(summary["latency_ns"]["min"] == min(latencies, default=0), "minimum mismatch")
    require(summary["latency_ns"]["max"] == max(latencies, default=0), "maximum mismatch")
    for key, expected in (("mean", statistics.mean(latencies) if latencies else 0),
                          ("stddev", statistics.pstdev(latencies) if latencies else 0)):
        require(math.isclose(summary["latency_ns"][key], expected, abs_tol=0.001), f"summary {key} mismatch")
    for role, record in (("producer", producer), ("consumer", consumer)):
        life = record["lifecycle"]
        require(math.isclose(summary[f"{role}_cpu_ms"], (life["user_us"] + life["system_us"]) / 1000,
                             abs_tol=0.001), "summary lifecycle CPU mismatch")
    eligibility = []
    if len(accepted) != case["samples"]:
        eligibility.append("accepted publications != target")
    if len(measured) < case["samples"] - case["warmup"]:
        eligibility.append("insufficient postwarm observations")
    if missed + initial_gap:
        eligibility.append("latest-value gaps (legal integrity, not qualified comparison)")
    if previous_seq != len(accepted):
        eligibility.append("final sequence != accepted publications")
    timeout_count = consumed["timeout"] + published["timeout"] + consumed["wait_timeouts"]
    if duplicate or consumed["torn"] or timeout_count:
        eligibility.append("duplicate/torn/terminal timeout")
    if overlap:
        eligibility.append("postwarm receive crossed next publication")
    for role, record in (("producer", producer), ("consumer", consumer)):
        life = record["lifecycle"]
        if life["exit_code"] != 0 or life["signal"] or life["timed_out"] or not life["exec_ok"]:
            eligibility.append(f"{role} lifecycle failed")
    observed_overlap = sum(rows[index]["receive_ns"] > rows[index + 1]["publish_ns"]
                           for index in range(warmup, len(rows) - 1))
    old_eligible = (len(accepted) == case["samples"] and len(rows) == case["samples"]
                    and not missed and not observed_overlap and bool(measured))
    require(summary["latency_comparison"]["eligible"] == old_eligible, "summary eligibility mismatch")
    require(summary["latency_comparison"]["queue_overlap_count"] == observed_overlap, "summary overlap mismatch")
    return {"integrity": consumed["torn"] == 0, "eligible": not eligibility,
            "integrity_errors": ["payload torn/counter integrity failed"] if consumed["torn"] else [],
            "ineligible_reasons": eligibility,
            "attempted": len(publications), "accepted": len(accepted), "unique_consumed": len(seen),
            "final_sequence": previous_seq, "gaps": missed + initial_gap, "missed_samples": missed,
            "initial_gap": initial_gap, "duplicate": duplicate,
            "torn": consumed["torn"], "timeouts": timeout_count,
            "postwarm_observations": len(measured), "queue_overlap": overlap, "latency_ns": stats,
            "publish_span_ns": previous_publish - next(iter(accepted.values())) if accepted else 0,
            "workers": {"producer": published, "consumer": consumed}}


def verify_runner_affinity(samples: list[dict], worker: dict, cpu: int, argv: list[str],
                           *, schema: int, minimum_threads: int = 1) -> dict:
    require(type(schema) is int and schema in (1, 2), "unsupported affinity sample schema")
    observations = [affinity for sample in samples for affinity in sample.get("affinities", [])
                    if affinity["pid"] == worker["pid"]]
    if schema == 1:
        require(all("schema" not in item for item in observations),
                "runner /proc missing affinity sample schema declaration")
        require(observations and all(item["cpus"] == [cpu] for item in observations),
                "runner /proc process affinity mismatch")
        return {"schema": 1, "lifecycle_observations": len(observations)}
    counts = dict(schema=2, startup=0, work=0, teardown=0, boundary=0)
    identity = None
    for item in observations:
        require(item.get("schema") == 2, "runner /proc affinity sample schema mismatch")
        begin, end = item["begin_ns"], item["end_ns"]
        require(type(begin) is int and type(end) is int and 0 < begin <= end,
                "runner /proc invalid affinity sample interval")
        if end < worker["begin_ns"]:
            counts["startup"] += 1
            continue
        if begin > worker["end_ns"]:
            counts["teardown"] += 1
            continue
        if begin < worker["begin_ns"] or end > worker["end_ns"]:
            counts["boundary"] += 1
            continue
        require(not item.get("error"), "runner /proc work affinity unverifiable: " + str(item.get("error")))
        require(item["argv"] == argv[3:] and item["exe"] == str(Path(argv[3]).resolve()),
                "runner /proc worker executable/argv mismatch")
        require(type(item["starttime_ticks"]) is int and item["starttime_ticks"] > 0,
                "runner /proc missing process identity")
        if identity is None:
            identity = item["starttime_ticks"]
        require(identity == item["starttime_ticks"], "runner /proc process identity changed")
        require(item["cpus"] == [cpu], "runner /proc work process affinity mismatch")
        threads = item["threads"]
        tids = [thread["tid"] for thread in threads]
        require(len(tids) >= minimum_threads and len(tids) == len(set(tids))
                and worker["pid"] in tids, "runner /proc work thread set incomplete")
        require(all(thread["cpus"] == [cpu] for thread in threads),
                "runner /proc work thread affinity mismatch")
        counts["work"] += 1
    require(counts["work"] > 0, "runner /proc no verifiable in-work affinity observation")
    return counts


def verify(root: Path, *, allow_smoke: bool = False,
           expected_backends: tuple[str, ...] | None = None) -> dict:
    plan, manifest = load(root / "plan.json"), load(root / "manifest.json")
    smoke = plan["smoke"]
    require(type(smoke) is bool and (not smoke or allow_smoke), "smoke is not formal evidence")
    require(plan["schema"] in (1, 2), "unsupported matrix schema")
    backends = normative_backends(plan.get("backends")) if plan["schema"] == 2 else ("futex", "eventfd")
    if expected_backends is not None:
        require(backends == expected_backends, "requested backend scope mismatch")
    expected = normative_cases(smoke, backends)
    rules = dict(RULES)
    if smoke:
        rules.update(accepted_publications=50, warmup_observations=5, minimum_postwarm_observations=45)
    require(plan["cases"] == expected, "normative matrix config mismatch")
    require(plan["rules"] == rules and plan["serial"] is True and plan["watchdog_s"] == 60,
            "immutable eligibility/window rules mismatch")
    require(load(root / "plan.sha256.json")["sha256"] == sha(root / "plan.json"), "plan digest mismatch")
    require(plan["cpu_a"] != plan["cpu_b"] and {plan["cpu_a"], plan["cpu_b"]}
            <= set(manifest["allowed_cpus"]), "CPU plan mismatch")
    require(manifest["source_dir"] == manifest["cwd"], "source/cwd mismatch")
    require(re.fullmatch(r"[a-f0-9]{40,64}", manifest["head"]) is not None, "source HEAD invalid")
    require(sha(root / "CMakeCache.txt") == manifest["cache_sha256"], "cache identity mismatch")
    cache_lines = (root / "CMakeCache.txt").read_text().splitlines()
    require(f"CMAKE_HOME_DIRECTORY:INTERNAL={manifest['source_dir']}" in cache_lines, "build/source mismatch")
    if not smoke:
        for setting in ("CMAKE_BUILD_TYPE:STRING=Release", "SELFIPCS_ENABLE_FAILPOINTS:BOOL=OFF",
                        "SELFIPCS_ENABLE_SANITIZERS:BOOL=OFF"):
            require(setting in cache_lines, f"formal build config mismatch: {setting}")
    require(sha(root / "source.patch") == manifest["source_patch_sha256"], "source patch mismatch")
    after = load(root / "source-after.json")
    require(after["head"] == manifest["head"] and after["status"] == manifest["status"]
            and after["binaries"] == manifest["binaries"] and after["source_files"] == manifest["source_files"],
            "source/binaries changed during collection")
    require(sorted(path.name for path in root.glob("case-*")) ==
            [f"case-{ordinal:02d}" for ordinal in range(1, len(expected) + 1)], "missing/extra case directory")
    outcomes = load(root / "outcomes.json")
    require(len(outcomes) == len(expected), "outcomes count mismatch")
    results = []
    previous_end_ns = 0
    for ordinal, case in enumerate(expected, 1):
        directory = root / f"case-{ordinal:02d}"
        try:
            command, exit_record = load(directory / "command.json"), load(directory / "exit.json")
            require(command["case"] == case and command["cwd"] == manifest["source_dir"], "command config mismatch")
            require(outcomes[ordinal - 1] == {"ordinal": ordinal, "case": case, **exit_record}, "exit record mismatch")
            require(command["start_epoch_ns"] >= previous_end_ns, "runs were not serial")
            previous_end_ns = exit_record["end_epoch_ns"]
            argv = command["argv"]
            for option, value in (("--git-dir", manifest["source_dir"]), ("--only", case["run_id"]),
                                   ("--notification", case["notification"]), ("--cpu-a", str(plan["cpu_a"])),
                                   ("--cpu-b", str(plan["cpu_b"]))):
                require(argument(argv, option) == value, f"driver config mismatch: {option}")
            require(argv.count("--notification-matrix") == 1 and ("--notification-smoke" in argv) == smoke,
                    "driver profile mismatch")
            require(sorted(path.name for path in (directory / "output").iterdir() if path.is_dir()) == [case["run_id"]],
                    "unexpected/missing worker output")
            result = verify_run(directory / "output" / case["run_id"], case, plan["cpu_a"], plan["cpu_b"],
                                require_backend_identity=plan["schema"] == 2)
            summary = load(directory / "output" / case["run_id"] / "summary.json")
            require(summary["git_commit"] == manifest["head"] + (" (dirty)" if manifest["status"] else ""),
                    "driver source identity mismatch")
            samples = [json.loads(line) for line in (directory / "process-samples.jsonl").read_text().splitlines()]
            require(samples and samples[-1]["returncode"] == exit_record["returncode"], "process log exit mismatch")
            worker_argv = load(directory / "output" / case["run_id"] / "workers.argv.json")
            result["runner_affinity"] = {}
            for role, worker in result["workers"].items():
                cpu = plan["cpu_a"] if role == "producer" or case["placement"] == "same" else plan["cpu_b"]
                result["runner_affinity"][role] = verify_runner_affinity(
                    samples, worker, cpu, worker_argv[role], schema=manifest.get("affinity_sample_schema", 1),
                    minimum_threads=2 if role == "producer" and case["notification"] in ("eventfd", "driver") else 1)
                require(worker["pid"] in exit_record["observed_child_pids"], "unobserved worker PID")
                require(exit_record["start_epoch_ns"] <= worker["begin_realtime_ns"]
                        <= worker["end_realtime_ns"] <= exit_record["end_epoch_ns"], "work outside runner lifecycle")
            execution_ok = (exit_record.get("returncode") == 0 and not exit_record.get("timed_out")
                            and not exit_record.get("orphan_group_reaped") and not exit_record.get("cleanup_unverified"))
            if not execution_ok:
                result["eligible"] = False
                result["ineligible_reasons"].append("driver/session failed")
            result["execution_ok"] = execution_ok
        except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
            result = {"integrity": False, "eligible": False, "errors": [str(error)]}
        results.append({"ordinal": ordinal, "case": case, **result})
    return {"schema": 2, "formal": not smoke, "backends": list(backends), "expected_runs": len(expected),
            "integrity": all(item["integrity"] for item in results),
            "all_eligible": all(item["eligible"] for item in results), "runs": results}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-smoke", action="store_true")
    parser.add_argument("--require-eligible", action="store_true")
    parser.add_argument("--backends", help="require this canonical backend scope, e.g. futex,eventfd,driver")
    args = parser.parse_args()
    try:
        scope = normative_backends(args.backends.split(",")) if args.backends is not None else None
        result = verify(args.input_root.resolve(), allow_smoke=args.allow_smoke, expected_backends=scope)
    except (OSError, ValueError, KeyError, TypeError, IndexError) as error:
        result = {"integrity": False, "all_eligible": False, "errors": [str(error)]}
    with args.output.open("x") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({key: value for key, value in result.items() if key != "runs"}, sort_keys=True))
    return 0 if result["integrity"] and (result["all_eligible"] or not args.require_eligible) else 1


if __name__ == "__main__":
    raise SystemExit(main())
