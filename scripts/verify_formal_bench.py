#!/usr/bin/env python3
"""Independently check IPC identities/counts and nearest-rank CSV statistics."""

from __future__ import annotations

import argparse
from array import array
from collections import Counter
import csv
import json
import mmap
from pathlib import Path
import statistics
import struct
import sys

from run_formal_bench import digest
from self_ipcs_compat import canonical_name, validate_release_cache


RECORD = struct.Struct("<IIQQ")
COUNTERS = {
    "mpmc": ["published", "delivered", "queue_full", "queue_empty", "queue_contention",
             "recovery", "errors", "invalid_payload"],
    "pool": ["published", "delivered", "pool_full", "pool_contention", "queue_full",
             "queue_empty", "queue_contention", "recovery", "errors", "invalid_payload"],
}


def fields(text: str) -> dict[str, str]:
    return dict(token.split("=", 1) for token in text.split() if "=" in token)


def quantiles(latencies) -> dict[str, int]:
    ordered = sorted(latencies)
    if not ordered:
        raise ValueError("no latency samples")
    return {f"p{percent}_ns": ordered[(len(ordered) * percent + 99) // 100 - 1]
            for percent in (50, 95, 99)} | {"max_ns": ordered[-1]}


def worker_header(path: Path, kind: str) -> dict:
    names = ["magic", "version", "role", *COUNTERS[kind], "first_error_code", "reserved",
             "record_count"]
    pattern = "<IHH" + "Q" * len(COUNTERS[kind]) + "IIQ"
    if kind == "mpmc":
        names += ["wait_calls", "wait_timeouts"]
        pattern += "QQ"
    names += ["perf_available", "reserved2", "perf_context_switches"]
    pattern += "IIQ"
    layout = struct.Struct(pattern)
    with path.open("rb") as source:
        header = dict(zip(names, layout.unpack(source.read(layout.size))))
    magic, version = (0x45524D42, 2) if kind == "mpmc" else (0x45524250, 1)
    if (header["magic"] != magic or header["version"] != version or
            header["role"] not in (1, 2) or header["reserved"] or header["reserved2"] or
            header["perf_available"] not in (0, 1) or
            path.stat().st_size != layout.size + RECORD.size * header["record_count"]):
        raise ValueError(f"invalid worker report {path}")
    header["header_bytes"] = layout.size
    header["path"] = str(path)
    return header


def records_from_workers(workers: list[dict]):
    for worker in workers:
        if worker["role"] != 2:
            continue
        with Path(worker["path"]).open("rb") as source:
            with mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
                for offset in range(worker["header_bytes"], len(mapped), RECORD.size):
                    yield RECORD.unpack_from(mapped, offset)


def check_delivery(case_dir: Path, case: dict) -> dict:
    exit_status = json.loads((case_dir / "exit.json").read_text())
    if exit_status["returncode"] or exit_status["timed_out"] or exit_status["orphan_group_reaped"]:
        raise ValueError(f"unsuccessful bounded execution: {exit_status}")
    lines = (case_dir / "stdout.raw").read_text().splitlines()
    results = [fields(line) for line in lines if line.startswith("RESULT ")]
    if len(results) != 1:
        raise ValueError("expected exactly one RESULT")
    result = results[0]
    configured = {"producers": case["producers"], "consumers": case["consumers"],
                  "messages_per_producer": case["messages"], "capacity": case.get("capacity", 64)}
    if case["kind"] == "mpmc":
        configured.update(payload_bytes=case["payload"], retry_policy="yield", wait_policy=case["wait"])
    else:
        configured.update(block_bytes=case["payload"], block_count=case["blocks"])
    if any(result.get(name) != str(value) for name, value in configured.items()):
        raise ValueError("RESULT configuration does not match manifest")
    expected = case["producers"] * case["messages"]
    for name in ("published", "delivered", "expected"):
        if int(result[name]) != expected:
            raise ValueError(f"{name} did not reach {expected}")
    if result["correctness"] != "PASS" or any(int(result[name]) for name in ("errors", "recovery")):
        raise ValueError(f"worker errors: {result}")
    cleanup = [fields(line) for line in lines if line.startswith("CLEANUP ")]
    required_cleanup = ({"remove": "SUCCESS", "reopen": "NotFound"} if case["kind"] == "mpmc"
                        else {"pool": "SUCCESS", "queue": "SUCCESS",
                              "reopen_pool": "NotFound", "reopen_queue": "NotFound"})
    if len(cleanup) != 1 or any(cleanup[0].get(name) != value for name, value in required_cleanup.items()):
        raise ValueError("missing successful creator cleanup")
    if any(name in cleanup[0] for name in ("unsafe", "force", "fallback")):
        raise ValueError("unsupported cleanup mode")
    if cleanup[0].get("stopped") != "false" or cleanup[0].get("child_kill") != "false":
        raise ValueError("normal performance case required forced cleanup")
    if case["kind"] == "pool":
        pool_status = [fields(line) for line in lines if line.startswith("POOL_STATUS ")]
        if (len(pool_status) != 1 or int(pool_status[0]["free"]) != case["blocks"] or
                any(int(pool_status[0][name]) for name in ("writing", "published", "reading"))):
            raise ValueError("shared blocks not all returned before creator removal")
    output = case_dir / "output"
    reports = sorted(output.glob("*.report"), key=lambda path: int(path.stem.rsplit("_", 1)[1]))
    workers = [worker_header(path, case["kind"]) for path in reports]
    if (len(workers) != case["producers"] + case["consumers"] or
            sum(worker["role"] == 1 for worker in workers) != case["producers"] or
            sum(worker["role"] == 2 for worker in workers) != case["consumers"]):
        raise ValueError("worker report/role count mismatch")
    for index, worker in enumerate(workers):
        if any(worker[name] for name in ("errors", "invalid_payload", "recovery")):
            raise ValueError(f"worker checksum/protocol/recovery error: {worker}")
        if worker["first_error_code"] != 0xFFFFFFFF:
            raise ValueError("unexpected worker first_error_code")
        if worker["role"] == 1:
            if worker["published"] != case["messages"] or worker["delivered"] or worker["record_count"]:
                raise ValueError("invalid producer report")
        elif worker["published"] or worker["record_count"] != worker["delivered"]:
            raise ValueError("invalid consumer report")
        expected_role = 2 if index < case["consumers"] else 1
        if worker["role"] != expected_role:
            raise ValueError("worker role/index mismatch")
        if expected_role == 2 and worker["delivered"] != expected // case["consumers"] + (index < expected % case["consumers"]):
            raise ValueError("consumer delivery target mismatch")
    for name in COUNTERS[case["kind"]] + (["wait_calls", "wait_timeouts"] if case["kind"] == "mpmc" else []):
        total = sum(worker[name] for worker in workers)
        if name in result and total != int(result[name]):
            raise ValueError(f"worker/summary {name} mismatch")
    cpu_rows = [fields(line) for line in lines if line.startswith("WORKER ")]
    if len(cpu_rows) != len(workers) or {row["report"] for row in cpu_rows} != {worker["path"] for worker in workers}:
        raise ValueError("missing per-worker wait4 evidence")
    if any(int(row["status"]) for row in cpu_rows):
        raise ValueError("unsuccessful worker exit")
    if len({row["pid"] for row in cpu_rows}) != len(workers) or any(int(row["pid"]) <= 0 for row in cpu_rows):
        raise ValueError("invalid worker PID identity")
    by_report = {row["report"]: row for row in cpu_rows}
    for worker in workers:
        row = by_report[worker["path"]]
        if row["role"] != ("producer" if worker["role"] == 1 else "consumer") or any(
                int(row[name]) < 0 for name in ("user_cpu_us", "system_cpu_us")):
            raise ValueError("worker role/CPU invalid")
    for name in ("user_cpu_us", "system_cpu_us"):
        if sum(int(row[name]) for row in cpu_rows) != int(result[name]):
            raise ValueError(f"wait4/summary {name} mismatch")
    if int(result["cpu_total_us"]) != int(result["user_cpu_us"]) + int(result["system_cpu_us"]):
        raise ValueError("CPU addition mismatch")
    paths = list(output.glob("*.csv"))
    if len(paths) != 1:
        raise ValueError("expected one delivery CSV")
    seen = bytearray(expected)
    latencies = array("Q")
    worker_records = records_from_workers(workers)
    with paths[0].open(newline="") as source:
        reader = csv.reader(source)
        if next(reader) != ["producer", "sequence", "latency_ns"]:
            raise ValueError("invalid CSV header")
        for row in reader:
            producer, sequence, latency = map(int, row)
            if not 0 <= producer < case["producers"] or not 0 <= sequence < case["messages"]:
                raise ValueError("producer/sequence out of range")
            identity = producer * case["messages"] + sequence
            if seen[identity]:
                raise ValueError(f"duplicate identity {producer}/{sequence}")
            seen[identity] = 1
            if next(worker_records, None) != (producer, 0, sequence, latency):
                raise ValueError("CSV and raw worker record disagree")
            latencies.append(latency)
    if len(latencies) != expected or 0 in seen or next(worker_records, None) is not None:
        raise ValueError("missing delivery or extra worker record")
    stats = quantiles(latencies)
    if any(int(result[name]) != value for name, value in stats.items()):
        raise ValueError(f"CSV/summary quantiles disagree: {stats}")
    wall_us = int(result["wall_us"])
    if wall_us <= 0:
        raise ValueError("nonpositive parent measurement window")
    throughput = expected * 1000000 / wall_us
    return {"valid": True, "kind": case["kind"], "case": case, "directory": str(case_dir),
            "count": expected, "duplicates": 0, "missing": 0, "checksum_errors": 0,
            "workers": workers, "wait4": cpu_rows, "csv": str(paths[0]), "result": result, **stats,
            "messages_per_s": throughput, "payload_mib_per_s": throughput * case["payload"] / 1024**2,
            "cpu_total_us": int(result["cpu_total_us"]), "wall_us": wall_us}


def check_spsc(case_dir: Path, case: dict) -> dict:
    transports = []
    reasons = []
    for summary in sorted((case_dir / "output").glob("*/summary.json")):
        report = json.loads(summary.read_text())
        if report["rate_hz"] != 100 or report["payload_bytes"] != 64 or report["placement"] != case["placement"]:
            raise ValueError("SPSC/socket configuration mismatch")
        with summary.with_name("samples.csv").open(newline="") as source:
            rows = [{key: int(value) for key, value in row.items()} for row in csv.DictReader(source)]
        previous = 0
        for row in rows:
            if (row["sequence"] <= previous or row["payload_bytes"] != 64 or
                    row["receive_ns"] < row["publish_ns"] or
                    row["latency_ns"] != row["receive_ns"] - row["publish_ns"]):
                raise ValueError("invalid SPSC/socket sequence/timestamp/payload")
            previous = row["sequence"]
        measured = rows[100:]
        if len(rows) > 1:
            intervals = [second["publish_ns"] - first["publish_ns"]
                         for first, second in zip(rows, rows[1:])]
            mean_interval = (rows[-1]["publish_ns"] - rows[0]["publish_ns"]) / (len(rows) - 1)
            if not (9000000 <= statistics.median(intervals) <= 11000000 and
                    9000000 <= mean_interval <= 11000000):
                raise ValueError("observed publish cadence is outside 100Hz ±10% gate")
        stats = quantiles([row["latency_ns"] for row in measured])
        for name, value in stats.items():
            if report["latency_ns"][name.removesuffix("_ns")] != value:
                raise ValueError("SPSC/socket CSV quantiles disagree")
        overlap = sum(first["receive_ns"] > second["publish_ns"]
                      for first, second in zip(measured, measured[1:]))
        published = fields(summary.with_name("producer.raw").read_text()).get("published")
        cpu_text = summary.with_name("process_status.txt").read_text()
        cpu_values = [int(line.strip().split()[0].split("=", 1)[1]) for line in cpu_text.splitlines()
                      if line.strip().startswith("cpu_us=")]
        if len(cpu_values) != 2 or any(abs(raw / 1000 - report[name]) > 0.00051 for raw, name in
                                       zip(cpu_values, ("producer_cpu_ms", "consumer_cpu_ms"))):
            raise ValueError("SPSC/socket wait4 CPU mismatch")
        missed = sum(row["missed_samples"] for row in rows)
        if (len(rows) != report["count"] or report["warmup_count"] != 100 or
                len(measured) != report["measured_count"] or missed != report["missed_samples"] or
                overlap != report["latency_comparison"]["queue_overlap_count"]):
            raise ValueError("SPSC/socket CSV and summary counts disagree")
        eligible = (len(rows) == 1000 and len(measured) == 900 and published == "1000" and
                    [row["sequence"] for row in rows] == list(range(1, 1001)) and
                    missed == 0 and report["torn_reads"] == 0 and overlap == 0 and
                    report["latency_comparison"]["eligible"])
        if not eligible:
            reasons.append(f"{report['transport']}: count={len(rows)} published={published} "
                           f"gap={missed} torn={report['torn_reads']} overlap={overlap}; "
                           f"{report['latency_comparison']['reason']}")
        transports.append({"transport": report["transport"], "eligible": bool(eligible), **stats,
                           "directory": str(summary.parent), "count": len(rows), "warmup": 100,
                           "measured_count": len(measured), "gaps": missed, "overlap": overlap,
                           "cpu_total_ms": report["producer_cpu_ms"] + report["consumer_cpu_ms"]})
    exit_status = json.loads((case_dir / "exit.json").read_text())
    valid = (len(transports) == 2 and {item["transport"] for item in transports} == {"shm", "socket"}
             and all(item["eligible"] for item in transports) and not exit_status["returncode"]
             and not exit_status["timed_out"] and not exit_status["orphan_group_reaped"])
    result = {"valid": valid, "kind": "spsc", "case": case, "directory": str(case_dir),
              "transports": transports, "reasons": reasons}
    if valid:
        by_transport = {item["transport"]: item for item in transports}
        if by_transport["shm"]["p50_ns"] <= 0:
            raise ValueError("undefined latency ratio with zero SHM p50")
        result["socket_over_shm_p50"] = by_transport["socket"]["p50_ns"] / by_transport["shm"]["p50_ns"]
    return result


def usable_affinity_snapshot(sample) -> bool:
    if not isinstance(sample, dict) or "error" in sample:
        return False
    argv, cpus = sample.get("argv"), sample.get("cpus")
    if (not isinstance(argv, list) or not argv or not all(isinstance(token, str) for token in argv)
            or not argv[0] or not isinstance(cpus, list) or not cpus
            or not all(type(cpu) is int and cpu >= 0 for cpu in cpus)):
        return False
    if "schema" not in sample:
        return True
    if type(sample["schema"]) is not int or sample["schema"] != 2:
        return False
    if any(type(sample.get(key)) is not int or sample[key] <= 0
           for key in ("pid", "begin_ns", "end_ns", "starttime_ticks")):
        return False
    executable, threads = sample.get("exe"), sample.get("threads")
    if (sample["begin_ns"] > sample["end_ns"] or not isinstance(executable, str)
            or not Path(executable).is_absolute()
            or canonical_name(Path(executable).name) != canonical_name(Path(argv[0]).name)
            or not isinstance(threads, list) or not threads):
        return False
    if any(not isinstance(thread, dict) or type(thread.get("tid")) is not int
           or thread["tid"] <= 0 or thread.get("cpus") != cpus
           or any(type(cpu) is not int for cpu in thread["cpus"]) for thread in threads):
        return False
    tids = [thread["tid"] for thread in threads]
    return sample["pid"] in tids and len(tids) == len(set(tids))


def check_manifest(path: Path, metadata: dict) -> None:
    argv = metadata["argv"]
    source = metadata["source"]
    if digest(Path(argv[0])) != source["binaries"][argv[0]]:
        raise ValueError("executed binary SHA does not match manifest")
    cache = path.parent.parent / "CMakeCache.txt"
    if digest(cache) != source["cache_sha256"]:
        raise ValueError("Release cache SHA mismatch")
    validate_release_cache(cache)
    options = {}
    for index, token in enumerate(argv[1:], 1):
        if token.startswith("--") and index + 1 < len(argv) and not argv[index + 1].startswith("--"):
            if token in options:
                raise ValueError("duplicate CLI option")
            options[token] = argv[index + 1]
    case = metadata["case"]
    if case["kind"] == "spsc":
        placement = case["placement"]
        if options.get("--cpu-a") != str(source["cpu_a"]) or options.get("--cpu-b") != str(source["cpu_b"]):
            raise ValueError("SPSC affinity configuration mismatch")
        if options.get("--only") != (f"shm-futex-64B-{placement}-100-controlled,"
                                     f"socket-64B-{placement}-100-controlled"):
            raise ValueError("controlled SPSC selector mismatch")
        expected = {"self_ipcs_shm_bench_producer": source["cpu_a"],
                    "self_ipcs_shm_bench_consumer": (source["cpu_a"] if placement == "same"
                                                   else source["cpu_b"])}
        found = set()
        unusable = 0
        for line in (path.parent / "process-samples.jsonl").read_text().splitlines():
            for sample in json.loads(line).get("affinities", []):
                if not usable_affinity_snapshot(sample):
                    unusable += 1
                    continue
                name = canonical_name(Path(sample["argv"][0]).name)
                if name == "self_ipcs_shm_bench_sock":
                    if sample["argv"].count("--role") != 1:
                        unusable += 1
                        continue
                    role_index = sample["argv"].index("--role")
                    if (role_index + 1 >= len(sample["argv"])
                            or sample["argv"][role_index + 1] not in ("producer", "consumer")):
                        unusable += 1
                        continue
                    name = ("self_ipcs_shm_bench_producer"
                            if sample["argv"][role_index + 1] == "producer"
                            else "self_ipcs_shm_bench_consumer")
                    name = "socket_" + name
                base_name = name.removeprefix("socket_")
                if base_name in expected and sample["cpus"] == [expected[base_name]]:
                    found.add(name)
        required = set(expected) | {"socket_" + name for name in expected}
        if found != required:
            raise ValueError("missing actual affinity snapshots for both transport pairs: "
                             + ", ".join(sorted(required - found))
                             + f"; unusable snapshots={unusable}")
    else:
        configured = {"--producers": case["producers"], "--consumers": case["consumers"],
                      "--messages": case["messages"], "--capacity": case.get("capacity", 64), "--runs": 1}
        if case["kind"] == "mpmc":
            configured.update({"--payload-size": case["payload"], "--retry-policy": "yield", "--wait-policy": case["wait"]})
        else:
            configured.update({"--block-size": case["payload"], "--block-count": case["blocks"],
                               "--test-consumer-hold-ms": case.get("hold_ms", 0)})
        if any(options.get(name) != str(value) for name, value in configured.items()):
            raise ValueError("CLI configuration mismatch")


def specification(section: str, planned: list[dict]) -> list[dict]:
    expected = []
    if section == "spsc":
        for placement in ("same", "different"):
            expected.extend({"kind": "spsc", "placement": placement, "repeat": repeat}
                            for repeat in (1, 2, 3))
    elif section == "mpmc":
        for payload, producers, consumers in ((64, 1, 1), (64, 4, 4), (64, 16, 16),
                                               (4096, 4, 4)):
            for wait in ("busy", "blocking"):
                expected.extend({"kind": "mpmc", "payload": payload, "producers": producers,
                                 "consumers": consumers, "messages": 20000, "wait": wait,
                                 "repeat": repeat} for repeat in (1, 2, 3))
        expected.extend({"kind": "mpmc", "payload": 64, "producers": producers,
                         "consumers": consumers, "messages": 20000, "wait": "busy",
                         "repeat": 1, "supplemental": True}
                        for producers, consumers in ((1, 4), (4, 1)))
    elif section == "pool":
        for payload in (4096, 16384):
            for workers in (4, 16):
                expected.extend({"kind": "pool", "payload": payload, "producers": workers,
                                 "consumers": workers, "messages": 20000, "blocks": 128,
                                 "capacity": 64, "repeat": repeat} for repeat in (1, 2, 3))
        expected.append({"kind": "pool", "payload": 4096, "producers": 4, "consumers": 1,
                         "messages": 2000, "blocks": 4, "capacity": 2, "hold_ms": 200,
                         "repeat": 1, "supplemental": True})
    elif section == "sustained":
        messages = planned[0].get("messages") if len(planned) == 1 else None
        if type(messages) is not int or not 1 <= messages <= 2000000:
            raise ValueError("sustained messages must be an integer in 1..2000000 per producer")
        expected.append({"kind": "mpmc", "payload": 64, "producers": 16, "consumers": 16,
                         "wait": "busy", "messages": messages, "repeat": 1,
                         "supplemental": True})
    else:
        raise ValueError(f"unsupported formal section: {section}")
    return expected


def check_plans(root: Path, require_suite: bool = False) -> list[dict]:
    planned_paths = sorted(set(root.glob("*/planned.json")) | set(root.glob("planned.json")))
    checks = []
    for path in planned_paths:
        plan = json.loads(path.read_text())
        if plan["section"] == "calibrate":
            continue
        errors = []
        for ordinal, case in enumerate(plan["cases"], 1):
            command = path.parent / f"case-{ordinal:02d}/command.json"
            if not command.is_file() or json.loads(command.read_text())["case"] != case:
                errors.append(f"missing/mismatched planned case {ordinal}")
        observed = list(path.parent.glob("case-*/command.json"))
        if len(observed) != len(plan["cases"]):
            errors.append("unexpected case count")
        outcomes = path.with_name("outcomes.json")
        if not outcomes.is_file() or len(json.loads(outcomes.read_text())) != len(plan["cases"]):
            errors.append("missing/incomplete final outcomes")
        try:
            expected = specification(plan["section"], plan["cases"])
            expected_counts = Counter(json.dumps(case, sort_keys=True) for case in expected)
            observed_counts = Counter(json.dumps(case, sort_keys=True) for case in plan["cases"])
            if observed_counts != expected_counts:
                errors.append("formal specification mismatch: "
                              f"missing={list((expected_counts - observed_counts).elements())}; "
                              f"unexpected={list((observed_counts - expected_counts).elements())}")
        except ValueError as error:
            errors.append(str(error))
        checks.append({"directory": str(path.parent), "section": plan["section"], "errors": errors})
    if require_suite:
        sections = Counter(item["section"] for item in checks)
        if sections != Counter({name: 1 for name in ("spsc", "mpmc", "pool", "sustained")}):
            checks.append({"directory": str(root), "section": "suite",
                           "errors": ["suite requires exactly one spsc, mpmc, pool and sustained phase"]})
    return checks


def aggregate(rows: list[dict]) -> list[dict]:
    groups = {}
    for row in rows:
        if not row["valid"]:
            continue
        case = {key: value for key, value in row["case"].items() if key != "repeat"}
        key = json.dumps(case, sort_keys=True)
        groups.setdefault(key, []).append(row)
    result = []
    for key, group in groups.items():
        metrics = {}
        if group[0]["kind"] == "spsc":
            for transport in ("shm", "socket"):
                for name in ("p50_ns", "p95_ns", "p99_ns", "cpu_total_ms"):
                    values = [next(item for item in row["transports"] if item["transport"] == transport)[name]
                              for row in group]
                    metrics[f"{transport}_{name}"] = {"median": statistics.median(values),
                                                       "min": min(values), "max": max(values)}
            names = ["socket_over_shm_p50"]
        else:
            names = ["messages_per_s", "payload_mib_per_s", "p50_ns", "p95_ns", "p99_ns",
                     "cpu_total_us", "wall_us"]
        for name in names:
            values = [row[name] for row in group]
            metrics[name] = {"median": statistics.median(values), "min": min(values), "max": max(values)}
        result.append({"case": json.loads(key), "valid_runs": len(group), "metrics": metrics,
                       "directories": [row["directory"] for row in group]})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--include-calibration", action="store_true")
    parser.add_argument("--metadata-only", action="store_true",
                        help="validate section configurations without reading delivery CSVs")
    parser.add_argument("--require-suite", action="store_true",
                        help="require exactly one phase of each of the four formal sections")
    args = parser.parse_args()
    commands = sorted(set(args.input_root.glob("*/case-*/command.json")) |
                      set(args.input_root.glob("case-*/command.json")))
    if not commands:
        parser.error("no formal command manifests found")
    rows = []
    excluded = []
    completeness = check_plans(args.input_root, args.require_suite)
    if args.metadata_only:
        with args.output.open("x", encoding="utf-8") as output:
            json.dump({"coverage_mode": "suite" if args.require_suite else "local_sections",
                       "metadata_only": True, "completeness": completeness}, output, indent=2)
            output.write("\n")
        return 0 if completeness and not any(item["errors"] for item in completeness) else 1
    for path in commands:
        metadata = json.loads(path.read_text())
        case = metadata["case"]
        if not args.include_calibration and (metadata.get("section") == "calibrate" or
                                             path.parent.parent.name.startswith("calibrate-")):
            excluded.append(str(path.parent))
            continue
        try:
            check_manifest(path, metadata)
            row = check_spsc(path.parent, case) if case["kind"] == "spsc" else check_delivery(path.parent, case)
        except (OSError, ValueError, KeyError, StopIteration, struct.error, TypeError, IndexError) as error:
            row = {"valid": False, "kind": case["kind"], "case": case,
                   "directory": str(path.parent), "error": str(error)}
        rows.append(row)
        print(f"verified={path.parent} valid={row['valid']}", flush=True)
    with args.output.open("x", encoding="utf-8") as output:
        json.dump({"runs": rows, "groups": aggregate(rows),
                   "excluded_calibration": excluded,
                   "completeness": completeness,
                   "coverage_mode": "suite" if args.require_suite else "local_sections",
                   "valid": sum(row["valid"] for row in rows),
                   "invalid": sum(not row["valid"] for row in rows)}, output, indent=2, sort_keys=True)
        output.write("\n")
    return 0 if rows and completeness and all(row["valid"] for row in rows) and not any(
        item["errors"] for item in completeness) else 1


if __name__ == "__main__":
    sys.exit(main())
