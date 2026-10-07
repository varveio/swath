#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0

"""Kill a real Go producer, then recover with Swath's actual Java store/runtime."""

import argparse
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time


STAGES = ("page-commit", "mid-part", "part-durable", "part-txn", "part-commit", "completed-tail", "all-parts-commit")


def md5(path):
    digest = hashlib.md5()
    with path.open("rb") as stream:
        for data in iter(lambda: stream.read(1 << 20), b""):
            digest.update(data)
    return digest.hexdigest()


def write_corpus(path):
    rows = []
    for index in range(513):
        rows.append({
            "key": f"key/{index:06d}/" + ("é" if index % 7 == 0 else "leaf"),
            "size": index * 17,
            "last_modified": None if index % 3 == 0 else 1729000000000000 + index,
            "etag": None if index % 4 == 0 else f"{index:032x}",
            "storage_class": None if index % 5 == 0 else "STANDARD",
            "version_id": None, "is_latest": None, "is_delete_marker": False,
            "owner_id": None, "owner_display_name": None,
            "checksum_algorithm": None, "checksum_type": None,
            "row_type": "OBJECT",
        })
    with path.open("w") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    return rows


def command_json(command, directory, label, env=None):
    result = subprocess.run(command, cwd=directory, env=env, capture_output=True, text=True, timeout=120)
    (directory / f"{label}-stdout.txt").write_text(result.stdout)
    (directory / f"{label}-stderr.txt").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(f"{label} exited {result.returncode}: {result.stderr[-3000:]}")
    return json.loads(result.stdout)


def checkpoint_snapshot(root, fixture):
    location = (root / ".swath/checkpoint.sqlite").as_uri() + "?mode=ro"
    with closing(sqlite3.connect(location, uri=True)) as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        node = connection.execute(
            "SELECT status, cursor, durable_cursor, generation, pages_emitted FROM listing_node WHERE id=?",
            (fixture["node_id"],),
        ).fetchone()
        parts = connection.execute(
            "SELECT path, rows, bytes FROM part_file WHERE run_id=? AND finalized=1 ORDER BY id",
            (fixture["run_id"],),
        ).fetchall()
    return {
        "schema_version": version,
        "node": {"status": node[0], "cursor": node[1].hex() if node[1] is not None else None,
                 "durable_cursor": node[2].hex() if node[2] is not None else None,
                 "generation": node[3], "pages_emitted": node[4]},
        "parts": [{"path": key, "rows": count, "bytes": size, "md5": md5(root / key)}
                  for key, count, size in parts],
    }


def wait_ready(process, timeout):
    selector = selectors.DefaultSelector()
    try:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(timeout):
            raise RuntimeError("Go producer did not reach its crash barrier")
        line = process.stdout.readline()
        if not line:
            raise RuntimeError(f"Go producer exited before its crash barrier ({process.poll()})")
        return json.loads(line)
    finally:
        selector.close()


def check_barrier(stage, initial, snapshot, rows):
    # Each state lists cursor row, durable row, finalized parts, new page
    # commits, and status independently of the producer's implementation.
    expected = {
        "page-commit": (191, 127, 1, 1, "IN_PROGRESS"),
        "mid-part": (255, 127, 1, 2, "IN_PROGRESS"),
        "part-durable": (255, 127, 1, 2, "IN_PROGRESS"),
        "part-txn": (255, 127, 1, 2, "IN_PROGRESS"),
        "part-commit": (255, 255, 2, 2, "IN_PROGRESS"),
        "completed-tail": (512, 511, 4, 7, "COMPLETED"),
        "all-parts-commit": (512, 512, 5, 7, "COMPLETED"),
    }
    cursor_index, durable_index, count, go_pages, status = expected[stage]
    # Initial Java state already has the first Go page's eventual cursor.
    # Counters prove Go performed both recovery and fresh commits, rather than
    # simply reporting a ready barrier over the untouched fixture.
    assert snapshot["node"]["generation"] == initial["node"]["generation"] + 1, (initial, snapshot)
    assert snapshot["node"]["pages_emitted"] == initial["node"]["pages_emitted"] + go_pages, (initial, snapshot)
    assert snapshot["schema_version"] == 1
    assert snapshot["node"]["cursor"] == rows[cursor_index]["key"].encode().hex(), snapshot
    assert snapshot["node"]["durable_cursor"] == rows[durable_index]["key"].encode().hex(), snapshot
    assert len(snapshot["parts"]) == count, snapshot
    assert snapshot["node"]["status"] == status, snapshot


def run_case(options, output, corpus, rows, stage, run_tests):
    root = output / stage
    root.mkdir()
    java = ["taskset", "-c", options.cpus, options.java, "--enable-native-access=ALL-UNNAMED",
            "-cp", options.java_classpath, "CheckpointInterop"]
    created = command_json(java + ["create", str(root), str(corpus)], output, f"{stage}-create")
    fixture = json.loads((root / "fixture.json").read_text())
    baseline = root / fixture["baseline_part"]
    assert md5(baseline) == fixture["baseline_md5"]
    if run_tests:
        environment = dict(os.environ, SWATH_CHECKPOINT_FIXTURE=str(root), CGO_ENABLED="1")
        result = subprocess.run(["taskset", "-c", options.cpus, options.go, "test", "-race", "-run", "^TestCheckpoint", "-count=1", "-v", "."],
                                cwd=Path(__file__).resolve().parent, env=environment,
                                capture_output=True, text=True, timeout=120)
        (output / "checkpoint-tests.txt").write_text(result.stdout + result.stderr)
        if result.returncode or "SKIP" in result.stdout:
            raise RuntimeError(f"focused checkpoint tests failed or skipped: {result.stdout}\n{result.stderr}")
    stderr_path = output / f"{stage}-go-stderr.txt"
    initial = checkpoint_snapshot(root, fixture)
    with stderr_path.open("wb") as stderr:
        process = subprocess.Popen(["taskset", "-c", options.cpus, options.go_binary,
                                    "resume-probe", "--root", str(root), "--stage", stage],
                                   stdout=subprocess.PIPE, stderr=stderr)
        try:
            ready = wait_ready(process, 30)
            if ready.get("ready") is not True or ready.get("stage") != stage or process.poll() is not None:
                raise RuntimeError(f"crash barrier was not live: {ready}")
            before = checkpoint_snapshot(root, fixture)
            check_barrier(stage, initial, before, rows)
            if stage == "all-parts-commit":
                assert all(not (root / marker).exists() for marker in ("manifest.json", ".swath-state.json", "_SUCCESS")), "Go reached publication before its intended crash barrier"
            candidate = root / ready["candidate_part"]
            candidate_before = {"exists": candidate.exists()}
            if candidate.exists():
                candidate_before.update(bytes=candidate.stat().st_size, md5=md5(candidate))
            if stage == "mid-part":
                assert candidate.stat().st_size == 128
                assert candidate.read_bytes()[:4] == b"PAR1"
            process.kill()
            returncode = process.wait(timeout=15)
            if returncode != -signal.SIGKILL:
                raise RuntimeError(f"expected real SIGKILL, got {returncode}")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=15)
            process.stdout.close()
    killed = checkpoint_snapshot(root, fixture)
    assert killed == before, "kill changed committed checkpoint state"
    recovered = command_json(java + ["recover", str(root)], output, f"{stage}-recover")
    assert recovered["recovered"] is True and recovered["total_rows"] == recovered["unique_keys"] == 513
    assert recovered["all_13_fields_and_nulls_exact"] is True
    assert recovered["baseline_md5_unchanged"] is True
    assert recovered["runtime_entrypoint"] == "runToParquetWorkStealing"
    if stage == "all-parts-commit":
        assert recovered["publication_only"] is True
        assert recovered["runtime_method"] == "ParquetWriterPool.close" and recovered["runtime_workers"] == 0
        assert recovered["source_pages"] == 0 and recovered["relisted_rows"] == 0
    else:
        assert recovered["publication_only"] is False
        assert recovered["runtime_method"] == "runToParquetWorkStealing" and recovered["runtime_workers"] == 1
    after = checkpoint_snapshot(root, fixture)
    after_parts = {part["path"]: part for part in after["parts"]}
    for part in before["parts"]:
        assert after_parts[part["path"]]["md5"] == part["md5"], "recovery rewrote a finalized part"
    assert md5(baseline) == fixture["baseline_md5"]
    for marker in ("manifest.json", ".swath-state.json", "_SUCCESS"):
        assert (root / marker).is_file(), f"Java runtime did not publish {marker}"
    assert after["node"]["status"] == "COMPLETED"
    assert after["node"]["cursor"] == after["node"]["durable_cursor"] == rows[-1]["key"].encode().hex()
    assert sum(part["rows"] for part in after["parts"]) == 513
    if stage == "all-parts-commit":
        assert set(after_parts) == {part["path"] for part in before["parts"]}, "publication-only recovery created new data parts"
        assert after["node"]["pages_emitted"] == before["node"]["pages_emitted"]
        assert after["node"]["generation"] == before["node"]["generation"]
    if stage in ("mid-part", "part-durable", "part-txn"):
        assert ready["candidate_part"] in recovered["orphans_removed"], recovered
    if stage in ("part-commit", "all-parts-commit"):
        assert ready["candidate_part"] not in recovered["orphans_removed"]
        assert after_parts[ready["candidate_part"]]["md5"] == candidate_before["md5"]
    receipt = {"stage": stage, "kill_returncode": returncode, "ready": ready, "java_create": created,
               "initial_checkpoint": initial,
               "before_kill": before, "candidate_before_kill": candidate_before,
               "after_kill": killed, "java_recovery": recovered, "after_recovery": after,
               "result": "passed"}
    (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt


def main():
    if not __debug__:
        raise RuntimeError("resume proof requires Python assertions; do not run with -O")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--go-binary", required=True)
    parser.add_argument("--java-classpath", required=True, help="absolute classpath entries, including wildcard jars")
    parser.add_argument("--java", default="java")
    parser.add_argument("--go", default="go")
    parser.add_argument("--cpus", default="2-17")
    parser.add_argument("--stages", nargs="+", choices=STAGES, default=list(STAGES))
    parser.add_argument("--skip-go-tests", action="store_true")
    options = parser.parse_args()
    output = options.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    # Production reconciliation deletes orphan files, so active datasets live
    # exclusively in /tmp. Only completed evidence is copied to output.
    with tempfile.TemporaryDirectory(prefix="go-parquet-resume-", dir="/tmp") as temporary:
        work = Path(temporary)
        corpus = work / "rows.jsonl"
        rows = write_corpus(corpus)
        started = time.time()
        try:
            receipts = [run_case(options, work, corpus, rows, stage, index == 0 and not options.skip_go_tests)
                        for index, stage in enumerate(options.stages)]
            summary = {"result": "passed", "rows": len(rows), "real_sigkills": len(receipts),
                       "stages": options.stages, "seconds": time.time() - started,
                       "runtime_methods": {receipt["stage"]: receipt["java_recovery"]["runtime_method"] for receipt in receipts},
                       "scope": "actual Java-created SQLite schema; pending tails recover through work-stealing with one worker; fully durable state publishes through production ParquetWriterPool.close with no listing; no CLI, split/concurrency or power-loss claim"}
            (work / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        finally:
            shutil.copytree(work, output, dirs_exist_ok=True)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
