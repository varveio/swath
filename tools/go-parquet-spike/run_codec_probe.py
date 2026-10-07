#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0

"""Compare Zstd implementations on identical actual encoded Parquet payloads."""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess


def run(command, directory, name):
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    (directory / f"{name}-stdout.json").write_text(result.stdout)
    (directory / f"{name}-stderr.txt").write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(f"{name} failed: {result.stderr[-2000:]}")
    return json.loads(result.stdout)


def medians(report):
    return {"wall_ms": statistics.median(row["wall_ns"] for row in report["runs"]) / 1e6,
            "cpu_ms": statistics.median(row["cpu_ns"] for row in report["runs"]) / 1e6,
            "compressed_bytes": report["runs"][-1]["compressed_bytes"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--go-binary", required=True)
    parser.add_argument("--java-classpath", required=True)
    parser.add_argument("--java", default="java")
    options = parser.parse_args()
    output = options.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    blocks = output / "blocks.jsonl"
    go_frames = output / "go-frames.jsonl"
    java_frames = output / "java-frames.jsonl"
    extract = run(["taskset", "-c", "2-17", options.go_binary, "codec-probe", "--mode", "extract",
                   "--parquet", str(options.parquet.resolve()), "--blocks", str(blocks)], output, "extract")
    extraction_check = run(["taskset", "-c", "2-17", options.go_binary, "codec-probe", "--mode", "self-check"],
                           output, "extraction-self-check")
    java = ["taskset", "-c", "0,1", options.java, "--enable-native-access=ALL-UNNAMED",
            "-cp", options.java_classpath, "CodecInterop"]
    source_validation = run(["taskset", "-c", "2-17"] + java[3:] +
                            ["source-verify", str(blocks), str(options.parquet.resolve())],
                            output, "native-original-source")
    go = ["taskset", "-c", "0,1", options.go_binary, "codec-probe"]
    reports = {"go": [], "java": []}
    # Alternate ordering across pairs so one codec is not always measured first.
    for pair in range(3):
        for arm in (("go", "java") if pair % 2 == 0 else ("java", "go")):
            if arm == "go":
                command = go + ["--blocks", str(blocks), "--compressed-output", str(go_frames),
                                "--warmup", "10", "--iterations", "20"]
            else:
                command = java + ["bench", str(blocks), str(java_frames), "10", "20"]
            reports[arm].append(run(command, output, f"pair-{pair}-{arm}"))
    go_validation = run(["taskset", "-c", "2-17"] + go[3:] +
                        ["--mode", "verify", "--blocks", str(blocks), "--compressed", str(java_frames)],
                        output, "go-decodes-native")
    java_validation = run(["taskset", "-c", "2-17"] + java[3:] +
                          ["verify", str(blocks), str(go_frames)], output, "native-decodes-go")
    for arm in reports.values():
        for report in arm:
            if report["blocks"] != reports["go"][0]["blocks"] or report["raw_bytes"] != reports["go"][0]["raw_bytes"]:
                raise RuntimeError("codec inputs do not match")
    summary = {"extraction": extract, "blocks_sha256": hashlib.sha256(blocks.read_bytes()).hexdigest(),
               "extraction_self_check": extraction_check,
               "native_original_source_validation": source_validation,
               "pairs": {arm: [medians(report) for report in runs] for arm, runs in reports.items()},
               "go_decodes_native": go_validation, "native_decodes_go": java_validation,
               "scope": "identical encoded page values/dictionaries; no Parquet encoding, footer, fsync, JNI integration or end-to-end speedup claim"}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
