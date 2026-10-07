#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Compare preloaded writers with pinned CPUs and matched row-group count."""

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import random
import statistics
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET


HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--jar", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--measured", type=int, default=10)
    parser.add_argument("--profile", action="store_true", help="separate diagnostic CPU/JFR run; not a timing comparison")
    parser.add_argument("--row-api", choices=("struct", "values", "columns"), default="struct")
    parser.add_argument("--go-binary", type=Path, help="use a frozen executable instead of building the current source")
    parser.add_argument("--codec", choices=("go", "go-entropy", "native"), default="go")
    parser.add_argument("--etag-encoding", choices=("dict", "delta"), default="dict")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if not args.output_dir.is_absolute() or output.is_relative_to(HERE.parents[1]):
        parser.error("output-dir must be absolute and outside the repository")
    if min(args.rounds, args.warmup, args.measured) < 1:
        parser.error("round, warmup and measurement counts must be positive")
    output.mkdir(parents=True, exist_ok=False)
    rows = sum(1 for line in args.input.open() if line.strip())
    if not rows:
        parser.error("comparison requires a nonempty corpus")
    cpus = sorted(os.sched_getaffinity(0))[:2]
    if len(cpus) != 2:
        parser.error("comparison requires two available CPUs")
    affinity = ",".join(map(str, cpus))
    env = dict(os.environ, GOMAXPROCS="2", GOGC="100")
    if args.codec == "native":
        env["CGO_ENABLED"] = "1"
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(HERE.iterdir())
              if p.suffix in (".go", ".java", ".py") or p.name in ("go.mod", "go.sum")}
    (output / "inputs.json").write_text(json.dumps({
        "input": str(args.input), "input_sha256": hashlib.sha256(args.input.read_bytes()).hexdigest(),
        "java_jar": str(args.jar), "java_jar_sha256": hashlib.sha256(args.jar.read_bytes()).hexdigest(),
        "source_sha256": hashes, "rows": rows, "cpus": cpus, "warmup": args.warmup,
        "measured": args.measured, "rounds": args.rounds, "profiling": args.profile,
        "row_api": args.row_api,
        "codec": args.codec,
        "etag_encoding": args.etag_encoding,
    }, indent=2) + "\n")
    commands = []

    def run(label, command, pinned=False):
        if pinned:
            command = ["taskset", "-c", affinity, *command]
        start = time.monotonic()
        with (output / f"{label}.stdout").open("w") as stdout, (output / f"{label}.stderr").open("w") as stderr:
            result = subprocess.run(command, cwd=HERE, env=env, stdout=stdout, stderr=stderr)
        commands.append({"label": label, "command": list(map(str, command)),
                         "elapsed_seconds": time.monotonic() - start, "exit_code": result.returncode})
        (output / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(f"{label} failed; see {output / (label + '.stderr')}")
        return (output / f"{label}.stdout").read_text()

    results = []
    with tempfile.TemporaryDirectory(prefix="swath-writer-compare-") as tmp:
        build = Path(tmp)
        go = str(args.go_binary.resolve()) if args.go_binary else str(build / "gowriter")
        if not args.go_binary:
            tags = ["-tags", "nativezstd"] if args.codec == "native" else []
            run("build-go", ["go", "build", *tags, "-o", go, "."])
        (output / "go-binary-sha256.txt").write_text(hashlib.sha256(Path(go).read_bytes()).hexdigest() + "\n")
        run("go-version", ["go", "version", "-m", go])
        run("compile-java", [str(args.java_home / "bin/javac"), "-cp", str(args.jar),
                             "-d", str(build), str(HERE / "SwathParity.java")])
        java = [str(args.java_home / "bin/java"), "-XX:ActiveProcessorCount=2", "-Xms1g", "-Xmx1g"]
        # The full-row oracle deliberately retains expected and decoded data;
        # it is outside timing and needs more heap than the streaming writer.
        verifier_java = [str(args.java_home / "bin/java"), "-XX:ActiveProcessorCount=2", "-Xms128m", "-Xmx4g"]
        classpath = ["-cp", os.pathsep.join((str(build), str(args.jar))), "SwathParity"]
        if args.profile:
            # CPU diagnosis needs no environment/property capture, which can
            # include credentials inherited by the Java process.
            settings = ET.parse(args.java_home / "lib/jfr/profile.jfc")
            for event in settings.getroot().findall("event"):
                if event.attrib.get("name") in ("jdk.InitialEnvironmentVariable", "jdk.InitialSystemProperty"):
                    event.find("setting[@name='enabled']").text = "false"
            settings.write(build / "profile-safe.jfc", encoding="utf-8", xml_declaration=True)
        count = args.warmup + args.measured
        for pair in range(args.rounds):
            for arm in (("java", "go") if pair % 2 == 0 else ("go", "java")):
                label = f"pair-{pair}-{arm}"
                destination = output / (label + ".parquet")
                if arm == "java":
                    profile = [f"-XX:StartFlightRecording=filename={output / (label + '.jfr')},settings={build / 'profile-safe.jfc'},dumponexit=true"] if args.profile else []
                    text = run(label, [*java, *profile, *classpath, "write", str(args.input),
                                      str(destination), "1024", str(8 << 20), str(count)], pinned=True)
                    runs = [json.loads(line) for line in text.splitlines() if line.startswith('{')]
                    samples = [r["writer_total_nanos"] / 1e6 for r in runs]
                    cpu = [r["process_cpu_nanos"] / 1e6 for r in runs]
                    paths = [Path(r["path"]) for r in runs]
                else:
                    profile = ["-cpu-profile", str(output / (label + ".pprof"))] if args.profile else []
                    codec = ["-codec", args.codec] if args.codec != "go" else []
                    etag = ["-etag-encoding", args.etag_encoding] if args.etag_encoding != "dict" else []
                    text = run(label, [go, "write", "-input", str(args.input), "-output", str(destination),
                                      "-layout", "served", "-sort-mode", "objects", "-key-encoding", "delta",
                                      "-row-api", args.row_api,
                                      "-integer-encoding", "delta", "-page-statistics=false", "-group-rows", str(rows),
                                      "-page-rows", "1024", "-iterations", str(count), *profile, *codec, *etag], pinned=True)
                    runs = json.loads(text)["runs"]
                    samples = [r["write_call_ns"] / 1e6 for r in runs]
                    cpu = [r["process_cpu_ns"] / 1e6 for r in runs]
                    paths = [Path(r["part"]["path"]) for r in runs]
                    if args.profile:
                        run(label + "-profile-top", ["go", "tool", "pprof", "-top", "-nodecount=45", go,
                                                    str(output / (label + ".pprof"))])
                if len(samples) != count:
                    raise SystemExit(f"{label}: expected {count} iterations, found {len(samples)}")
                digests = [hashlib.sha256(p.read_bytes()).hexdigest() for p in paths]
                if len(set(digests)) != 1:
                    raise SystemExit(f"{label}: outputs differ; full per-file validation is required")
                verify_text = run(label + "-verify", [*verifier_java, *classpath, "verify", str(args.input), str(paths[0])])
                verify = json.loads(verify_text)
                if len(verify["row_groups"]) != 1:
                    raise SystemExit(f"{label}: expected one row group as layout control")
                run(label + "-duckdb", ["python3", str(HERE / "verify_duckdb.py"), str(args.input), str(paths[0])])
                measured = samples[args.warmup:]
                result = {"pair": pair, "arm": arm, "all_wall_ms": samples, "all_cpu_ms": cpu,
                          "wall_median_ms": statistics.median(measured),
                          "cpu_median_ms": statistics.median(cpu[args.warmup:]),
                          "timed_half_ratio": statistics.median(measured[len(measured)//2:]) / statistics.median(measured[:max(1,len(measured)//2)]),
                          "bytes": paths[0].stat().st_size, "sha256": digests[0],
                          "key_pages": verify["key_pages"], "column_totals": verify["column_totals"]}
                results.append(result)
                (output / "arms.json").write_text(json.dumps(results, indent=2) + "\n")
                print(label + " complete", flush=True)
    ratios = []
    for pair in range(args.rounds):
        arms = {r["arm"]: r for r in results if r["pair"] == pair}
        ratios.append(arms["go"]["wall_median_ms"] / arms["java"]["wall_median_ms"])
    rng = random.Random(20261007)
    boot = sorted(math.exp(statistics.mean(math.log(rng.choice(ratios)) for _ in ratios)) for _ in range(5000))
    summary = {"profiling": args.profile, "pair_wall_ratios_go_over_java": ratios,
               "geometric_mean_ratio": math.exp(statistics.mean(map(math.log, ratios))),
               "bootstrap_95pct_process_pair_interval": [boot[125], boot[4874]],
               "note": "Diagnostic profiles are not timing evidence. Five process pairs are exploratory; page encodings may still differ."}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
