#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Run the bounded interoperability probe, retaining raw receipts outside Git."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--java-home", type=Path, required=True)
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--profile", choices=("objects", "wide"), default="objects")
    parser.add_argument("--integer-encoding", choices=("dict", "delta"), default="dict")
    parser.add_argument("--page-statistics", choices=("true", "false"), default="true")
    parser.add_argument("--key-encoding", choices=("both", "dict", "delta"), default="both")
    parser.add_argument("--go-only", action="store_true", help="reuse the separately recorded Java benchmark")
    parser.add_argument("--row-api", choices=("struct", "values", "columns"), default="struct")
    parser.add_argument("--single-page-order", choices=("library", "ascending"), default="library")
    parser.add_argument("--etag-encoding", choices=("dict", "delta"), default="dict")
    parser.add_argument("--java-jar", type=Path, help="override the Java reader/writer artifact, for compatibility checks")
    parser.add_argument("--codec", choices=("go", "go-entropy", "native"), default="go")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if not args.output_dir.is_absolute() or output.is_relative_to(REPO):
        parser.error("output-dir must be an absolute path outside the repository")
    output.mkdir(parents=True, exist_ok=False)
    def source_hashes():
        return {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(HERE.iterdir())
                if p.suffix in (".go", ".java", ".py") or p.name in ("go.mod", "go.sum")}

    sources = source_hashes()
    (output / "source-sha256.json").write_text(json.dumps(sources, indent=2) + "\n")
    receipts = []
    serving_results = []
    environment = dict(os.environ, GOMAXPROCS="2")
    if args.codec == "native":
        environment["CGO_ENABLED"] = "1"
    encodings = ("dict", "delta") if args.key_encoding == "both" else (args.key_encoding,)
    if args.single_page_order == "ascending" and encodings != ("delta",):
        parser.error("ascending single-page order requires --key-encoding delta")
    go_policy = ["-integer-encoding", args.integer_encoding,
                 "-page-statistics=" + args.page_statistics, "-row-api", args.row_api,
                 "-single-page-order", args.single_page_order, "-etag-encoding", args.etag_encoding,
                 "-codec", args.codec]

    def run(label, command, cwd=HERE):
        started = time.monotonic()
        with (output / f"{label}.stdout").open("w") as stdout, (output / f"{label}.stderr").open("w") as stderr:
            result = subprocess.run(
                command, cwd=cwd, env=environment, stdout=stdout, stderr=stderr, text=True
            )
        receipt = {"label": label, "command": [str(x) for x in command], "cwd": str(cwd),
                   "elapsed_seconds": time.monotonic() - started, "exit_code": result.returncode}
        receipts.append(receipt)
        (output / "commands.json").write_text(json.dumps(receipts, indent=2) + "\n")
        if result.returncode:
            raise SystemExit(f"{label} failed; see {output / (label + '.stderr')}")
        return output / f"{label}.stdout"

    def measured(label, command):
        return run(label, ["/usr/bin/time", "-f", "%e %U %S %M", "-o",
                           str(output / f"{label}.time"), *command])

    with tempfile.TemporaryDirectory(prefix="swath-go-parquet-") as temporary:
        build = Path(temporary)
        jar = args.java_jar.resolve() if args.java_jar else REPO / "swath-cli/build/libs/swath.jar"
        if not jar.is_file():
            parser.error("build the current :swath-cli:shadowJar first")
        java_jar_sha256 = hashlib.sha256(jar.read_bytes()).hexdigest()
        run("compile-java", [str(args.java_home / "bin/javac"), "-cp", str(jar),
                             "-d", str(build), str(HERE / "SwathParity.java")])
        go = str(build / "gowriter")
        tags = ["-tags", "nativezstd"] if args.codec == "native" else []
        run("compile-go", ["go", "build", *tags, "-o", go, "."])
        run("go-binary-versions", ["go", "version", "-m", go])
        run("java-version", [str(args.java_home / "bin/java"), "-version"])
        run("duckdb-version", ["duckdb", "--version"])
        java = [str(args.java_home / "bin/java"), "-XX:ActiveProcessorCount=2",
                "-cp", os.pathsep.join((str(build), str(jar))), "SwathParity"]

        def verify(label, corpus, parquet):
            report = run(label + "-java", [*java, "verify", str(corpus), str(parquet)])
            run(label + "-go", [go, "verify", "-input", str(corpus), "-parquet", str(parquet)])
            run(label + "-duckdb", ["python3", str(HERE / "verify_duckdb.py"), str(corpus), str(parquet)])
            decoded = json.loads(report.read_text())
            serving_results.append({"label": label, **decoded["serving"]})
            return decoded

        if not args.bench:
            cases = [
                ("empty", 0, "empty", "served"),
                ("single", 1, "onepage", "served"),
                ("pages", 4096, "objects", "served"),
                ("tail", 4097, "tinytail", "served"),
                ("edges", 1153, "edges", "served"),
                ("truncation", 1024, "unicode-truncation", "direct"),
                ("equal-minima", 1024, "equal-page-minima", "direct"),
                ("mixed", 256, "mixed", "served"),
                ("versions", 256, "versions", "served"),
            ]
            results = []
            for name, count, profile, layout in cases:
                corpus = output / f"{name}.jsonl"
                run(name + "-generate", [*java, "generate", str(corpus), str(count), profile])
                reference = output / f"{name}-java.parquet"
                run(name + "-java-write", [*java, "write", str(corpus), str(reference), "32", "8192", "1"])
                reference_report = verify(name + "-java-reference", corpus, reference)
                for encoding in encodings:
                    candidate = output / f"{name}-go-{encoding}.parquet"
                    run(name + "-go-" + encoding + "-write", [
                        go, "write", "-input", str(corpus), "-output", str(candidate),
                        "-layout", layout, "-key-encoding", encoding,
                        "-page-rows", "32", "-group-rows", "128", *go_policy,
                    ])
                    report = verify(name + "-go-" + encoding, corpus, candidate)
                    results.append({"case": name, "rows": count, "layout": layout,
                                    "key_encoding": encoding, "java_reference_serving": reference_report["serving"],
                                    "go_file_serving": report["serving"]})
                    (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
        else:
            corpus = output / f"{args.profile}.jsonl"
            run("generate", [*java, "generate", str(corpus), "200000", args.profile])
            (output / "input-sha256.txt").write_text(hashlib.sha256(corpus.read_bytes()).hexdigest() + "\n")
            if not args.go_only:
                measured("java-write", [*java, "write", str(corpus), str(output / "java.parquet"),
                                        "1024", str(8 << 20), "6"])
            for encoding in encodings:
                measured("go-" + encoding + "-write", [
                    go, "write", "-input", str(corpus), "-output", str(output / f"go-{encoding}.parquet"),
                    "-layout", "served", "-key-encoding", encoding,
                    "-page-rows", "1024", "-group-rows", "65536", "-iterations", "6", *go_policy,
                ])
            names = ([] if args.go_only else ["java"]) + [f"go-{encoding}" for encoding in encodings]
            for name in names:
                paths = sorted(output.glob(name + ".*.parquet"))
                if len(paths) != 6:
                    raise SystemExit(f"expected six output parts for {name}, found {paths}")
                hashes = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
                (output / f"{name}-sha256.json").write_text(json.dumps(hashes, indent=2) + "\n")
                # Identical files need one full cross-read. Otherwise check all six.
                selected = paths[:1] if len(set(hashes.values())) == 1 else paths
                for path in selected:
                    verify(path.stem + "-verify", corpus, path)
    rejected = [result for result in serving_results
                if result.get("tested") and result.get("compatible") is False]
    final = {"output": str(output), "commands": len(receipts),
             "file_contract_checks_passed": True, "serving_rejections": rejected,
             "selected_java_jar": str(jar), "selected_java_jar_sha256": java_jar_sha256,
             "all_tested_files_serve_in_selected_java": not rejected,
             "selected_java_jar_unchanged": java_jar_sha256 == hashlib.sha256(jar.read_bytes()).hexdigest(),
             "sources_unchanged_during_run": sources == source_hashes()}
    (output / "result.json").write_text(json.dumps(final, indent=2) + "\n")
    print(json.dumps(final))
    if not final["sources_unchanged_during_run"]:
        raise SystemExit("source files changed during this run; retain as exploratory evidence only")
    if not final["selected_java_jar_unchanged"]:
        raise SystemExit("Java artifact changed during this run; retain as exploratory evidence only")


if __name__ == "__main__":
    main()
