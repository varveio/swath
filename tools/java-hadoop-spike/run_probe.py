#!/usr/bin/env python3
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
"""Reproduce stock failure and a narrowly patched public-API experiment; no production edits."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.request
import zipfile

SOURCE_URL = "https://raw.githubusercontent.com/apache/parquet-java/apache-parquet-1.18.1/parquet-hadoop/src/main/java/org/apache/parquet/ParquetReadOptions.java"
SOURCE_SHA256 = "459b2bf3f1827535364e71950b6a7b69ce4f4293ee304956307228b6dba0c2bb"
ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent.parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jdk", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / "canonical.parquet").exists():
        raise RuntimeError("Use a fresh output directory")
    receipts = []
    implementation_hashes = {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in ROOT.rglob("*") if path.is_file()
    }

    def run(name, command, *, expected=0, cwd=REPO):
        result = subprocess.run(list(map(str, command)), cwd=cwd, capture_output=True, text=True, env={**os.environ, "JAVA_HOME": str(args.jdk)})
        (output / (name + ".txt")).write_text(result.stdout + result.stderr)
        receipts.append({"name": name, "command": list(map(str, command)), "exit_code": result.returncode})
        if result.returncode != expected:
            raise RuntimeError(f"{name}: exit {result.returncode}, expected {expected}; see receipt")
        return result

    java, javac = args.jdk / "bin/java", args.jdk / "bin/javac"
    run("java-version", [java, "-version"])
    # Build caches and compiled output live outside the repository.
    with tempfile.TemporaryDirectory(prefix="swath-java-hadoop-") as scratch:
        scratch = Path(scratch)
        base = [REPO / "gradlew", "-p", ROOT, "--project-cache-dir", scratch / "gradle-cache", "--no-daemon", "-q", f"-PprobeBuildDir={scratch / 'build'}"]
        run("compile", base + ["classes"])
        runtime = run("runtime-classpath", base + ["classpath"]).stdout.strip()
        compile_cp = run("compile-classpath", base + ["compileClasspath"]).stdout.strip()
        artifacts = []
        for item in runtime.split(":"):
            path = Path(item)
            with zipfile.ZipFile(path) as jar:
                hadoop_entries = [name for name in jar.namelist() if name.startswith("org/apache/hadoop/")]
            if hadoop_entries or "/org.apache.hadoop/" in item:
                raise AssertionError(f"Hadoop runtime entry: {item}")
            artifacts.append({"path": item, "bytes": path.stat().st_size,
                              "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                              "hadoop_class_entries": len(hadoop_entries)})
        (output / "runtime-artifacts.json").write_text(json.dumps(artifacts, indent=2) + "\n")
        classes = scratch / "build/classes/java/main"
        parquet_artifact = next(a for a in artifacts if Path(a["path"]).name == "parquet-hadoop-1.18.1.jar")
        ordinary = f"{classes}:{runtime}"
        command_base = [java, "--enable-native-access=ALL-UNNAMED", "-cp"]
        run("write", command_base + [ordinary, "CanonicalProbe", output / "canonical.parquet"])
        blocked = run("stock-builder", command_base + [ordinary, "ReadOptionsProbe", "stock", output / "canonical.parquet"], expected=1)
        if "HadoopCodecs.newFactory" not in blocked.stderr or "NoClassDefFoundError" not in blocked.stderr:
            raise AssertionError("Stock failure was not the expected eager Hadoop codec linkage")
        source = urllib.request.urlopen(SOURCE_URL, timeout=30).read()
        if hashlib.sha256(source).hexdigest() != SOURCE_SHA256:
            raise AssertionError("Pinned upstream source hash changed")
        (output / "upstream-ParquetReadOptions.java").write_bytes(source)
        work = scratch / "source"
        target = work / "parquet-hadoop/src/main/java/org/apache/parquet/ParquetReadOptions.java"
        target.parent.mkdir(parents=True)
        target.write_bytes(source)
        run("apply-patch", ["patch", "--batch", "--forward", "-p1", "-i", ROOT / "read-options-1.18.1.patch"], cwd=work)
        patched = output / "patched-classes"
        patched.mkdir()
        run("compile-patch", [javac, "-cp", f"{classes}:{compile_cp}", "-d", patched, target,
                              ROOT / "patched-api/PatchedOptions.java"])
        patched_cp = f"{patched}:{ordinary}"
        run("patched-reader", command_base + [patched_cp, "ReadOptionsProbe", "patched", output / "canonical.parquet"])
        # Separate JVMs: the expected dump uses only the untouched released artifact.
        stock_defaults = run("stock-defaults", command_base + [f"{classes}:{compile_cp}", "DefaultsProbe", "stock"])
        patched_defaults = run("patched-defaults", command_base + [f"{patched}:{classes}:{compile_cp}", "DefaultsProbe", "patched"])
        if stock_defaults.stdout != patched_defaults.stdout:
            raise AssertionError("Patched defaults/configuration differ from untouched release")
        (output / "defaults-comparison.json").write_text(json.dumps({
            "equal": True, "stock_jar_sha256": parquet_artifact["sha256"],
            "stock_class_shadowed": False, "scenarios": 2,
            "options_dump_sha256": hashlib.sha256(stock_defaults.stdout.encode()).hexdigest()
        }, indent=2) + "\n")
        run("jdeps", [args.jdk / "bin/jdeps", "--multi-release", "25", "--ignore-missing-deps",
                      "-recursive", "-verbose:class", "-cp", runtime, parquet_artifact["path"]])
        linkage = (output / "jdeps.txt").read_text()
        missing = [line for line in linkage.splitlines() if "org.apache.hadoop." in line and "not found" in line]
        (output / "artifact-linkage.json").write_text(json.dumps({
            "parquet_artifact": parquet_artifact["path"],
            "hadoop_missing_class_reference_lines": len(missing),
            "note": "missing-reference lines, not distinct classes; artifact linkage remains"
        }, indent=2) + "\n")
        (output / "maven-metadata.xml").write_bytes(urllib.request.urlopen(
            "https://repo.maven.apache.org/maven2/org/apache/parquet/parquet-hadoop/maven-metadata.xml", timeout=30).read())
        report = {"parquet_version": "1.18.1", "source_url": SOURCE_URL,
                  "source_sha256": hashlib.sha256(source).hexdigest(),
                  "runtime_hadoop_artifacts": 0, "runtime_hadoop_class_entries": 0,
                  "stock_public_builder": "blocked before caller overrides",
                  "patched_public_overload": "passed", "rows": 2048,
                  "limits": ["upstream patch proposal; not a released API", "compile-time Hadoop remains",
                             "stock parquet-hadoop artifact remains", "not full production Swath integration or performance"],
                  "commands": receipts}
        after_hashes = {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in ROOT.rglob("*") if path.is_file()
        }
        if implementation_hashes != after_hashes:
            raise AssertionError("Probe source changed during execution")
        (output / "implementation-hashes.json").write_text(json.dumps({
            "before": implementation_hashes, "after": after_hashes, "unchanged": True
        }, indent=2) + "\n")
        frozen = output / "probe-source"
        import shutil
        shutil.copytree(ROOT, frozen)
        (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({key: value for key, value in report.items() if key != "commands"}, indent=2))


if __name__ == "__main__":
    main()
