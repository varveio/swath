#!/usr/bin/env python3
"""Freeze source identity around the protocol run; retain one self-contained evidence root."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("/workspaces/swath-data/java-storage-spike-20261007/remote-validated"))
    args = parser.parse_args()
    output = args.out.resolve()
    source = Path(__file__).resolve().parent
    repo = source.parents[1]
    if output.is_relative_to(repo):
        raise ValueError("evidence output must be outside the repository")
    if output.exists():
        raise ValueError("fresh nonexisting evidence directory required")
    output.mkdir(parents=True)
    jar = repo / "swath-cli/build/libs/swath.jar"
    jar_before = digest(jar)
    files = sorted([*source.glob("*.java"), source / "build.gradle", source / "settings.gradle", source / "README.md", source / "run_probe.py"])
    before = {str(file.relative_to(repo)): digest(file) for file in files}
    command = [str(repo / "gradlew"), "-p", str(source), "--project-cache-dir", "/tmp/java-remote-spike/gradle-cache", "runProbe", f"-PoutputRoot={output}", "--no-daemon"]
    with (output / "run.log").open("w") as log:
        subprocess.run(command, cwd=repo, stdout=log, stderr=subprocess.STDOUT, check=True)
    after = {str(file.relative_to(repo)): digest(file) for file in files}
    if before != after or jar_before != digest(jar):
        raise RuntimeError("source changed during run; evidence cannot be cited")
    launcher_result = subprocess.run([str(repo / "gradlew"), "-p", str(source),
        "--project-cache-dir", "/tmp/java-remote-spike/gradle-cache", "jdkIdentity", "--no-daemon", "-q"],
        cwd=repo, capture_output=True, text=True, check=True)
    launcher = [line.strip() for line in launcher_result.stdout.splitlines() if line.strip().endswith("/bin/java")]
    if len(launcher) != 1:
        raise RuntimeError("could not identify Gradle Java25 launcher")
    jdk_version = subprocess.run([launcher[0], "-version"], capture_output=True, text=True, check=True).stderr.strip()
    summary = json.loads((output / "summary.json").read_text())
    receipt = {
        "command": command,
        "source_sha256": before,
        "sources_unchanged": True,
        "fixture_sha256": digest(output / "fixture.parquet"),
        "swath_jar_sha256": jar_before,
        "swath_jar_unchanged": True,
        "versions": {"aws_sdk_s3": "2.31.78", "google_storage_json_sdk": "v1-rev20260524-2.0.0", "google_http_client_apache": "2.2.0", "parquet_java": "1.15.1"},
        "jdk": {"gradle_launcher": launcher[0], "version": jdk_version},
        "measured_full_operation": "open/footer and decode every physical field/null of all20000rows",
        "measured_indexed_operation": "open/footer plus one keycursor8000<=key<8010 and objectRange same10rows withowner=true",
        "limits": ["local fakeHTTP and separate LocalStack smoke, no livecloud", "single rowgroup synthetic20k fixture", "shared SDK request cancellation not implemented", "overlapping GET bytes counted repeatedly, metadata/headers excluded", "no IO planner or throughput claim"],
        "summary": summary,
    }
    (output / "evidence.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
