#!/usr/bin/env python3
"""Run a core-only Hardwood API smoke; downloads/builds/output stay outside the repo."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import urllib.request

VERSION = "1.1.0.Beta1"
URL = f"https://repo.maven.apache.org/maven2/dev/hardwood/hardwood-core/{VERSION}/hardwood-core-{VERSION}.jar"
SHA256 = "0b0207777e22d45942da05ec3c5bc4530222ba0b1e648c101aa024574d0e01d7"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--java", required=True, help="JDK 25 java executable")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    repo = Path(__file__).resolve().parents[2]
    if output == repo or repo in output.parents:
        parser.error("Output must be outside the repository")
    output.mkdir(parents=True, exist_ok=True)
    jar = output / f"hardwood-core-{VERSION}.jar"
    if not jar.exists():
        urllib.request.urlretrieve(URL, jar)
    if hashlib.sha256(jar.read_bytes()).hexdigest() != SHA256:
        raise RuntimeError("Hardwood jar checksum mismatch")
    parquet = output / "hardwood-smoke.parquet"
    command = [args.java, "--enable-native-access=ALL-UNNAMED", "-cp", str(jar),
               str(Path(__file__).with_name("HardwoodSmoke.java")), str(parquet)]
    result = subprocess.run(command, text=True, capture_output=True)
    (output / "java.stdout.txt").write_text(result.stdout)
    (output / "java.stderr.txt").write_text(result.stderr)
    result.check_returncode()
    import duckdb  # Independent read check; install in an external virtual environment if needed.
    with duckdb.connect() as connection:
        values = connection.execute("SELECT count(*), min(size), max(size) FROM read_parquet(?)", [str(parquet)]).fetchone()
        groups = connection.execute("SELECT count(DISTINCT row_group_id) FROM parquet_metadata(?)", [str(parquet)]).fetchone()[0]
    if values != (513, 0, 512) or groups != 6:
        raise AssertionError((values, groups))
    receipt = {"library": "Hardwood", "version": VERSION, "jar_url": URL, "jar_sha256": SHA256,
               "command": command, "rows": values[0], "min_size": values[1], "max_size": values[2],
               "row_groups": groups, "classpath": "hardwood-core only", "duckdb_version": duckdb.__version__,
               "scope": "Two-column uncompressed API smoke; not Swath format, null-value, performance, or remote IO proof"}
    (output / "result.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
