#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
#
# make-fixtures.sh OUT: writes the Java-written replay fixtures the gated
# store tests read (SWATH_REPLAY_FIXTURES=ROOT reads ROOT/fixtures, so pass
# ROOT/fixtures as OUT). OUT must not exist or be empty. It writes:
#
#   synthetic-10k/{capture,sorted}       10,000 synthetic rows, one sorted part
#   synthetic-1m/{capture,sorted}        1,000,000 rows, the default sizing
#   synthetic-1m/sorted-rolled           the same capture rolled into many
#                                        parts and row groups
#   evidence-gcs/{corpus.csv,capture,sorted}
#                                        the GCS evidence corpus (1,084 names)
#   evidence-azure/{corpus.csv,capture}  the Azure evidence corpus (5,083
#                                        names up to 2,010 bytes, which the
#                                        Java sort-fixture refuses: capture only)
#
# Inputs:
#   - this checkout (the evidence archive docs/replay-provider-evidence.tar.gz
#     and evidence_csv.py beside this script);
#   - swath-replay on PATH: the Java replay distribution
#     (./gradlew :swath-replay:installDist), whose lib/ holds the DuckDB
#     JDBC driver that writes the captures;
#   - JAVA_HOME: the JDK the distribution runs on (25 or later);
#   - python3.
#
# Captures are written by DuckDB (the SQL is below) as one zstd Parquet file
# of the 13-column canonical schema, then sorted by the Java
# `swath-replay sort-fixture`. Paths must not contain a single quote (they
# are spliced into SQL). About 30 s and 120 MB on a 4-vCPU host.
set -eu

if [ $# -ne 1 ]; then
	echo "usage: $0 OUT" >&2
	exit 2
fi
out=$1
if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
	echo "$out is not empty" >&2
	exit 2
fi
: "${JAVA_HOME:?JAVA_HOME must name the JDK the replay distribution runs on}"
replay=$(command -v swath-replay) || { echo "swath-replay is not on PATH" >&2; exit 2; }
case $out in *"'"*) echo "OUT must not contain a single quote" >&2; exit 2 ;; esac

here=$(cd "$(dirname "$0")" && pwd)
checkout=$(cd "$here/../../.." && pwd)
lib=$(cd "$(dirname "$(readlink -f "$replay")")/../lib" && pwd)
jar=$(ls "$lib"/duckdb_jdbc-*.jar 2>/dev/null | head -n 1)
[ -n "$jar" ] || { echo "no duckdb_jdbc-*.jar in $lib" >&2; exit 2; }
mkdir -p "$out"
out=$(cd "$out" && pwd)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# DuckSql runs each argument as one statement on an in-memory DuckDB.
cat >"$work/DuckSql.java" <<'JAVA'
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class DuckSql {
    public static void main(String[] args) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("SET threads TO 2");
            for (String sql : args) {
                s.execute(sql);
            }
        }
    }
}
JAVA
duck() {
	"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED -cp "$jar" "$work/DuckSql.java" "$@"
}

# synthetic N DIR: N synthetic current objects under four top-level
# prefixes and dated directories, unique keys.
synthetic() {
	mkdir -p "$2/capture"
	duck "COPY (
SELECT printf('logs/%s/dt=2026-%02d-%02d/part-%05d.json.gz', ['alpha','beta','gamma','delta'][1 + (i % 4)],
              1 + (i // 400) % 12, 1 + (i // 13) % 28, i) AS key,
       CAST(1000 + (i * 7919) % 1000000 AS BIGINT) AS size,
       TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_microseconds(CAST(i AS BIGINT) * 1000003) AS last_modified,
       md5(CAST(i AS VARCHAR)) AS etag, 'STANDARD' AS storage_class,
       CAST(NULL AS VARCHAR) AS version_id, true AS is_latest, false AS is_delete_marker,
       CAST(NULL AS VARCHAR) AS owner_id, CAST(NULL AS VARCHAR) AS owner_display_name,
       CAST(NULL AS VARCHAR) AS checksum_algorithm, CAST(NULL AS VARCHAR) AS checksum_type,
       'OBJECT' AS row_type
FROM range($1) t(i)
) TO '$2/capture/part-00000.parquet' (FORMAT parquet, COMPRESSION zstd)"
	swath-replay sort-fixture --capture "$2/capture" --output "$2/sorted"
}

# evidence PROVIDER MANIFEST DIR: the corpus's names, sizes and times as
# current objects; every other column null.
evidence() {
	mkdir -p "$3/capture"
	python3 -I "$here/evidence_csv.py" "$1" "$2" "$3/corpus.csv"
	duck "COPY (
SELECT decode(unhex(name_hex)) AS key, CAST(size AS BIGINT) AS size,
       make_timestamptz(CAST(updated_us AS BIGINT)) AS last_modified,
       CAST(NULL AS VARCHAR) AS etag, CAST(NULL AS VARCHAR) AS storage_class,
       CAST(NULL AS VARCHAR) AS version_id, true AS is_latest, false AS is_delete_marker,
       CAST(NULL AS VARCHAR) AS owner_id, CAST(NULL AS VARCHAR) AS owner_display_name,
       CAST(NULL AS VARCHAR) AS checksum_algorithm, CAST(NULL AS VARCHAR) AS checksum_type,
       'OBJECT' AS row_type
FROM read_csv('$3/corpus.csv', header=true, columns={'name_hex':'VARCHAR','size':'BIGINT','updated_us':'BIGINT'})
) TO '$3/capture/part-00000.parquet' (FORMAT parquet, COMPRESSION zstd)"
}

synthetic 10000 "$out/synthetic-10k"
synthetic 1000000 "$out/synthetic-1m"
# Small final files and row groups: many parts and row groups from one capture.
SWATH_REPLAY_OPTS="-Dswath.sort.final-file-bytes=4194304 -Dswath.sort.final-row-group-bytes=262144" \
	swath-replay sort-fixture --capture "$out/synthetic-1m/capture" --output "$out/synthetic-1m/sorted-rolled"

mkdir -p "$work/evidence"
tar -xzf "$checkout/docs/replay-provider-evidence.tar.gz" -C "$work/evidence"
evidence gcs "$work/evidence/gcs/manifest.json-v1.json" "$out/evidence-gcs"
swath-replay sort-fixture --capture "$out/evidence-gcs/capture" --output "$out/evidence-gcs/sorted"
evidence azure "$work/evidence/azure/manifest.2026-06-06.json" "$out/evidence-azure"
# The fixture identity hashes each part's name, size, modification time and
# stamp: a fixed time makes it reproducible (the Java writer's parts are
# deterministic in size and stamps, not in every footer byte).
find "$out" -type f -exec touch -d '2026-01-01T00:00:00Z' {} +
echo "fixtures written to $out"
