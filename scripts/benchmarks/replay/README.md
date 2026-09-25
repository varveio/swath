<!-- SPDX-License-Identifier: Apache-2.0 -->
# Replay HTTP benchmark

`ReplayHttpBench` is an out-of-process Java 25 client for S3, GCS JSON, and Azure Blob
listing walks. It uses StAX/Jackson streaming parsers, each protocol's own continuation
token, and an independent DuckDB scan of the sorted fixture's raw key bytes. Every full
walk, or every client's assigned range in partitioned mode, must return the exact row
count and ordered SHA-256 digest. Digest input is
the sequence of four-byte big-endian key lengths followed by raw key bytes. A duplicate,
gap, reordered key, malformed response, missing token, or failed HTTP request fails the
run. Successful receipts include attempted and successful request counts. Failed runs
print those counters before exiting nonzero.

Compile the driver against a frozen `installDist` so it can use that distribution's
Jackson Core and DuckDB JDBC jars. The driver itself runs in a separate JVM from the
server; the server binary can be the unmodified baseline or a candidate:

```sh
BASE=/path/to/frozen-baseline/swath-replay
JDK="$JAVA_HOME"
mkdir -p /tmp/swath-replay-bench-classes
"$JDK/bin/javac" -cp "$BASE/lib/*" -d /tmp/swath-replay-bench-classes \
  scripts/benchmarks/replay/ReplayHttpBench.java
```

The paired runner starts one server per arm, pins server and client to disjoint CPU sets,
performs an oracle-checked warmup at the declared concurrency in the same driver JVM,
and then runs
12 interleaved baseline/candidate pairs. It writes every attempt and its before/after
server metrics to `rounds.jsonl`, plus all paired ratios and log-ratio 95% confidence
intervals to `summary.json`. A missing pair or an interval crossing a budget fails the
panel. It reports marker-aligned server/client process CPU, sampled RSS peak, direct-buffer
bytes before/after, and JVM GC allocation **estimate**; that estimate is not an exact
allocation counter. Peak direct memory and exact allocation require a separate sampled/JFR arm before
those resource acceptance claims can pass.

Example S3 gate invocation, after substituting a candidate distribution path and a
durable output directory:

```sh
python3 scripts/benchmarks/replay/run_pair.py \
  --baseline "$BASE/bin/swath-replay" --candidate "$CANDIDATE/bin/swath-replay" \
  --fixture /path/to/sorted-fixture \
  --fixture-glob '/path/to/sorted-fixture/*.parquet' \
  --classpath "/tmp/swath-replay-bench-classes:$BASE/lib/*" \
  --clients 16 --page-size 1000 --connections 16 \
  --partitioned --repetitions 10 \
  --server-cpus 0-7 --client-cpus 8-15 \
  --server-java-opts '-Xms4g -Xmx4g -XX:MaxDirectMemorySize=512m -Djdk.nio.maxCachedBufferSize=262144' \
  --output /path/to/durable-receipts/s3-c16
```

Use only CPU IDs available to the current process. Run an independent `--pilot` first and
set `--repetitions` before round 1 for a conservative 50-second target, above the
30-second default minimum;
the same fixed work and partitions run in every pair. Partitioned mode assigns contiguous,
disjoint byte-key ranges whose union is the complete fixture. Each client's digest is
checked separately against its fixture range. S3 has no upper listing bound, so an end
page may emit keys owned by the next partition; those rows count in emitted rows, encoded
bytes, requests and CPU, while the owned-object throughput denominator counts each key
once. The aggregate corpus must exceed the configured cache footprint. Retain a separate
unbounded single-client full walk and staggered same-bound sharing tests.

Predeclare JVM flags, fixture, server options, response budgets, cache state, and per-arm
protocols before timing. For native comparison, point both distribution paths at the
candidate, set `--baseline-protocol s3`, set `--candidate-protocol gcs` or `azure`, and pass
the candidate's enablement flags with `--baseline-option` and `--candidate-option`. For
the S3 regression panel, compare the frozen baseline and candidate distributions. The
native comparison's p99 number is characterization until an
explicit cross-protocol latency budget is adopted; the S3 regression budget is 1.10.
The suite currently covers flat walks. One-key seeks, delimiters, narrowed
GCS ranges, mixed offered traffic, slow readers, and 512 outstanding long-key traffic
need their own declared workload modes and receipts before final epic acceptance.

`resource_fault.py` is the separate opt-in response-resource arm. It starts a candidate
server and writes `receipt.json`, a server log, before/after `/metrics` snapshots,
sampled RSS peak, direct-buffer meter values, and `jcmd VM.native_memory` summaries to
the declared durable output directory. With `--jfr`, it also starts and dumps a JFR
recording. It has three modes: `normal512` completes 512 concurrent S3 long-key pages
with zero refusals under an adequately sized byte budget; `slow` holds unread sockets,
checks a labeled overload when requested, then checks permit/byte recovery after
disconnect; `trickle` reads one byte at a time and checks the total write deadline.
The run fails if a slow socket never holds a callback, so a small response cannot
masquerade as a backpressure test.

Declare an upper bound `M` on the encoded body length, then size the normal arm with
`B >= C × max(initialCapacity, ceil(2.5 × M))`, where `C` is concurrent responses and
`initialCapacity = min(cap, max(4096, 512 + min(pageSize,1000) × 320))` for S3. The
2.5 factor conservatively covers both old and new arrays during a 1.5× growth copy.
The driver rejects a budget below this bound and any response above `M`. This byte
arithmetic excludes decoded rows, the shared cache, Jetty buffers, JDK direct buffers,
and transient garbage; predeclare `-Xmx`, `MaxDirectMemorySize`, and
`jdk.nio.maxCachedBufferSize=262144` separately. The `slow` arm may intentionally use
a smaller budget to prove 503 refusal and recovery; it is not a throughput pass.

For a deterministic long-key resource corpus, generate a 100,000-object capture with
1,024-byte keys and stamp it through `sort-fixture` into a new durable directory:

```sh
python3 scripts/benchmarks/replay/make_longkey_fixture.py \
  --replay "$CANDIDATE/bin/swath-replay" \
  --output /path/to/durable-fixtures/replay-longkeys-100k \
  --count 100000 --key-bytes 1024
```

The output includes `capture/`, `sorted/`, `sort.log`, and a SHA-256 file manifest.
Keys share a long prefix and end in a fixed-width decimal suffix, so their byte order
is deterministic. The resource driver should point `--fixture` at `sorted/`.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/benchmarks/replay -p test_resource_fault.py

python3 scripts/benchmarks/replay/resource_fault.py \
  --server "$CANDIDATE/bin/swath-replay" --fixture /path/to/stamped-long-key-fixture \
  --bucket bench --output /path/to/durable-receipts/longkeys-c512 \
  --mode normal512 --clients 512 --page-size 1000 \
  --body-upper 1500000 --response-buffer-budget 2147483648 \
  --max-response-bytes 67108864 --max-concurrent-requests 512 \
  --server-java-opts '-Xms4g -Xmx4g -XX:MaxDirectMemorySize=512m -Djdk.nio.maxCachedBufferSize=262144' \
  --jfr
```

The example bound is `512 × ceil(2.5 × 1,500,000) = 1,920,000,000` bytes, so the
declared 2 GiB budget clears only the encoded-array term. Measure a serial preflight
response and inspect fixture key lengths before choosing `M`; the script checks its
actual preflight and measured response lengths but cannot prove unseen pages fit `M`.
The resource arm is characterization until its fixture, JVM limits, direct-memory
measurements, and no-refusal result are recorded. No JFR/NMT claim follows from unit
tests or this example command alone.
