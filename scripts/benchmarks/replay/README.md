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
  scripts/benchmarks/replay/ReplayHttpBench.java \
  scripts/benchmarks/replay/FixtureMetadataOracle.java \
  scripts/benchmarks/replay/MetadataVerifier.java \
  scripts/benchmarks/replay/ReplayShapeBench.java
```

The paired runner starts one server per arm, pins server and client to disjoint CPU sets,
performs an oracle-checked warmup at the declared concurrency in the same driver JVM,
and then runs 12 interleaved baseline/candidate pairs. `--end-ack` holds client carrier
and selector threads alive until their final CPU snapshot. It writes every attempt and its before/after
server metrics to `rounds.jsonl`, plus all paired ratios and log-ratio 95% confidence
intervals to `summary.json`. A missing pair or an interval crossing a budget fails the
panel. It reports marker-aligned server/client process CPU, sampled RSS peak, direct-buffer
bytes before/after, and JVM GC allocation **estimate**; that estimate is not an exact
allocation counter. It also records conservative foreign CPU activity on reserved cores,
client thread utilization, G1 region size, GC pause causes, promoted-byte estimate,
and the fraction of measured time that clients were actively walking. Warmup verifies
fixture-backed key, size and last-modified time at each protocol's wire precision; separate
conformance tests cover optional and synthetic metadata fields. Peak direct memory and exact allocation require a separate sampled/JFR arm before
those resource acceptance claims can pass.

Example S3 gate invocation, after substituting a candidate distribution path and a
durable output directory:

```sh
python3 scripts/benchmarks/replay/run_pair.py \
  --baseline "$BASE/bin/swath-replay" --candidate "$CANDIDATE/bin/swath-replay" \
  --fixture /path/to/sorted-fixture \
  --fixture-glob '/path/to/sorted-fixture/*.parquet' \
  --classpath "/tmp/swath-replay-bench-classes:$BASE/lib/*" \
  --clients 16 --page-size 1000 --connections 16 --end-ack \
  --partitioned --repetitions 7 --require-oversized-fixture \
  --server-cpus 0-5 --client-cpus 6-15 \
  --server-java-opts '-Xms1536m -Xmx1536m -XX:MaxDirectMemorySize=512m -Djdk.nio.maxCachedBufferSize=262144' \
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

Before release panels, `--aa-control` runs six interleaved identical-binary pairs. Its
95% log-ratio intervals must contain 1; the receipt records their half-width and the
projected twelve-pair interval resolution. This noise control never shortens the
twelve-pair release gates. The runner conservatively flags more than 2% other-process
or kernel CPU on its reserved cores; keep other builds and benchmarks quiet.

Predeclare JVM flags, fixture, server options, response budgets, cache state, and per-arm
protocols before timing. For native comparison, point both distribution paths at the
candidate, set `--baseline-protocol s3`, set `--candidate-protocol gcs` or `azure`, and pass
the candidate's enablement flags with `--baseline-option` and `--candidate-option`. For
the S3 regression panel, compare the frozen baseline and candidate distributions. The
native comparison's p99 number is characterization until an
explicit cross-protocol latency budget is adopted; the S3 regression budget is 1.10.
`--workload seek --page-size 1` drives deterministic fixture-backed one-key seeks;
`--workload delimiter --delimiter-prefix PREFIX` drives complete native token walks
and validates emitted objects and prefixes. Predeclared wide/deep candidates on
`tc-rg3m` are `projects/headers-testing/` (121 direct prefixes over 8.13m descendants)
and `contrib/datacomp/DCLM-pool/jsonl/` (89 prefixes over 5.08m descendants), at a
50-entry page size for prefix-ending pages. Shape panels compare requests/s; seek adds
CPU/request, and delimiter adds decoded key rows and page reseeks per emitted prefix,
with zero S3 baselines handled explicitly. Narrowed GCS restarts and mixed open-loop
traffic still need separate declared workload drivers and receipts before final epic
acceptance.

`resource_fault.py` is the separate opt-in response-resource arm. It starts a candidate
server and writes `receipt.json`, a server log, before/after `/metrics` snapshots,
sampled RSS peak, direct-buffer meter values, and `jcmd VM.native_memory` summaries to
the declared durable output directory. With `--jfr`, it also starts and dumps a JFR
recording. It has three modes: `normal512` completes 512 concurrent S3 long-key pages
with zero refusals under an adequately sized byte budget; `slow` holds unread sockets,
checks a labeled overload when requested, then checks permit/byte recovery after
disconnect; `trickle` periodically reads half the declared TCP receive buffer to
reopen its window and checks the total write deadline after progress beyond idle timeout.
The run fails if a slow socket never holds a callback, so a small response cannot
masquerade as a backpressure test.

Declare an upper bound `M` on the encoded body length, then size the normal arm with
`B >= C × max(preflightPeakCapacity, M + chunk)` on chunked candidates, where `C`
is concurrent responses and `preflightPeakCapacity` is read from the fresh
server's `peak_charged_response_bytes` after one serial page. For the frozen
growable-array baseline, the driver uses `ceil(2.5 × M)` instead of `M + chunk`
to cover both old and new arrays during a 1.5× growth copy. The receipt names
which allocator model was measured.
The driver rejects a budget below this bound and any response above `M`. This byte
arithmetic excludes decoded rows, the shared cache, Jetty buffers, JDK direct buffers,
and transient garbage; predeclare `-Xmx`, `MaxDirectMemorySize`, and
`jdk.nio.maxCachedBufferSize=262144` separately. The `slow` arm may intentionally use
a smaller budget to prove 503 refusal and recovery; it is not a throughput pass.

For a deterministic long-key resource corpus, generate a 600,000-object capture with
1,024-byte keys and stamp it through `sort-fixture` into a new durable directory:

```sh
python3 scripts/benchmarks/replay/make_longkey_fixture.py \
  --replay "$CANDIDATE/bin/swath-replay" \
  --output /path/to/durable-fixtures/replay-longkeys-600k \
  --count 600000 --key-bytes 1024
```

The output includes `capture/`, `sorted/`, `sort.log`, and a SHA-256 file manifest.
Keys share a long prefix and end in a fixed-width decimal suffix, so their byte order
is deterministic. The resource driver should point `--fixture` at `sorted/`. For
slow/trickling sockets, add `--escape-heavy`: it fills the 1,024-byte S3 keys with
ASCII spaces, so `encoding-type=url` expands their response keys to about 3 MiB per
1,000-key page. The generator refuses keys above S3's 1,024-byte limit.

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/benchmarks/replay -p test_resource_fault.py

python3 scripts/benchmarks/replay/resource_fault.py \
  --server "$CANDIDATE/bin/swath-replay" --java-home "$JAVA_HOME" \
  --fixture /path/to/stamped-long-key-fixture \
  --fixture-glob '/path/to/stamped-long-key-fixture/*.parquet' \
  --driver-classpath '/path/to/versioned-benchmark-classes:/path/to/candidate/lib/*' \
  --bucket bench --output /path/to/durable-receipts/longkeys-c512 \
  --mode normal512 --clients 512 --page-size 1000 \
  --body-upper 1500000 --response-buffer-budget 2147483648 \
  --max-response-bytes 67108864 --max-concurrent-requests 512 \
  --output-chunk-bytes 262144 --decoded-row-bytes 2048 \
  --heap-headroom 1610612736 --cache-staging-headroom 268435456 \
  --server-java-opts '-Xms4g -Xmx4g -XX:MaxDirectMemorySize=512m -XX:NativeMemoryTracking=summary -Djdk.nio.maxCachedBufferSize=262144' \
  --jfr
```

For the growable-array baseline, the example bound is
`512 × ceil(2.5 × 1,500,000) = 1,920,000,000` bytes. A 256 KiB chunked candidate
uses at least `512 × (1,500,000 + 262,144) = 902,217,728` bytes, or more if its
measured preflight credit exceeds that amount per response. The declared 2 GiB budget
clears only the encoded-output term. The separate 1.5 GiB heap headroom declares
`512 × 1,000 × 2,048 = 1,048,576,000` bytes of prepared rows plus 256 MiB for
cache, serializer staging, and other heap use; 2,048 bytes per decoded row is a
declared assumption to validate against the fixture and sampled heap. Measure a serial preflight
response and inspect fixture key lengths before choosing `M`; the script checks its
actual preflight and measured response lengths but cannot prove unseen pages fit `M`.
The resource arm is characterization until its fixture, JVM limits, direct-memory
measurements, and no-refusal result are recorded. No JFR/NMT claim follows from unit
tests or this example command alone. The driver records exact client-side peak
outstanding requests and sampled server-active responses separately; Java HttpClient
does not expose a reliable connection identity, so pooled-connection reuse needs a
separate transport observation before the full 512-client lifecycle gate can pass.

The default transport on this host accepted even a 7.12 MiB Azure page into
Jetty/kernel buffers before the client consumed it. That run cannot prove a pending
write or a write deadline. For a diagnostic that forces pending writes without changing
the shipped server, compile the separate `ResourceFaultServer` launcher against the
frozen candidate and declare a small accepted TCP send buffer in the receipt:

```sh
mkdir -p /tmp/swath-resource-fault-classes
"$JAVA_HOME/bin/javac" -cp "$CANDIDATE/lib/*" \
  -d /tmp/swath-resource-fault-classes \
  scripts/benchmarks/replay/ResourceFaultServer.java

python3 scripts/benchmarks/replay/resource_fault.py \
  --server "$CANDIDATE/bin/swath-replay" --java-home "$JAVA_HOME" \
  --fixture /path/to/stamped-escape-heavy-fixture --bucket bench \
  --output /path/to/durable-receipts/azure-trickle5k-send64k \
  --mode trickle --protocol azure --page-size 5000 --clients 1 \
  --body-upper 12000000 --response-buffer-budget 134217728 \
  --max-response-bytes 16777216 --max-concurrent-requests 16 \
  --output-chunk-bytes 262144 --decoded-row-bytes 2048 \
  --write-timeout 5s --idle-timeout 1s --inject-latency '' \
  --accepted-send-buffer-bytes 65536 \
  --diagnostic-server-classpath "/tmp/swath-resource-fault-classes:$CANDIDATE/lib/*" \
  --server-java-opts '-Xms2g -Xmx2g -XX:MaxDirectMemorySize=512m -XX:NativeMemoryTracking=summary -Djdk.nio.maxCachedBufferSize=262144' \
  --jfr
```

This checks the timeout path under a declared diagnostic socket setting. It is not
evidence that a default-socket client consuming already buffered bytes triggers that
server-side deadline. The receipt pins the diagnostic class file and candidate main JAR,
and the connector listener records accepted `SO_SNDBUF` at connection open: minimum,
maximum, and sample count. With no explicit send-buffer setting, Linux may autotune
later writes up to the receipt's recorded `tcp_wmem` maximum. Keep those receipts
separate and label the transport choice.
With the same diagnostic classpath but `--accepted-send-buffer-bytes 0`, the launcher
leaves Jetty's socket setting at its default while recording accepted connection count
and open-time send-buffer values. That `instrumented_default_sndbuf` arm can show at
least one reuse when listing requests exceed new accepts, with new accepts at most the
declared client count; it does not prove every request reused a connection. It is a separate transport
observation, not a substitute for the uninstrumented throughput arm.

`OutputChunkBench` is an offline encoder diagnostic for the chunked response change.
It fixes 200 warmup and 1,000 measured renders of 1,000 real fixture rows per
protocol, in 64/128/256 KiB chunk arms. It reports encoded bytes, charged chunk
capacity, view count, thread CPU, and thread-allocated bytes; it does not include
Jetty callbacks or network cost. Run it only with a declared fixture and JVM flags,
and write its JSON lines and a source/fixture plan to a durable receipt directory:

```sh
./gradlew :swath-replay:compileBenchmarkJava
"$JAVA_HOME/bin/java" --enable-native-access=ALL-UNNAMED \
  -Xms2g -Xmx2g -XX:MaxDirectMemorySize=512m \
  -Djdk.nio.maxCachedBufferSize=262144 \
  -cp 'swath-replay/build/classes/java/benchmark:swath-replay/build/classes/java/main:swath-replay/build/install/swath-replay/lib/*' \
  io.varve.swath.replay.server.OutputChunkBench /path/to/sorted-fixture \
  > /path/to/durable-receipts/chunk-panel.jsonl
```

The fixed 256 KiB serving chunk size was retained after an inconclusive
representative server comparison with 128 KiB. Offline chunk arms still exercise
the package-private test constructor. The first 1,000 keys of a fixture may have
shorter names than its median or worst pages; retain those real body-size
distributions when interpreting an offline panel.
