# swath replay toolkit

This is a contributor tool. It is not part of the `swath` CLI and is not needed for a
normal listing.

`swath-replay` prepares, benchmarks, and serves Parquet listings through the subset of S3
`ListObjectsV2` that swath uses. It makes real bucket shapes repeatable for client tests,
engine debugging, conformance checks, and benchmarks without repeatedly listing S3.

It is built in this repository but distributed separately from the `swath` CLI. Its wire
behavior is conformance-tested; diagnostics and launcher settings remain a development
surface.

It is not a general S3 emulator: there is no object data, mutation, authentication,
virtual-host routing, version listing, or API other than path-style `ListObjectsV2`.

For an investigation workflow, see [Replay troubleshooting](replay-troubleshooting.md).

## Build

```bash
./gradlew :swath-cli:installDist :swath-replay:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PWD/swath-replay/build/install/swath-replay/bin:$PATH"
```

This installs `swath`, `swath-replay`, and `swath-replay-conformance`.
Replay production sources cannot compile directly against Parquet/Hadoop; the module's
classpath check preserves that boundary while runtime support comes through `swath-core`.

## Container image

Tagged releases publish a separate multi-architecture toolkit image:

```bash
docker pull ghcr.io/varveio/swath-replay:<version>
```

The image contains both toolkit launchers and uses `swath-replay` as its entrypoint.
Fixtures are never baked into the published image: mount a file or directory read-only and pass
its container path to `--fixture`:

```bash
FIXTURE_DIR=/absolute/path/to/sorted
docker run --rm -p 127.0.0.1:19090:19090 \
  -v "$FIXTURE_DIR:/fixtures:ro" \
  ghcr.io/varveio/swath-replay:<version> serve \
  --fixture /fixtures --bucket digitalcorpora \
  --host 0.0.0.0 --port 19090 --serving-mode sorted
```

Binding the process to `0.0.0.0` is necessary for the published port to reach it. The server has
no authentication, so bind the host side to loopback as shown unless an isolated container
network provides the boundary. Pin reproducible runs by the digest printed in the release summary:
`ghcr.io/varveio/swath-replay@sha256:<digest>`.

Build the image locally with `docker build -f Dockerfile.replay -t swath-replay:dev .`.

## Create a fixture

Keep captures outside the repository because key names and metadata may be sensitive.

```bash
RUN_DIR=/tmp/swath-replay-example
mkdir -p "$RUN_DIR"

swath list s3://digitalcorpora \
  --region us-west-2 --no-sign-request \
  --format parquet -o "$RUN_DIR/capture" \
  --restart --concurrency 16
```

A managed Parquet dataset stores its part files in `capture/data/`. Replay commands accept a
single file or a directory whose immediate children are `*.parquet`; they do not recurse.
Pass `capture/data`, not the dataset root.

## Sort a legacy capture (`sort-fixture`)

`swath --sort` output is already globally sorted and stamped. Convert older or unsorted
parts once:

```bash
swath-replay sort-fixture \
  --capture "$RUN_DIR/capture/data" \
  --output "$RUN_DIR/sorted"
```

The transform uses the same bounded sort library as swath, writes temporary files then
renames them, and prints row/file/byte/segment/merge counts. Output files are named
`part-00000.parquet` onward. Versioned rows or duplicate keys are rejected in the current
non-versioned format rather than silently collapsed. Duplicate raw keys are rejected in the shared
final drain, after sorting and before publication; `SORT.equal_key_rejected` records the failed
fixture once. Live `swath --sort` permits equal raw-key rows for version-compatible output.

Unlike live `swath --sort` staging, independently sorted fixture chunks can overlap across their
entire key ranges. `sort-fixture` therefore uses the library's bounded serial entry-stream merge and
does not engage the page-frontier parallel range optimization, even when merge parallelism is
configured. This keeps active state bounded for arbitrary captures; `SORT.merge_range_frontier_disabled`
reports that expected policy choice.

## Start the server

```bash
swath-replay serve \
  --fixture "$RUN_DIR/sorted" \
  --bucket digitalcorpora \
  --host 127.0.0.1 --port 19090 \
  --serving-mode sorted
```

`--serving-mode` selects the fixture path:

| Mode | Behavior |
| --- | --- |
| `sorted` | Require that direct path and fail startup if eligibility cannot be proved. Use this for a focused test. |
| `duckdb` | Materialize and index the fixture in temporary DuckDB storage. This is the independent oracle and legacy fallback. |

Sorted mode derives an in-memory first-key-per-row-group index and performs bounded Parquet
reads. A bounded sequential-window cache is enabled by default. Its system properties are
`swath.replay.prefetch.enabled` (`true`), `swath.replay.prefetch.window-rows` (`12500`),
and `swath.replay.prefetch.max-windows` (`96`). Size the window at least as large as one
row group's row count or repeated fills will decode the same group.

Request admission happens at this cache: hits bypass Parquet permits, continuation anchors
are claimed before a request can wait for a backing read, and the sorted store's connection
pool remains the sole bound on concurrent Parquet decoding. This avoids cold breadth-first
waves without weakening the backing-read bound. The cache-path engagement is visible through
the prefetch hit/miss and anchor meters below; acquired backing readers remain visible through
`parquet.queries.in_flight` and `.peak`.

Sorted mode reads Parquet's page index directly and stops once it has the requested rows.
Final sorted files therefore default to 1,024 rows per data page
(`swath.sort.final-page-rows`); a page is the smallest unit a bounded read can decode.
Older fixtures remain correct and can be re-sorted to adopt the smaller seek geometry.

## Serving concurrency (`--max-concurrent-requests`)

Injected latency blocks a serving thread, so the request ceiling is explicit rather than
Jetty's implicit 200-thread default. It defaults to 512 and is reported as
`max_concurrent_requests=` at startup. Set it at or above the widest client fan-out so
connector queueing is not mistaken for backend latency.

`--parquet-connections 0` uses the selected store's own reader default:
`max(8, min(32, cores))` for sorted serving and `min(4, cores)` for DuckDB.
Size this for concurrent decode work, not total requests: readers are returned before
injected sleep. Each sorted slot holds an open reader and decoded footer per file, so
very large pools can waste file descriptors and heap. Sorted mode eagerly opens the range-reader
pool per file (`connections × files` readers). Its independent row-group pool opens lazily, one
file at a time, only when a native `delimiter=/` skip-scan cannot answer from routing-index bounds;
the conservative fully engaged resident bound remains `2 × connections × files`. An ordinary range
fill uses only the range pool. An engaged skip-scan uses one row-group reader, including when it
materializes a bare object: the bounded full-row read reuses the cursor's reader and the row-group
page index that reader primed under its maximal object projection. Concurrent ordinary and delimiter
requests can still engage both independent pools. The first delimiter-pool engagement for each file
is visible through `delimiter.reader_pool.open.latency`.

### Resource sizing for sorted serving

Fixture cardinality does not become a heap-sized object index. At startup, sorted mode retains
file/footer state and one routing entry per Parquet row group. Object rows are decoded on demand
into bounded reader and prefetch windows; fixture bytes remain on storage and may occupy the OS
page cache outside the JVM heap. Consequently, row-group count, file count, active readers, page
geometry, and cached windows are more useful sizing inputs than the fixture's total object count.

For an eight-CPU replay allocation driving up to 16 independent continuation walks, start with:

```bash
export JAVA_TOOL_OPTIONS='-Xms4g -Xmx4g -Dswath.replay.prefetch.max-windows=24'

swath-replay serve \
  --fixture <sorted-fixture> --bucket <bucket> \
  --host 127.0.0.1 --port 19090 --serving-mode sorted \
  --parquet-connections 0 --max-concurrent-requests 512 \
  --metrics-port 19192
```

This is a capacity-test starting point, not a minimum. Leave G1 region sizing automatic and leave
headroom above `-Xmx` for JVM native state and the OS page cache. The default 96-window cache is the
safer starting point for wider or unknown fan-out; 24 windows was the measured knee for 16 active
walks. A window retains decoded rows, so `max-windows` and heap must be tuned together.

Tune one resource at a time after warming the JVM and storage cache:

1. Keep prefetch enabled for continuation-heavy worker traffic. Disable it only to isolate random
   seek/decode cost; that is a different workload, not a generally faster configuration.
2. Compare `parquet.queries.peak` with the reader count. If the pool is full, CPU has headroom, and
   `page.read.latency` is stable, try a small increase. If page latency or GC rises, reduce it.
   Reader count is decode parallelism, not HTTP concurrency.
3. Size `max-windows` near the number of independently advancing worker streams, with some headroom.
   Use the prefetch hit/miss and live-window meters rather than retaining windows speculatively.
4. Set `--max-concurrent-requests` at or above client fan-out, then verify that the client—not only
   the server—has spare CPU. An undersized client can make a server change look neutral.
5. Measure `worker_page`, `pivot_probe`, and `structure_probe` separately. Full pages benefit most
   from prefetch; one-key pivots and delimiter skip-scans exercise different Parquet paths.

On 2026-08-24, warm loopback saturation tests on eight isolated physical Xeon 8581C cores measured
about **4.15–5.52 million object rows/s** over a 1.049-billion-row, 32-file fixture, depending on
request mixture. A smaller warm random-page fixture reached **6.13 million rows/s**. Treat these as
an order-of-magnitude capacity corridor, not a portable guarantee: CPU generation, Parquet page
geometry, filesystem cache state, response fields, request locality, and client cost all move it.
The setup, shape mix, resource sweep, and limitations are retained in the
[dated field investigation](ops/dev/field-investigations.md#2026-08-24--sorted-replay-server-capacity).

## Reading a running server's meters (`--metrics-port`)

`serve` keeps every meter in the "Metrics and tuning" table below, but a long-running
server does not produce a `bench` report. `--metrics-port` exposes the meters over HTTP
for as long as the server runs:

```bash
swath-replay serve ... --metrics-port 19192

curl -s http://127.0.0.1:19192/metrics | jq .
```

The endpoint exposes only three paths. `GET /metrics` returns the whole registry as JSON;
`GET /runtime-attestation` returns the resource limits visible to the server process; and
`GET /healthz` returns `ok` once the server is listening. Poll the health endpoint before
starting a client because building the index for a large fixture can take time. A negative
port (the default) disables the endpoint; `0` binds a free port and reports it in the
startup line, which carries `metrics_endpoint=` whenever the endpoint is on.

The attestation uses `schema_version: runtime-attestation-v1`. Its `cgroup_v2`
object names the resolved directory and records `cpuset.cpus.effective`,
`memory.max`, and `memory.swap.max`; `proc_self_status` independently records
`Cpus_allowed_list`. Each observation is a `{value, error}` pair. Finite memory
and swap limits are byte counts, the kernel's unlimited value remains the
literal `max`, and an absent, unreadable, or empty source — or an invalid memory
or swap limit — stays an explicit error instead of becoming a requested or
inferred limit. CPU-list grammar, agreement between the two CPU observations,
and agreement with the declared allocation are downstream verifier checks, not
claims made by this producer. This is runtime evidence for a controller to
compare with its declared allocation, not an allocation request of its own.

The payload is `{schema_version, serving_mode, uptime_ms, sampled_at_epoch_ms,
meters[]}`. Each meter carries its `name`, `type` (`timer`, `counter`,
`distribution`, `gauge`), and `tags`; a timer adds `count`, `sum_ms`, `mean_ms`,
`max_ms`, `p50_ms`, `p99_ms` — the same values `bench` reports, read the same
way, so a scrape and a bench report of the same run agree. Meters are emitted in
a stable order so two scrapes diff cleanly, and `uptime_ms` lets a reader bound
an interval without trusting its own clock against the server's.

Read server headroom from `swath.replay.request.latency{shape}` per request shape,
against the injected profile for that same shape. A pooled average would hide the
important case because clients issue different mixtures of differently priced requests.

**It is a second port on purpose.** A metrics or runtime-attestation request does not
enter the serving path, take a read permit, receive injected latency, or increment a
listing counter. It therefore remains available while serving threads are delayed and
does not directly alter the request measurements. Its thread pool is limited to four
threads so diagnostics do not consume significant capacity from the workload being measured.

**Poll it; do not wait for shutdown.** There is no dump-on-exit, by design: a
server run as a sidecar is typically *killed* when the process it serves exits,
rather than asked to stop, so anything written on shutdown is written by a code
path that may never run. Scrape before a measured
window opens, poll through it, and scrape again at the end — the series is what
shows whether the server stayed out of the way, which a single aggregate at the
end cannot.

Set the `swath.replay.slow-request-log-ms` system property to log the request shape,
range parameters, result size, and server cost for requests exceeding the threshold.
Negative or unset disables this diagnostic.

`delimiter=/` uses a native skip-scan that advances past each returned common prefix and
stops at `max-keys`; other delimiter shapes use the ordinary range walk. If a stepped row
group proves internally disordered, sorted mode returns `500 InternalError` and increments
`swath.replay.serving.refused{reason=row_group_disorder}`. It never guesses past disorder.
Re-sort the capture or use DuckDB.

`--parquet-connections N` controls the DuckDB pool; `0` chooses `min(4, CPUs)`. Connections
divide available threads, so raising the pool trades per-query resources for request
parallelism.

## Fault-latency injection (`--inject-latency`)

Inject deterministic delay by request shape:

```bash
swath-replay serve ... \
  --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15
```

A delimiter request is a `structure_probe`; a delimiter-free `max-keys<=1` request is a
`pivot_probe`; other requests are `worker_page`. The optional `/cp` term charges for each
returned common prefix, which a network proxy cannot infer. Jitter is derived from request
bytes, so the same request gets the same delay. `prod-commoncrawl` is a built-in reference
profile. Injection is off by default.

### The delay is a deadline, not a surcharge

Injected latency is the request's total target time. After serving the page, the server
waits only for the remainder, so the client observes `max(server_cost, profile)`, not
`server_cost + profile`. Requests that exceed their profile increment
`swath.replay.inject.overrun{shape}` and record the excess in
`swath.replay.inject.overrun.ms{shape}`. If overruns are substantial, the benchmark is
measuring replay-server overhead rather than the configured latency profile.

### Compressed time (`--latency-scale`)

`--latency-scale N` divides every injected base and `/cp` delay by `N`:

```bash
swath-replay serve ... \
  --inject-latency prod-commoncrawl --latency-scale 50
```

This preserves relative profile shape but not absolute runtime: server and client CPU/I/O
do not scale. Use it for fast mechanism iteration, then confirm timing-dependent conclusions
at scale `1`.

## Query it

```bash
curl 'http://127.0.0.1:19090/digitalcorpora?list-type=2&max-keys=10&encoding-type=url'

swath list s3://digitalcorpora \
  --endpoint-url http://127.0.0.1:19090 \
  --force-path-style --no-sign-request \
  --format jsonl --checkpoint none
```

Supported parameters are `list-type=2`, `prefix`, `delimiter`, `start-after`,
`continuation-token`, `max-keys`, `encoding-type=url`, and `fetch-owner`. Replay tokens and
real S3 continuation tokens are intentionally not interchangeable.

A request may carry both `continuation-token` and `start-after`, which some clients do because
they keep their opening `start-after` in the request template and add the token as they page.
Replay resumes at the token and ignores the `start-after`, as real S3 does, and omits the ignored
value from the response. A malformed token is still rejected, `start-after` present or not:
precedence chooses which boundary applies, it does not make one a fallback for the other.

## Run real-S3 conformance

The harness can capture S3 traffic through Dockerized mitmproxy, create a fixture, replay
the captured requests, and compare canonical XML. For an existing HAR and fixture:

```bash
swath-replay-conformance \
  --har "$RUN_DIR/s3.har" \
  --fixture "$RUN_DIR/capture/data" \
  --bucket digitalcorpora \
  --mismatch-dir "$RUN_DIR/mismatches" \
  --sample-entries 200 --parallelism 4 \
  --replay-parquet-connections 4
```

The comparator checks status, ordering, pagination, prefix/delimiter behavior, and encoded
XML while normalizing opaque token values that cannot literally match. Start with a sample
on large captures; use a full comparison for a fidelity gate.

## Benchmark a full token walk (`bench`)

```bash
swath-replay bench \
  --fixture "$RUN_DIR/sorted" --bucket digitalcorpora \
  --modes sorted,duckdb --max-keys 1000 \
  --json "$RUN_DIR/bench-report.json"
```

`bench` starts each mode, follows continuation tokens to completion over loopback HTTP, and
reports startup, client request latency, server list/read latency, page/key counts, and
throughput. Multiple modes walk the same fixture and report ratios. Keep startup time
separate from walk time, and client round-trip latency separate from the store-level
`page.read.latency` backing-decode signal. In sorted mode that timer counts bounded page decodes,
not HTTP pages: cache hits and routing-index-only delimiter hops add no sample, while a delimiter
request adds a sample only when it materializes a bare object through its already-owned row-group
reader. The key-cursor work remains in `parquet.query.latency`.

## Metrics and tuning

Replay meters use the `swath.replay.*` namespace. Important groups are:

| Meters | Meaning |
| --- | --- |
| `sortfixture.build.latency`, `sortfixture.output.bytes`, `sort.steal_reason{outcome,reason}`, `sort.progress`, `sort.merge.boundaries.embedded.entries` / `.embedded.bytes` / `.scan.bytes` | Legacy-fixture sort work, engagement, progress, and complete boundary-I/O adapter counters. The three boundary counters are structurally zero for the current `sort-fixture` path: `ARBITRARY_SORTED_RUNS` disables range boundaries. They remain registered to keep the adapter complete and future-safe. |
| `index.load.latency{source=derived}`, `index.entries` | Sorted routing-index construction. |
| `serving.path{mode}`, `serving.fallback{reason}`, `serving.refused{reason}` | Selected path, startup decline, or request-time safety refusal. |
| `delimiter.path{path}`, `delimiter.skipscan.row_group_opens`, `delimiter.skipscan.whole_group_shortcuts`, `delimiter.reader_pool.open.latency` | Rollup vs walk, skip-scan I/O, routing-index-only whole-group engagements, and lazy per-file delimiter-pool first touch (timer count = files opened). |
| `page.read.latency`, `fixture.list.latency` | Post-borrow bounded-page decode service time (pool wait excluded) and complete pager operation. Cache hits add no page-read sample. |
| `parquet.queries.in_flight`, `parquet.queries.peak` | Current and run-peak acquired backing readers. DuckDB is bounded by `connections`; sorted serving has independent `connections`-wide range and lazy row-group pools per file. One request owns one reader, while concurrent ordinary and delimiter requests can engage both pools. |
| `request.latency{shape}` | Server request cost, including reader-pool wait but excluding injected delay, separated into `worker_page`, `pivot_probe`, and `structure_probe`. |
| `inject.overrun{shape}`, `inject.overrun.ms{shape}` | Requests exceeding the injected profile and their excess latency. Absent when injection is off; zero overruns is the healthy state. |
| `prefetch.window.fill`, `prefetch.window.hit`, `prefetch.window.miss{reason}`, `prefetch.fill.rows` | Window-cache cost, effectiveness, and ramp behavior. |
| `prefetch.windows.live`, `prefetch.anchors.live`, `prefetch.anchor{event}` | Live cache/anchor occupancy and anchor registration, claim, or eviction-before-claim churn. |

Names above omit the common `swath.replay.` prefix for compactness. Fallback reasons are
`no_stamp`, `unsupported_mode`, `unknown_format_version`, `incomplete_multifile`,
`mixed_row_types`, and `sanity_failed`.

Tune on your fixture and machine. More conformance parallelism or DuckDB connections can
reduce wall time until scans contend; after that they increase per-query latency. Warm the
JVM, connection pool, and filesystem cache before recording a benchmark.

## Fidelity limits

- Only object rows and `ListObjectsV2` are served; version listing is unsupported.
- The server is usually faster than S3 unless a target profile is injected.
- One local process does not reproduce distributed service capacity or real network faults.
- In-server injection adds latency, not API errors, resets, malformed bodies, or partial
  responses. Use engine test fixtures for deterministic API faults and a transport proxy
  for wire faults.
- Differential tests and the conformance harness establish behavior; the repository does
  not yet publish a versioned, portable corridor benchmark bundle.

Whenever you publish benchmark results, retain the fixture provenance, source revision,
machine details, configuration, and report. Keep captures, HARs, checkpoints, and
mismatch artifacts in `/tmp` or another ignored location; never commit real bucket data.
