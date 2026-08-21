# swath replay server

This is a contributor tool. It is not part of the `swath` CLI and is not needed for a
normal listing.

`swath-replay-server` serves a Parquet listing through the subset of S3
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
./gradlew :swath-cli:installDist :swath-replay-server:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PWD/swath-replay-server/build/install/swath-replay-server/bin:$PATH"
```

This installs `swath`, `swath-replay-server`, and `swath-replay-conformance`.
Replay production sources cannot compile directly against Parquet/Hadoop; the module's
classpath check preserves that boundary while runtime support comes through `swath-core`.

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
swath-replay-server sort-fixture \
  --capture "$RUN_DIR/capture/data" \
  --output "$RUN_DIR/sorted"
```

The transform uses the same bounded sort library as swath, writes temporary files then
renames them, and prints row/file/byte/segment/merge counts. Output files are named
`part-00001.parquet` onward. Versioned rows or duplicate keys are rejected in the current
non-versioned format rather than silently collapsed.

## Start the server

```bash
swath-replay-server serve \
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
`max(8, min(32, 2 × cores))` for sorted serving and `min(4, cores)` for DuckDB.
Size this for concurrent decode work, not total requests: readers are returned before
injected sleep. Each sorted slot holds an open reader and decoded footer per file, so
very large pools can waste file descriptors and heap. Sorted mode opens both a row-group
and range-reader pool per file: approximately `2 × connections × files` readers.

## Reading a running server's meters (`--metrics-port`)

`serve` keeps every meter in the table under "Metrics And Tuning" below, but a
long-running server has no `bench` report to print them into. `--metrics-port`
exposes them over HTTP for as long as the server runs:

```bash
swath-replay-server serve ... --metrics-port 19192

curl -s http://127.0.0.1:19192/metrics | jq .
```

Three paths and nothing else: `GET /metrics` returns the whole registry as JSON,
`GET /runtime-attestation` returns the resource limits visible inside this
server process, and `GET /healthz` returns `ok` once the server is listening —
which is also the readiness signal to poll before starting a client, since a
large fixture's index derive is not instant. A negative port (the default)
disables the endpoint; `0` binds a free port and reports it in the startup line,
which carries `metrics_endpoint=` whenever the endpoint is on.

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

**It is a second port on purpose.** A metrics or runtime-attestation scrape never
enters the serving path: it takes no read permit, receives no injected latency,
and increments no listing request counter, so polling cannot perturb what it
measures — and it still answers while every serving thread is parked in an
injected sleep, which is exactly when an answer is most wanted. Its thread pool
is deliberately tiny (4), because taxing
the box to answer a diagnostic would tax the measurement the diagnostic exists
to validate.

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
swath-replay-server serve ... \
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
`swath.replay.inject.overrun.ms{shape}`; a run with material overruns measured the replay
server rather than the intended profile.

### Compressed time (`--latency-scale`)

`--latency-scale N` divides every injected base and `/cp` delay by `N`:

```bash
swath-replay-server serve ... \
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
swath-replay-server bench \
  --fixture "$RUN_DIR/sorted" --bucket digitalcorpora \
  --modes sorted,duckdb --max-keys 1000 \
  --json "$RUN_DIR/bench-report.json"
```

`bench` starts each mode, follows continuation tokens to completion over loopback HTTP, and
reports startup, client request latency, server list/read latency, page/key counts, and
throughput. Multiple modes walk the same fixture and report ratios. Keep startup time
separate from walk time, and client round-trip latency separate from the store-level
`page.read.latency` acceptance signal.

## Metrics and tuning

Replay meters use the `swath.replay.*` namespace. Important groups are:

| Meters | Meaning |
| --- | --- |
| `sortfixture.build.latency`, `sortfixture.output.bytes`, `sort.steal_reason{outcome,reason}` | Legacy-fixture sort work and engagement. |
| `index.load.latency{source=derived}`, `index.entries` | Sorted routing-index construction. |
| `serving.path{mode}`, `serving.fallback{reason}`, `serving.refused{reason}` | Selected path, startup decline, or request-time safety refusal. |
| `delimiter.path{path}`, `delimiter.skipscan.row_group_opens` | Rollup vs walk and skip-scan I/O. |
| `page.read.latency`, `fixture.list.latency` | Store read and complete pager operation. |
| `request.latency{shape}` | Server request cost, including reader-pool wait but excluding injected delay, separated into `worker_page`, `pivot_probe`, and `structure_probe`. |
| `inject.overrun{shape}`, `inject.overrun.ms{shape}` | Requests exceeding the injected profile and their excess latency. Absent when injection is off; zero overruns is the healthy state. |
| `prefetch.window.fill`, `prefetch.window.hit`, `prefetch.window.miss{reason}`, `prefetch.fill.rows` | Window-cache cost, effectiveness, and ramp behavior. |

Names above omit the common `swath.replay.` prefix for compactness. Fallback reasons are
`no_stamp`, `unsupported_mode`, `unknown_format_version`, `incomplete_multifile`,
`mixed_row_types`, and `sanity_failed`.

Tune on your fixture and machine. More conformance parallelism or DuckDB connections can
reduce wall time until scans contend; after that it increases per-query latency. Warm the
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

Retain fixture provenance, source revision, machine, configuration, and report with any
published result. Keep captures, HARs, checkpoints, and mismatch artifacts in `/tmp` or
another ignored location; never commit real bucket data.
