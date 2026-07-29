# swath replay server

> **Built here, but not part of the `swath` CLI distribution.** `swath-replay-server` is built by
> this repository's Gradle project and is a supported thing to use; it is simply shipped separately
> from the `swath` CLI rather than inside it. Its wire behavior — what it serves for a given fixture
> — is stable and conformance-tested. Its *operational* surface (diagnostics, logging destinations,
> JVM launcher settings) is still being smoothed and is not yet a v0.1 user contract.

The swath replay server serves a swath Parquet listing as an HTTP endpoint
that looks like S3 `ListObjectsV2` — the only wire protocol it speaks today;
a future GCS/Azure protocol would sit beside it.

It is useful when you want to test listing clients, replay tricky bucket shapes,
or debug swath's parallel listing behavior without repeatedly hitting a real S3
bucket. It is not a general S3 emulator: it only implements path-style
`ListObjectsV2` over an existing listing fixture.

For the investigation methodology built on this server, see
[`replay-troubleshooting.md`](replay-troubleshooting.md).

## What It Is Good For

- Reproducing bugs found on real S3 buckets without re-listing the bucket.
- Testing swath against large or adversarial bucket shapes locally.
- Comparing replay responses with real S3 HAR captures.
- Developing listing algorithms against stable fixtures.
- Running fast regression tests against bucket layouts that would be expensive
  or slow to recreate live.

## What It Is Not

- Not a full S3 server.
- No `GetObject`, `PutObject`, bucket creation, deletes, multipart uploads, or
  version listing.
- No authentication or SigV4 validation.
- No virtual-hosted-style routing; use path-style requests.
- No real object data. Only listing metadata captured in swath's Parquet output
  is available.

## Build

```bash
./gradlew :swath-cli:installDist :swath-replay-server:installDist
export PATH="$PWD/swath-cli/build/install/swath/bin:$PWD/swath-replay-server/build/install/swath-replay-server/bin:$PATH"
```

Binaries:

```text
swath
swath-replay-server
swath-replay-conformance
```

`:swath-replay-server:check` runs `verifyNoParquetOrHadoopOnCompileClasspath`, which fails the
build if any `org.apache.parquet`/`org.apache.hadoop` artifact reaches this module's main **compile**
classpath — no `io.varve.swath.replay` source may import a parquet/hadoop type directly; the
swath-core module's own parquet dependency reaches this module's **runtime** classpath only, through the
`implementation(project(":swath-core"))` edge. Test-scope parquet dependencies (used to build fixtures in this
module's own tests) are unaffected — the check only looks at `compileClasspath`.

## Create A Fixture

Use swath to list a real bucket or prefix into Parquet. Keep this output outside
the repo, for example under `/tmp`.

```bash
RUN_DIR=/tmp/swath-replay-example
mkdir -p "$RUN_DIR"

swath list s3://digitalcorpora \
  --region us-west-2 \
  --no-sign-request \
  --format parquet \
  -o "$RUN_DIR/capture" \
  --restart \
  --concurrency 16
```

For private buckets, omit `--no-sign-request` and use the normal swath
credential options.

Do not commit real bucket captures. The capture directory is a swath **directory
dataset**: the Parquet parts land in `$RUN_DIR/capture/data/`, alongside a manifest
and a summary sidecar at the dataset root. Every command below that wants the parts
takes `$RUN_DIR/capture/data` — a path resolves as a single file or as the
`*.parquet` files **directly** inside it, never recursively, so pointing at the
dataset root instead finds nothing.

## Sort A Legacy Capture (`sort-fixture`)

Captures made without `--sort` (or from before it existed) are unsorted Parquet
parts. `sort-fixture` turns one into a single stamped, globally-sorted Parquet
file — the same file a `swath --sort` run would have produced directly, and what
`--serving-mode auto`/`sorted` (below) serves straight off, without DuckDB
materialization:

```bash
swath-replay-server sort-fixture \
  --capture "$RUN_DIR/capture/data" \
  --output "$RUN_DIR/sorted"
```

`--capture` accepts either a directory holding `*.parquet` parts (a capture
dataset's `data/` directory, as above) or a single part file; it does not descend
into subdirectories, so the dataset root itself fails with "no `*.parquet` files
found". The output goes under `--output` as `part-00001.parquet`
(uniform naming shared with swath `--sort`'s own final files, no
`sorted-` prefix; more files only if `swath.sort.final-file-bytes` is tuned to
roll the output); the write is atomic (`*.tmp` then rename), and a crash
leaves only a stale `.tmp`/staging leftover that the next run cleans up
before retrying.

On success it prints a one-line summary:

```
sort_fixture rows=<count> files=<count> bytes=<bytes> wall_ms=<ms> segments=<count> merge_passes=<count> cascaded_passes=<count> output=/path/to/sorted
```

`segments` is how many staging segments the run flushed before the final merge;
`merge_passes`/`cascaded_passes` are the merge's own engagement counts (`cascaded_passes > 0`
means the merge needed more than one pass — see `swath.replay.sort.steal_reason{outcome=SORT,
reason=merge_pass_cascaded}` below to confirm whether the cascade engaged).

This is a **non-versioned, key-unique transform only** (v1): a row carrying a
`version_id` or two rows sharing the same key fail the whole run fast, with a
clear message naming the offending key, rather than silently producing a broken
sorted file. There is no dedup or versioned-serving flag yet — this is a known,
deliberate v1 gap, not a bug, if you hit either case on a real capture.

## Start The Replay Server

```bash
swath-replay-server serve \
  --fixture "$RUN_DIR/capture/data" \
  --bucket digitalcorpora \
  --host 127.0.0.1 \
  --port 19090 \
  --parquet-connections 4
```

`--fixture` may be either a single Parquet file or a directory holding the parts —
for a swath capture that is the dataset's `data/` directory, not its root. A
directory is read as `*.parquet` directly inside it (no recursion); sidecars are
ignored. `sort-fixture` output (`--output` above) is already a flat directory of
parts, so it is passed as-is.

`--parquet-connections` controls concurrent DuckDB connections used to read the
fixture. `0` uses the default `min(4, CPUs)`. Each pooled connection is an
independent DuckDB instance, and the server divides the machine across the pool
(`threads = CPUs / connections` per connection) — without that cap an
N-connection pool defaults to N×CPUs threads and thrashes under a concurrent
client. More connections therefore means more concurrent requests but fewer
threads each; the one request shape that wants many threads is the delimiter
rollup scan, so do not raise this blindly.

### Fault-latency injection (`--inject-latency`)

`serve` can inject per-request latency keyed on the **shape** of the request, so
a fixture can be driven at a real bucket's measured latency profile instead of
loopback speed:

```bash
swath-replay-server serve ... --inject-latency prod-commoncrawl
swath-replay-server serve ... \
  --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15
```

Shapes are classified from the request: a `delimiter` present is a
`structure_probe`, a bare `max-keys<=1` is a `pivot_probe`, anything else is a
`worker_page`. A structure probe's delay may carry a `/cp` term — the delay
grows with the CommonPrefixes the response **returned**, which reproduces S3's
`max-keys` cost sensitivity (S3 stops scanning once it has `max-keys` entries;
the server's own rollup scan does not). That response-proportional term is why
injection lives in the server rather than in a TCP proxy: a proxy never sees
the fanout. `prod-commoncrawl` is a reserved profile carrying reference
measurements from the public Common Crawl bucket. Jitter is a deterministic
fraction in `[0,1)` keyed off the request bytes, so a run is exactly
reproducible. Injection is off by default and adds no cost when off.

#### Compressed time (`--latency-scale`)

`--latency-scale N` divides every injected delay by `N`, so a profile that
describes an hour-long run can be walked in a fraction of that wall clock:

```bash
swath-replay-server serve ... --inject-latency prod-commoncrawl --latency-scale 50
```

Both terms of a delay scale — the flat base and the `/cp` slope — so the
profile keeps its *shape*: a wide structure probe stays the same multiple of a
worker page that it was, which is the relationship a split/steal pathology
turns on. It requires `--inject-latency` (a scale with no profile to scale is
rejected, not ignored), and the default `1` injects the profile exactly as
written, to the nanosecond.

The distortion to keep in mind: **only the injected delay scales.** The
server's own per-request cost, the client's CPU, and the engine's own
scheduling do not, so at `--latency-scale 50` those fixed costs are weighted
~50× heavier relative to the profile than they are in the unscaled run. A
scaled run's wall clock is therefore not the unscaled run's divided by `N`, and
must never be quoted as an absolute time. Use it to compress a long profile
into an affordable iteration loop, and confirm any result that depends on
absolute timing at `--latency-scale 1`.

### `--serving-mode` (`auto` | `sorted` | `duckdb`)

`--serving-mode` chooses how a fixture is served (default `auto`):

- **`auto`** — serve directly over a **stamped, globally sorted** fixture when
  EVERY resolved file (not just the first) carries a recognized sortedness stamp,
  is `objects` mode, and a known `format_version`; the resolved file set proves
  multi-file completeness (contiguous `file_index` from 1, exactly one
  `file_final` flag, on the max index — self-describing, catches a crash mid
  multi-file publish); and the derived row-group index is strictly ascending and
  provably pure `OBJECT`. Otherwise falls back to the DuckDB path and records
  `swath.replay.serving.fallback{reason}` (`no_stamp`, `unsupported_mode`,
  `unknown_format_version`, `incomplete_multifile`, `mixed_row_types`, or
  `sanity_failed`) with a log line explaining the decline. This is the right
  default: it is fast on `--sort` output and still works on any legacy capture
  **whose declared order the file actually has** — see the next paragraph for the
  one shape that is discovered later than startup.
- **`sorted`** — require a sorted-eligible fixture and **fail fast** (the server does
  not start) if it is not one. Use it to guarantee you are exercising the direct
  sorted path.
- **`duckdb`** — always use the materialized DuckDB path, even on a sorted file.
  This is the conformance **oracle** and the path for unsorted captures.

**Disorder *inside* a row group is a request-time hard failure, not a startup
decline.** Eligibility proves the ascent of row-group **first** keys, because that
is all the derive step reads; the rows within one group are proved where they are
stepped over, in the `delimiter=/` skip-scan's key cursor. So a fixture that is
stamped, complete, pure-`OBJECT` and internally disordered — what an older or
foreign producer can publish, and what `sort-fixture` cannot produce — passes
eligibility under `auto`, takes the **sorted** path, and then fails any `delimiter=/`
request **whose skip-scan actually steps into the bad row group** with `500
InternalError`. The check is per stepped row, in the cursor that decodes them, so the
disorder is discovered by whichever request first walks that far and fails every later
one that walks there too — while requests served entirely out of ordered groups keep
succeeding. Nothing latches, so one prefix's listing can be fine while the next
prefix's is a hard failure, and a fixture can look healthy for as long as nothing scans
that far. There is no fallback at the point it does: the path was chosen at startup,
the request has no other path to take, and the server keeps serving the same fixture
until it is restarted against `--serving-mode duckdb`.

That is deliberate. Serving the request instead would mean trusting a cursor
position that no longer stands for "the first key at/after this target": the hop
would emit a common prefix it had already passed, or skip a subtree it never
reached — keys silently dropped or misplaced in an output whose whole purpose is
to be a faithful replay. A hard failure with an address (file, row group, row) is
recoverable; a wrong listing that looks right is not. The refusal bumps
`swath.replay.serving.refused{reason=row_group_disorder}`, so a corpus sweep
classifies the excluded capture from the metrics alone — the HTTP body carries the
reason, the fixture's **file name** and the row group, never the server's paths,
which go to the server log. Re-sort such a capture with `sort-fixture`, or serve it
with `--serving-mode duckdb`, which re-sorts at query time and does not care.

A **sorted** fixture skips materialization entirely: there is no temporary DuckDB
database and no `key` index. At startup the server derives a tiny in-memory routing
index by reading each row group's actual first key (never Parquet footer stats,
which may be truncated for long keys), then serves each page with one bounded
`read_parquet` query whose upper bound is chosen so the window is guaranteed to hold
a full page without scanning the whole table. Sorted files can be produced by
`swath --sort` or, for legacy captures, by `sort-fixture` above. Sorted serving
is non-versioned (`objects`) only in v1.

Sorted-mode page reads are designed so page cost does not grow with the total
fixture size: each request performs a bounded `WHERE + ORDER BY + LIMIT` scan
over `read_parquet`. The fixed per-query Parquet-scan cost therefore matters
more than the fixture's total row count. The `ORDER BY` operates on already
sorted input; use `bench` below to measure the resulting floor on your own
fixture and hardware.

### Footer sortedness stamp (schema)

Every file `swath --sort` or `sort-fixture` writes carries footer key-value metadata
(the standard Parquet extension slot, the same one Arrow uses for schemas) that make
the file self-describing serving input — **static values only, no routing data**:

| Key | Value | Meaning |
| --- | --- | --- |
| `swath.sort.order` | `key_bytes_unsigned,version_id_null_first,row_type_rank` | the total order the file is sorted in: unsigned key bytes, then `version_id` (null first), then `row_type` rank as the deterministic tail |
| `swath.sort.mode` | `objects` \| `versions` | whether the file may contain multiple versions per key; sorted **serving** refuses `versions` files self-containedly (non-versioned-only in v1) |
| `swath.sort.format_version` | `1` | stamp schema version; an unrecognized value falls back to DuckDB (`unknown_format_version`) |
| `swath.sort.file_index` | 1-based integer | this file's position in a multi-file (rolled) output; defaults to `1` when absent |
| `swath.sort.file_final` | `true` (present ONLY on the last file) | marks the last file of a multi-file output; a resolved file set proves completeness when observed `file_index` values are exactly `1..N` and only the file at index `N` carries this key — a crash mid multi-file publish leaves an incomplete set, which the server detects (`incomplete_multifile`) rather than silently serving a truncated dataset |

`--serving-mode auto` checks **every** resolved file against this schema (not just the
first) before trusting the derived index — see the fallback reasons in the
`--serving-mode` section above.

### When to use sorted vs duckdb mode

**`sorted` is the serving path; `duckdb` is the oracle and the legacy
fallback.** They are not a latency trade-off to weigh per fixture:

- **`sorted`** derives only the routing index at startup, then performs bounded
  Parquet reads while serving. It is the path to serve from whenever the
  fixture qualifies — on a large fixture, materializing everything up front
  before the first request is not a real alternative.
- **`duckdb`** materializes and indexes the whole fixture before serving. It
  earns its keep twice, neither reason being speed: it is the independent
  implementation the sorted-vs-DuckDB differential suites validate the sorted
  path against, page for page; and it is the only path for legacy captures
  (every `swath.replay.serving.fallback{reason}` lands here, and it carries the
  legacy-column backfill the sorted store deliberately omits).

The performance acceptance corridors are:

- across representative sorted fixtures spanning nearly two orders of magnitude
  in row count, the largest average server `page.read.latency` is no more than
  `2x` the smallest, with machine, software version, fixture construction, and
  benchmark settings held constant;
- sorted index derivation (`swath.replay.index.load.latency`) completes in
  `<= 60 s`; and
- average server `page.read.latency` is `<= 10 ms`.

These are acceptance criteria, not published results. This repository currently
contains no versioned benchmark evidence bundle with the fixture provenance,
machine specification, run date, and reproduction command needed to assign
a portable result. Use `bench` on representative fixtures and retain its JSON
report together with that provenance when evaluating the corridors.

#### Sequential-window prefetch (sorted path, on by default)

The sorted path wraps `SortedParquetStore` in a `WindowedListingStore`. On a
range read it looks up a cached **window** by **coverage**: among windows
sharing the request's `(toExclusive, projection)`, the one whose buffered range
covers `from` with the greatest lower bound. Position must be part of the
window's identity because a real `ListObjectsV2` carries no upper bound — every
page a concurrent work-stealing client issues shares
`(toExclusive=null, projection)`, and keying on the bounds alone would funnel
all N walkers through one cache slot, each request evicting the last one's
window. On a hit the page is served by binary-searching `from` in the buffered
rows and slicing `limit` rows (byte-identical to a fresh store read, since the
fixture is immutable).

On a miss, the fill is sized to the caller's demonstrated intent: a cold miss
reads exactly `limit` rows (a one-shot probe never pays for a window it will
not re-read), while a caller continuing from the tail of a page it was just
served — the pagination signal, which the protocol makes exact — ramps
`limit → 4×limit → … → window-rows` (default 12,500). Fully consumed fills are
not cached, so spent probe reads cannot evict live walkers' windows. A shared
LRU (default 96 windows — size it at or above the expected client concurrency)
holds the windows; delegate-final windows are dropped eagerly once served
through their last row. Memory is bounded by `max-windows × window-rows`,
never by the fixture size. `window-rows` should be at least one row group's
rows, or every fill re-decodes a whole row group to keep a fraction of it. The
`12500` default is sized to the default row-group granularity; a fixture built
with a larger `final-row-group-bytes` packs more rows per group (the 3 MiB
troubleshooting fixture is ~55k rows/group), and serving that wants `window-rows`
raised to match — left at the default it hits exactly the re-decode above. Size
the two together rather than leaving the smaller default against a coarser
fixture.

Corridor observability is preserved: `page.read.latency` measures the amortized
outer per-page call; `prefetch.window.fill` / `prefetch.window.hit` report fill
cost and hit rate; `prefetch.window.miss{reason=cold|continuation}` says
whether the ramp engaged on a given client's request mix; and
`prefetch.fill.rows` records requested fill sizes. Config (system properties,
`swath.replay.prefetch.*`): `enabled` (default `true`), `window-rows` (default
`12500`), `max-windows` (default `96`); set `enabled=false` to serve from the
bare store. The v1 window holds materialised row lists; PageBlock packing /
Arrow fills are possible future optimizations. The materialized DuckDB path is
unwrapped.

#### Delimiter skip-scan (sorted path)

A `delimiter=/` listing is answered by a native store-level skip-scan
(`ListingStore.delimitedRollup` → `SortedParquetStore`): hop the cursor to
`successor(P)` per rolled-up common prefix against the row-group routing index
the server already derives — the same algorithm S3 itself runs. Each hop
touches at most one row group's key column (a bare object directly under the
prefix is the only full-row decode), a whole row group provably inside one
common prefix is skipped off the index with no read at all, and the scan stops
at `max-keys` — so cost is **O(entries emitted), never O(subtree objects)**,
and a `max-keys=32` probe is genuinely cheaper than a `max-keys=1000` one,
exactly as on real S3. This replaced two earlier shapes that each got half of
it right: a seek-per-prefix walk through the store (correct asymptotics, but a
fixed ~31 ms store-query cost per hop that multiplied into seconds on a wide
directory) and an interim one-query `DISTINCT` rollup (fast constant, but
O(subtree) and blind to `max-keys`).

The skip-scan is gated to the single-byte `/` delimiter only; an open upper
bound (a no-prefix root rollup, or a prefix whose `0xFF`-carry has no finite
bound) is scanned to the end of the fixture, same as a plain range read.
Root rollups are exactly the wide structure probes an engine's seed phase
issues, and before the open-bound case was admitted (issue #77) they fell to
the range walk's seek-per-prefix cost — 70 s on a fixture with ~1,000 dense
date prefixes, against a ~3 s engine probe budget. Every other delimiter shape
declines to the pager's range walk. The skip-scan must return byte-identical
results to that walk — the sorted-vs-DuckDB differential suite enforces this,
and the DuckDB oracle deliberately does not take the skip-scan so the range
walk stays the reference. Which path actually served a delimited request is
visible in `swath.replay.delimiter.path` and a debug log line, so a stalled
probe is attributable to the server without a bisecting session.

On startup in the **DuckDB** path (unsorted fixtures, or `--serving-mode duckdb`),
the server materializes the swath Parquet fixture into a temporary DuckDB database.
This avoids repeated direct Parquet scans for every request. The temporary DuckDB
database is local scratch and is deleted when the server closes.

## Query It

The server expects path-style S3 `ListObjectsV2` requests:

```bash
curl 'http://127.0.0.1:19090/digitalcorpora?list-type=2&max-keys=10&encoding-type=url'
```

With swath:

```bash
swath list s3://digitalcorpora \
  --endpoint-url http://127.0.0.1:19090 \
  --force-path-style \
  --no-sign-request \
  --format jsonl \
  --checkpoint none
```

Supported request parameters:

- `list-type=2`
- `prefix`
- `delimiter`
- `start-after`
- `continuation-token`
- `max-keys`
- `encoding-type=url`
- `fetch-owner`

Continuation tokens are replay-server tokens, not real S3 tokens. Real S3
continuation tokens cannot be used against the replay server, and replay tokens
cannot be used against S3.

## Run Real-S3 Conformance

The conformance harness captures real S3 responses through Dockerized
`mitmproxy` against a chosen bucket/region, writes a swath Parquet fixture from
the same run (`swath list` against the mitmproxy endpoint), then compares the
recorded S3 XML with replay-server XML — all scratch output lives under
`/tmp/swath-replay-conformance.*` and is never committed.

For manual comparison against an existing HAR and fixture:

```bash
swath-replay-conformance \
  --har "$RUN_DIR/s3.har" \
  --fixture "$RUN_DIR/capture/data" \
  --bucket digitalcorpora \
  --mismatch-dir "$RUN_DIR/mismatches" \
  --sample-entries 200 \
  --parallelism 4 \
  --replay-parquet-connections 4
```

The comparator:

- starts the replay server in-process,
- replays path-style `ListObjectsV2` requests from the HAR,
- compares HTTP status and canonical XML,
- can compare either all eligible requests, the first `--max-entries`, or a
  deterministic spread across the HAR with `--sample-entries`,
- normalizes only values that cannot literally match, such as opaque real S3
  continuation tokens,
- writes mismatch artifacts when responses differ.

## Benchmark A Full Token-Walk (`bench`)

`bench` measures the acceptance-corridor signals described in "When to use
sorted vs duckdb mode": it starts a real
in-process replay server per requested serving mode, walks the fixture's FULL
listing page-by-page over real loopback HTTP (exactly like a client would,
following `NextContinuationToken` until `IsTruncated=false`), and reports
page/key counts, wall time, throughput, and per-page latency.

```bash
swath-replay-server bench \
  --fixture "$RUN_DIR/sorted" \
  --bucket digitalcorpora \
  --modes sorted,duckdb \
  --max-keys 1000 \
  --json "$RUN_DIR/bench-report.json"
```

`--modes` is comma-separated: give more than one (e.g. `sorted,duckdb`) to walk
the SAME fixture through each serving mode in one invocation for an easy A/B —
the report includes a ratio of every later mode against the first.
`--prefix`/`--delimiter` scope the walk the same way a real request would.
`--json PATH` is optional; without it the report goes only to stdout as a
human-readable table.

Each mode's report has three distinct latency measurements — do not conflate
them:

- **`startup_ms`** — time to construct and start the server for that mode,
  including the sorted index-derive pass (`swath.replay.index.load.latency`
  on the `auto`/`sorted` path).
- **client `ms/page`** (`avg`/`p50`/`p99`) — wall time `bench` itself observed
  per HTTP request, i.e. what a real client would see (network + full
  request/response round trip).
- **server `page.read.latency`** and **server `fixture.list.latency`**
  (`count`/`avg_ms`/`p50_ms`/`p99_ms`) — read directly off the replay server's
  own Micrometer registry, the same `swath.replay.page.read.latency`
  (store-level range read, excluding connection-pool wait) and
  `swath.replay.fixture.list.latency` (whole pager `list()` call) timers
  documented below. These are strictly smaller than the client numbers (no
  HTTP/network overhead) and are the values used for the page-read corridor.

`wall_ms`/`pages_per_sec`/`keys_per_sec` cover the walk only, not `startup_ms`.

## Metrics And Tuning

`swath-replay-conformance` prints replay-server-side metrics because it owns the
temporary replay server:

```text
replay_server_metrics wall_ms=<ms> http_requests=<count> ... parquet_queries=<count> parquet_query_ms_sum=<ms> parquet_query_ms_avg=<ms> parquet_query_ms_max=<ms> parquet_rows=<count> parquet_query_errors=0
```

Important fields:

- `wall_ms` is elapsed time for the replay server during comparison.
- `http_request_ms_sum`, `fixture_list_ms_sum`, and `parquet_query_ms_sum` are
  summed timer durations. Under parallel replay these can exceed wall time
  because requests overlap.
- `parquet_query_ms_avg` and `parquet_query_ms_max` show DuckDB/parquet read
  cost per query.
- `parquet_rows_per_query_avg` shows how much data each DuckDB query is pulling
  from the fixture.

Raising `--parallelism`/`--replay-parquet-connections` together trades wall
time against per-query cost: more parallel requests can shorten the overall
walk, but each concurrent DuckDB scan gets slower as connections start
contending on the same fixture — past a point, more parallelism buys back
less wall time than it costs in per-query latency. Profile your own fixture
and hardware rather than assuming higher is always better.

The current default is `min(4, CPUs)` for both comparator parallelism and replay
DuckDB connections.

For very large captures, prefer `--sample-entries` initially. A sampled
comparison still checks request parsing, response field shape, XML ordering,
prefix/start-after/delimiter behavior, and encoding across the captured run. It
does not prove full-scan replay throughput; run `bench` for that.

### `sort-fixture` / sorted-serving meters

These are module-local (`swath.replay.*`) Micrometer meters, separate from the
`swath.sort.*` meters swath's own `--sort` emits. See
`docs/metrics-and-observability.md` for the general meter idiom; this table
documents the replay-server-specific set.

| Meter | Type | Meaning |
| --- | --- | --- |
| `swath.replay.sortfixture.build.latency` | timer | Wall time of one `sort-fixture` run. |
| `swath.replay.sortfixture.output.bytes` | distribution | Sorted output file size(s) for that run. |
| `swath.replay.sort.steal_reason{outcome,reason}` | counter | `sort-fixture`'s registry-backed `io.varve.swath.sort.SortMetrics` adapter exposes the root sort library's engagement counters (`outcome=SORT`: `segment_flushed`, `merge_pass_cascaded`, `merge_fastpath`, `buffer_sort_fallback`, `buffer_byte_gated`). Mirrors `io.varve.swath.observability.RunMetrics#recordStealReason`'s tagging without depending on it. |
| `swath.replay.index.load.latency{source=derived}` | timer | Time to derive the in-memory row-group routing index at startup (the `source` tag anticipates a future `footer` value, once an embedded routing blob lets the server skip the derive pass). |
| `swath.replay.index.entries` | distribution | Row-group index entries produced by one derive pass. |
| `swath.replay.serving.fallback{reason}` | counter | Auto serving-mode declined sorted serving. Reasons: `no_stamp` (a resolved file carries no recognized sortedness stamp), `unsupported_mode` (a resolved file is stamped a `versions` file — unsupported for serving in v1), `unknown_format_version` (a resolved file's stamp carries a `format_version` this reader doesn't recognize), `incomplete_multifile` (the resolved file set's `file_index`/`file_final` stamps don't prove completeness — e.g. a crash left only a prefix of a multi-file publish on disk), `mixed_row_types` (a row group's `row_type` footer stats do not prove every row is `OBJECT` — e.g. a legacy delimiter'd capture re-sorted by `sort-fixture`, which stamps `mode=objects` unconditionally without checking `row_type`), `sanity_failed` (the derived row-group first-key array was not strictly ascending — a corrupt/mis-stamped/mis-ordered fixture). Every resolved file is checked for the first four reasons, not just the first one. |
| `swath.replay.serving.refused{reason}` | counter | A request the sorted path had to refuse outright, tagged with the typed reason from `io.varve.swath.sort.RowGroupOrderException`: `row_group_disorder` (a row group's own rows are not in strictly ascending key order, seen by the `delimiter=/` skip-scan's key cursor as it steps over them). NOT a `serving.fallback` reason: eligibility already passed (it proves the ascent of row-group *first* keys only), the serving path was chosen at startup, and nothing can take the request over — it fails `500`. Bumped BEFORE the throw, so the exclusion survives into a sweep's metrics; a corpus sweep classifies a disordered capture from this counter, never from the error body. |
| `swath.replay.delimiter.path{path}` | counter | Which implementation served one `delimiter=/` list request: `rollup` (the store's native skip-scan) or `walk` (the pager's seek-per-prefix range walk, i.e. the store declined). A wide structure probe landing on `walk` is the #77 cost profile — attribute a stalled probe here first. |
| `swath.replay.delimiter.skipscan.row_group_opens` | counter | Row groups the skip-scan actually opened a key cursor on (the zero-I/O whole-group skip does not count). Pinned by a regression test to stay O(prefixes emitted), never O(keys under them). |
| `swath.replay.page.read.latency` | timer | One store-level range read (`ListingStore#rows`), excluding connection-pool wait. Emitted by both stores. |
| `swath.replay.serving.path{mode}` | counter | Per-`list`-request engagement signal for the resolved serving path (`sorted` or `duckdb`), fixed once at server startup. |

`mixed_row_types` and `sanity_failed` are raised directly by the index-derive
step (`io.varve.swath.replay.fixture.SortedFixtures#loadIndex`); the other four
reasons are raised by the `--serving-mode auto` decision itself
(`io.varve.swath.replay.fixture.SortedEligibility#decide`). `--serving-mode
sorted` (a hard fail, not a fallback) does not bump this counter for any of
these reasons — it throws instead.

## Current Fidelity Limits

- Current swath fixtures carry `owner_display_name` and `checksum_type`; replay
  emits them as S3 `Owner.DisplayName` and `ChecksumType` when present. Legacy
  fixtures without `checksum_type` fall back to the older `FULL_OBJECT`
  synthesis when `checksum_algorithm` is present.
- Literal real S3 continuation tokens are not replayable.
- Only object rows are served. Versioned listings are not implemented.
- Unmodified, the server is typically *faster* per request than a real bucket
  (the delimiter skip-scan serves a wide probe in tens to low hundreds of
  milliseconds where a real backend takes seconds) — a reason to inject the
  target latency profile with `--inject-latency` rather than treat loopback
  numbers as environmental truth. The skip-scan's cost does scale with
  `max-keys` the way S3's does, so relative cost shape is faithful; absolute
  cost still needs injection.
- Repository tests exercise the HAR comparator and compare sorted and DuckDB
  serving on generated fixtures, including page-for-page differential coverage.
  They do not constitute published real-bucket or corridor-scale validation.
- No public, versioned full-walk result with fixture and machine provenance is
  currently published in this repository. Use `swath-replay-conformance` for
  fidelity checks and `bench` for the acceptance corridors, and retain the
  inputs, report, source revision, run date, and machine specification with any
  reported result.

## Possible decoded-row-group cache

Direct sorted serving uses the stamped Parquet files plus an in-memory
first-key-per-row-group index derived at startup; it writes no persistent
sidecar. A future implementation could cache decoded row groups shared across
adjacent requests to avoid repeating the per-query Parquet-scan cost. This is
not implemented; the shipped bounded sequential-window prefetch above is the
current cache layer.

## Data Policy

Real bucket captures can contain sensitive key names and metadata. Keep Parquet
fixtures, HAR files, checkpoints, and mismatch artifacts under `/tmp` or another
ignored scratch directory. Do not commit them.
