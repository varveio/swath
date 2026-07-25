# Troubleshooting bucket-shape pathologies with the replay server

A methodology for diagnosing runs that go serial: a listing that plateaus at some
fraction of the bucket and crawls the rest at a trickle, or one that burns an
outsized share of its API budget on structure probes without making forward
progress. This page assumes you've read [`swath-replay-server.md`](swath-replay-server.md)
for the tool itself; it does not repeat that reference, only how to use it as an
investigation loop.

## 1. When to reach for this

Three symptoms, one underlying failure mode: the work-stealing engine cannot find
a place to cut a busy worker's remaining range fast enough to keep the rest of
the pool fed.

- **Dense-tail / serial collapse** — the `-vv` progress line's `in_flight` falls
  and pins near `1` while `target_workers` stays high; the run finishes (or is
  killed) having spent most of its wall time draining a handful of ranges alone.
- **Stalls at N%** — total emitted plateaus at a fixed fraction of the expected
  object count and the rate that was climbing goes flat or inverts.
- **Probe storms** — `steals`/`api_calls` keep climbing while `splits` does not;
  the engine is spending its budget asking "where can I cut this?" and getting
  no usable answer back.

A live bucket cannot cheaply answer the questions you have once you see one of
these: was that RTT? Would raising a timeout help? Does a candidate engine
change actually fix it, or just move the failure? Re-running against the live
bucket to find out costs real money, real wall time — a run against a
bucket large enough to trigger this is easily an hour or more per attempt — and,
worse, it is not deterministic: two runs against the same live bucket can walk
key ranges in different orders, split at different points, and time out on
different requests, so you can never be fully sure whether a change moved the
needle or you just got a different draw.

A captured listing served locally removes all three problems: the *environment*
is deterministic — same responses, same injected costs, deterministic jitter (a
function of the request bytes, never `Math.random`) — so the seed decisions
replay byte-identically (matching `seed_shallow` counters against real S3, when
checked), the collapse reproduces in minutes instead of hours once you've cut the
fixture to the sick region, it costs zero cloud spend, and it is directly
A/B-able: swap the engine binary, hold the server/fixture/profile fixed, and any
systematic difference is attributable to that one change. What replay does *not*
freeze is the fine-grained trajectory of a highly parallel walk — a
`--concurrency 64` run still has thread-scheduling variance run to run — which is
why the A/B discipline below scores on summary counters, not wall-clock
impressions (§4).

## 2. The loop, step by step

### a. Obtain a captured sorted listing

Any prior successful `swath list ... --sort` run's output is a ready fixture —
including the run that got you here. `--sort` output is already a single
stamped, globally-sorted Parquet file; the replay server's `--serving-mode
sorted` path takes it directly, no `sort-fixture` pass needed. If the capture
predates `--sort` (or you're working from a legacy directory of unsorted
parts), run it through `sort-fixture` first — see
[`swath-replay-server.md`](swath-replay-server.md#sort-a-legacy-capture-sort-fixture).

### b. Cut it down to the misbehaving region

The full fixture reproduces the collapse, but may take a long time to walk into
its own tail. Cutting the fixture down to just the region that stalled shortens
that loop without changing the failure: the sick run's own progress line and JSON summary name the
region — `oldest_pending_range` (progress line) or `slow_ranges[].lo`/`.hi`
(summary) tell you which key prefixes were still draining when things went
bad. Keep only those prefixes, then re-sort:

```bash
duckdb -c "copy (select * from read_parquet('<fixture>/data/part-00000.parquet')
                 where cast(key as varchar) like '<prefix-1>%'
                    or cast(key as varchar) like '<prefix-2>%'
                 order by key) to '<cut-src>/part-00000.parquet' (format parquet, compression zstd);"

JAVA_OPTS="-Dswath.sort.final-row-group-bytes=3145728" \
  swath-replay-server sort-fixture --capture <cut-src> --output <cut-fixture>
```

The `cast(key as varchar) like` predicate assumes the prefixes you keep are
ASCII — true for most path-shaped keyspaces. Keys are stored and
compared as raw bytes, so for a prefix carrying non-text bytes, cast to `blob`
and bound a byte range rather than `like`; a `varchar` cast can mis-slice or
error on invalid UTF-8.

`-Dswath.sort.final-row-group-bytes=3145728` (3 MiB) is a useful starting point
for structure-probe cost on the sorted path — see the row-group pitfall below
and benchmark the fixture before picking a different value. Keeping only the
stalled prefixes usually shortens the reproduction substantially while
preserving the tail shape that matters.

One thing the cut *does* change is the collapse's *fraction*. Removing the
healthy head shifts where in the run the collapse appears because the cut
drops fast ranges that used to run ahead of it. The
reproduction criterion is the *signature* — `in_flight` pinned near 1, frozen
`splits`, the same tail keys draining alone (§3) — not the fraction. A collapse
that reproduces at a different fraction than the sick run has still reproduced;
don't read the shifted fraction as a failure to reproduce.

### c. Serve it

```bash
swath-replay-server serve \
  --fixture <cut-fixture> --bucket <bucket> \
  --host 127.0.0.1 --port 19090 --serving-mode sorted
```

`--serving-mode sorted` fails fast if the fixture isn't sorted-eligible, so you
know immediately if step (b) didn't stamp a servable file. A healthy start
prints one line naming the resolved mode and every knob that's live:

```
s3_listing_replay_server endpoint=http://127.0.0.1:19090 bucket=<bucket> fixture=<cut-fixture> serving_mode=SORTED parquet_connections=4 inject_latency=off
```

### d. Calibrate a per-shape latency profile from the sick run's own metrics

The replay server is typically *faster* per request than the real bucket you're
reproducing (the sorted path's delimiter skip-scan answers a structure probe in
tens to low hundreds of milliseconds where a real backend can take seconds), so
an uninjected local run usually will not collapse the same way — the probes
that time out at 3 s against a slow backend finish comfortably against
loopback. To reproduce the failure, inject the *sick run's own* observed
latency profile rather than guessing at one.

Pull the calibration numbers from the sick run's JSON summary, not from the
replay server — the profile has to describe the environment that failed, and
the replay server's own numbers describe the replay server:

- `probe_latency[]` gives the mean/percentile cost per call class
  (`worker_page`, `pivot_probe`, `structure_probe`) — use the `total` phase.
- `shape.delimiter_fanout.{total,probes}` gives the mean CommonPrefixes
  returned per structure probe (`total / probes`).

`structure_probe` cost on a real bucket decomposes as roughly a flat floor
(pin it to the measured `worker_page` mean — the raw data usually doesn't
separate floor from slope) plus a linear per-CommonPrefix term:

```
slope ≈ (structure_probe_mean − worker_page_mean) / mean_common_prefixes_per_probe
```

Feed the result to `--inject-latency`:

```bash
swath-replay-server serve ... \
  --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15
```

The `55ms/cp` slope shown is the current calibration of the built-in reference
profile.

The `/cp` term is the reason this lives in the server rather than in a TCP
proxy: the injector sees the parsed response, so it can charge a structure
probe in proportion to the CommonPrefixes it actually returned — reproducing
S3's own `max-keys` sensitivity (a capped, cheaper probe costs less, exactly
the lever an engine-side fanout cap pulls). A proxy only sees bytes on the
wire; it cannot tell a 4-prefix response from a 400-prefix one, so it can only
ever inject a flat per-shape delay. `prod-commoncrawl` is a reserved profile
carrying reference measurements from the public Common Crawl bucket, usable
directly against a similarly shaped fixture.

### e. Run the same swath invocation and compare

```bash
swath list s3://<bucket> --endpoint-url http://127.0.0.1:19090 --force-path-style \
  --no-sign-request --format parquet -o <out> --sort --concurrency 64 \
  --progress-interval 10s --restart
```

Use the exact flags (`--concurrency`, `--sort`, any `--engine-toggle`) the sick
run used. Watch the progress line and diff the terminal summary against the
sick run's — §3 below is what to read.

## 3. Reading the signature

Four outputs, read together, tell the whole story without a debugger.

**The progress-line trajectory** (`-vv`, every `--progress-interval`):
`in_flight` vs `target_workers` — how much of the configured concurrency is
actually busy; `live_rate` — the windowed keys/sec, so a stall shows up
immediately rather than being smoothed into `avg_rate`; `steals` vs `splits` —
climbing steals with flat splits is the probe-storm signature (a lot of
attempts, nothing committing); `oldest_pending_range` / `cursor` — a frozen
cursor with climbing `splits`/`api_calls` is the livelock signature.

**The summary's `trajectory` block** (end-of-run JSON): `serial_frac` — the
fraction of wall time spent at `<= 2` in-flight; `collapse_at_frac` — the
fractional point in the run where a *trailing* run of `<= 2` in-flight began
(`null` if the run never permanently collapsed); `peak_workers` /
`final_workers` — how parallel the run got vs. how parallel it ended.
Alongside it, `efficiency.steal_success_rate` (`CHILD_CREATED` outcomes over
total steal attempts) turns "steals climbing" into a number: near-zero means
every attempt is failing.

**`seed.decisions[]`** — one entry per `delimiter=/` structure probe the seed
step issued, `{prefix, fanout, truncated, classification, cuts_kept,
cuts_discarded}`. This is a replayable record of every seed-time probe, and it
answers questions no live rerun can, because it's exact and it's already
there. Worked example from the reference profile: a seed variant that
descends per-cut into dense subtrees probed `contrib/datacomp/DCLM-pool/`
(`fanout=1`, not truncated) and enqueued its child `jsonl/` for a follow-up
probe — but no later entry in `decisions[]` ever names `jsonl/`. Cross-checking
the run's own `probes`/`cut_points` counts showed the seed's cut budget
(`targetSeeds`) had already been reached by cheaper subtrees earlier in the
FIFO frontier order, so the descent stopped before it ever reached `jsonl/`.
That's a frontier-starvation diagnosis made entirely from the trace — a path
that *should* fire and visibly does not, read straight out of the array, no
rerun and no debugger required.

That run predates the best-first seed frontier this branch ships, so read it as
*method*, not current behavior. The older FIFO one-shot descent could stop once
the cut-point cap (`targetSeeds`) was hit — exactly the starvation this trace
caught. The current seed pass keeps descending past `targetSeeds` and trims an
over-cap cut set afterward by a mass-weighted subsample (see
[`algorithms.md`](internals/algorithms.md)), so a present-day trace won't
reproduce this specific stop; it stands here as a worked example of reading
starvation straight from `decisions[]`.

**The checkpoint** (`.swath/checkpoint.sqlite`, `listing_node` table) — the
durable record of every range the engine ever created, whether it survived to
completion. Query it directly with `sqlite3`:

```sql
-- every range not yet COMPLETED at the moment you queried (or the run stalled)
SELECT id, parent_id, hex(range_start) AS lo, hex(range_end) AS hi, hex(cursor) AS cursor,
       status, pages_emitted, api_calls
FROM listing_node
WHERE run_id = (SELECT id FROM run_meta ORDER BY id DESC LIMIT 1)
  AND status != 'COMPLETED';
```

On a collapsed run this typically returns one or two survivors out of
hundreds — the exact ranges the tail is stuck draining. Worked example, from
the reference case: walking `parent_id` back from a survivor to a
node with `parent_id IS NULL` (an original seed range, never itself a
product of a split) landed on the seed range covering
`projects/headers-testing/` in its entirety — 8.1M objects, 121 uniform
sub-directories, and exactly one seed range. That single query exposed the
seed-layout hole directly: the shallow seed had stopped descending one level
too early and folded a whole dense, trivially splittable subtree into one
range before the engine ever got a chance to split it at run time. The same
table also quantifies how lopsided a run's split tree got — most completed
nodes finishing in a handful of pages while one or two seed ranges never got
subdivided is the checkpoint-level view of the same collapse:

```sql
SELECT COUNT(*) FILTER (WHERE pages_emitted < 10) AS thin, COUNT(*) AS total
FROM listing_node
WHERE run_id = (SELECT id FROM run_meta ORDER BY id DESC LIMIT 1) AND status = 'COMPLETED';
```

## 4. A/B discipline

To attribute a trajectory difference to an engine change and nothing else:

1. **Build the stock engine at the base commit in a separate worktree.**
   `git worktree add ../swath-stock <base-commit>` and build the CLI dist
   there; keep the branch under test in this worktree.
2. **Hold everything but the engine fixed.** Same server process, same
   fixture, same `--inject-latency` profile, same `swath` flags — only the
   `swath` binary invoked differs between arms.
3. **Score on the trajectory and the summary counters, never on
   impressions.** `serial_frac`, `collapse_at_frac`, `final_workers`,
   `efficiency.steal_success_rate`, plus whatever engagement counters your
   change touches (e.g. `STRUCTURE.fanout_capped`, `PIVOT.structure_capped`)
   — not "it looked faster."
4. **Run the arms sequentially on a quiet box.** A concurrent build (Gradle,
   or anything else CPU-heavy) perturbs the same probe-cost margins you're
   trying to measure — a structure probe that would clear a 3 s budget with
   the box otherwise idle can miss it under unrelated load, producing an
   apparent regression that is really contention.
5. **Make sure the server's own request cost is negligible next to the
   injected margin, or the harness measures itself.** A delimiter path whose
   real scan cost approaches the injected timeout margin can produce a false
   negative for an otherwise-correct engine change. Confirm the uninjected,
   idle-server floor for your busiest shape (`bench`, §7) is small relative to
   your injected margin before trusting an A/B.
6. **Verify each arm's binary actually contains the change under test.** An A/B
   is only meaningful if the two dists differ — a rebuild that silently no-ops (a
   cached task, a build that reports success without repackaging the dist, a
   worktree left on the wrong commit) leaves you comparing two copies of the same
   engine and reading the identical numbers as "no effect." Before trusting a
   result, confirm the binary under test carries the change: inspect the built classes for
   the value you changed (`javap -p -c` on the class, or a string grep of the
   dist for the new constant), check a build stamp or the class-file mtime
   against the commit, or run a one-shot that exercises the changed path and
   emits a distinguishing counter. Cheap to check; a wasted day of A/B is the
   cost of not checking.
7. **Start each arm from clean state.** Point every arm at its own fresh `-o`
   output path, and don't let a prior arm's checkpoint carry over. The §2e
   invocation passes `--restart`, which discards any existing checkpoint for that
   run and lists from scratch — so re-running into the same `-o` won't silently
   *resume* a half-finished prior arm — but a leftover output directory from a
   crashed arm can still be mistaken for the next arm's result, so give each arm
   its own destination.

A valid comparison holds the server, fixture, and injected profile identical
across both arms, changes one engine mechanism, and evaluates completion plus
the engagement counters named above. Record the exact configuration with the
result so the comparison is reproducible without relying on campaign history.

## 5. Fidelity boundaries

What the harness proves:

- **Mechanism.** The collapse reproduces from the engine's own behavior, not
  from network variance — it reproduces at ~0 ms RTT on loopback.
- **Keyspace decisions.** Comparing seed and split counters between a capture
  and its replay reveals whether the server preserves the structural choices
  that matter to the engine; agreement is the fidelity check, not an assumed
  property of the fixture.
- **Protocol conformance.** The sorted-vs-DuckDB differential suite proves the
  serving path byte-identical to its own oracle, page for page.

What it does not prove without extra care:

- **Absolute latencies, without injection.** Unmodified, the server is
  typically faster per request than a real bucket. Loopback numbers are not
  environmental truth; inject the target profile (§2d) before drawing
  conclusions about timing.
- **A single box under dozens of concurrent heavy scans.** A single-process
  replay server shares one machine's cores across every concurrent structure
  probe, where a real object store answers each on independent capacity. The
  native delimiter skip-scan removed the specific failure mode this used to
  cause (an O(subtree) rollup scan saturating the box under a probe storm and
  producing false collapses) — see §4 point 5 — but a single process is still
  a single process; don't assume concurrency fidelity you haven't checked.
- **RTT effects.** Somewhat counterintuitively, this harness is *good*
  evidence against an RTT hypothesis, not just silent on it: in the
  reference case, injecting the measured round-trip latency
  of a slow client (500 ms ± 150 ms jitter, via a TCP proxy) scaled the
  healthy-phase throughput down proportionally but left the collapse's onset
  point and its terminal `in_flight=1` point essentially unchanged — RTT
  was directly tested and found not to be the driver of that collapse. Don't
  assume the same holds for a different pathology; test it the same way
  rather than inferring it.

## 6. Pitfalls

Each of these cost real time in practice.

- **Toolchain JDK required.** The built `swath`/`swath-replay-server`
  distributions need the toolchain JDK your Gradle build used (this repo
  currently builds class files that require JDK 25); the `java` on `PATH` in
  a general-purpose container or CI image is often an older LTS. Run every
  invocation with `JAVA_HOME` pointed at the toolchain JDK explicitly, or
  you'll hit an opaque `UnsupportedClassVersionError`.
- **`pkill -f` can self-kill.** `pkill -f "swath list ..."` matches any
  process whose command line contains that text — including the shell
  wrapper or script that's driving the run, if the pattern also appears
  there. It silently kills your own driver (exiting non-zero) while leaving
  the actual load running, poisoning every measurement taken afterward. Kill
  by PID.
- **Cold-JVM measurements lie.** A freshly started server, an unwarmed DuckDB
  connection pool, and a cold page cache on the fixture file all inflate
  early numbers by an order of magnitude or more compared to steady state.
  Always run a warm-up pass before taking a timing measurement, and prefer
  a walk long enough to amortize startup rather than a handful of one-shot
  requests against a server that just started.
- **`final-row-group-bytes` counts uncompressed bytes, and the mapping to row
  count is non-linear.** It's a flush heuristic, not a target — the row count
  per group has to be calibrated empirically per fixture, not assumed
  proportional to the byte value you passed.
- **`prefetch.window-rows` must be at least one row group's row count.**
  Below that, every window fill decodes a whole row group just to keep a
  fraction of it, and the prefetch degenerates into repeatedly re-decoding
  the same row group — measurably worse than having no prefetch at all. Set
  row-group size first, then size `window-rows` against it.
- **Don't run two `swath list` invocations against one replay server
  concurrently, unless that's specifically what you're testing.** The server
  has finite DuckDB connections and a finite window cache; unrelated
  concurrent load will corrupt whatever you think you're isolating.
- **Port hygiene for side-by-side servers.** When comparing two arms with two
  separate server processes (e.g. one per engine build), pin explicit
  `--port` values rather than the ephemeral default — it's easy to point a
  client at the wrong server by accident when both bind ports assigned at
  random.

## 7. Extending it

Fault injection has three layers today, at three different levels of
readiness. Know which one answers your question before reaching for a proxy.

- **What exists: in-server, shape-aware latency (`--inject-latency`, §2d).**
  See [`swath-replay-server.md`](swath-replay-server.md#fault-latency-injection---inject-latency)
  for the flag itself. It's the only layer that sees the parsed response, so
  it's also the only one that can charge a probe for what its response
  actually carried — the `/cp` term on `structure_probe` — rather than a
  flat per-shape delay. Jitter is a deterministic function of the request
  bytes, never `Math.random`, so the injected delay for a given request is
  reproducible run to run — the *latency profile* replays exactly, even though
  the fine-grained trajectory of a parallel walk still varies with scheduling
  (§1, §2d).
- **What does not exist yet: in-server API-level errors.** The injector hook
  (`ShapeLatency`, wired through `ReplayHandler`) returns a `Duration` only —
  there is no seam to make it return a 503 SlowDown, any other 5xx, a
  connection reset mid-response, or a truncated/malformed body. Those are
  exactly the inputs the engine's retry/backoff/AIMD paths exist to react to,
  and captured runs may include real 503 SlowDown responses that this harness
  currently has no way to replay. Until an HTTP-level error injector exists,
  error-path testing has two options: the toxiproxy layer below (for
  transport-level faults), or, for API-level errors and their retry/backoff
  handling in isolation, swath-core's in-process `MockPageFetcher` test
  harness (`io.varve.swath.testkit`) — its `PageInterceptor` seam already
  supports deterministic, per-call fault injection, including throwing a
  `ThrottleException` shaped as a real S3 503 SlowDown, an attempt timeout, or
  network exhaustion, as well as page-level faults like a kill-at-page cutoff
  (crash/resume testing) and stuck continuation tokens. It runs in-process
  against the engine, not over HTTP, so it's the current home of deterministic
  error-path coverage, not a replacement for exercising the real client/server
  wire path.
- **What is proven but unpackaged: TCP-level toxiproxy.** A latency toxic in
  front of the replay server can reproduce a
  slow client's measured TTFB profile and scaled the healthy-phase throughput
  down proportionally — and, in doing so, falsified the RTT hypothesis for
  that collapse without moving the collapse's onset point (§5). That's the
  kind of negative result only a controlled, independent layer gives you.
  `reset_peer`, `timeout`, `slicer` (partial reads), and bandwidth-cap toxics
  are transport-only faults the in-server injector can never express, no
  matter how it's extended — a mid-response connection reset happens below
  the layer that parses a response into a `Duration`. The tradeoff is that
  toxiproxy is shape-blind: it injects the same delay or fault on every
  request regardless of shape, so never reach for it to model per-shape cost
  (that's §2d's job). The wiring itself works but is a one-off today, and
  sibling-container networking has sharp edges of its own. A repeatable public
  recipe is not packaged yet.
- **Which layer for which question.** Cost profile / shape sensitivity (does
  a fanout cap actually reduce structure-probe cost?) → in-server latency
  (§2d). Error/retry semantics (does the engine back off correctly on a 503
  storm?) → `MockPageFetcher` today, and in-server errors once an HTTP-level
  injector exists. Transport faults / link behavior (does a reset mid-page surface
  as the right exception?) → toxiproxy.
- **Where the per-shape injector stops and a custom profile starts.**
  `--inject-latency` classifies every request into exactly three shapes —
  `worker_page`, `pivot_probe`, `structure_probe` — from the request alone
  (a `delimiter` present is a structure probe; a bare `max-keys<=1` is a
  pivot probe; anything else is a worker page), with an optional
  response-proportional per-CommonPrefix term available only on
  `structure_probe`. A custom `shape=delay[+delay/cp]` spec (see the grammar
  in §2d) covers any profile expressible in those terms. If your pathology
  needs a cost model the classifier can't express — cost that depends on
  prefix depth, on object count under a range rather than CommonPrefixes
  returned, or on which worker is asking — that's past what the shipped
  injector does, and the next step is a small extension to its classifier
  rather than forcing an existing shape to stand in for a different one.
- **`bench` for clean per-shape floors.** `swath-replay-server bench
  --fixture ... --prefix P [--delimiter /] --max-keys N` against an idle,
  uninjected server gives the harness's own floor for a shape — the number a
  fault profile is injected *on top of*, not instead of. Run it before
  trusting an A/B (§4 point 5): if the idle floor for your busiest shape
  isn't small relative to the margin you're injecting, the harness is
  measuring itself.
