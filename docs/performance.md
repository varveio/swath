# swath performance

This page defines the measurements required before swath makes quantitative
performance or scale claims: scaling, memory, resume cost, throughput, and
known slow paths. There is no published release-candidate measurement bundle
yet. Where a section carries numbers they are **single-machine field
observations**, labelled as such and stamped; where it carries none it says so.
Neither is an RC measurement.

For a head-to-head against other S3 listing tools, see the
[S3-listing comparison study](https://github.com/varveio/s3-listing-study),
which is built for exactly that and commits to what it measures before it
measures it. Its comparative runs have not started yet, so it carries a
[methodology](https://github.com/varveio/s3-listing-study/blob/main/docs/methodology.md)
and a tool roster but no results so far.

Most sections now carry **field observations** rather than release-candidate
measurements: single-machine, single-vantage runs against public buckets,
stamped with version/date/machine/bucket per the [Methodology](#methodology)
bar. They are reported because they are more useful than an empty placeholder,
and because the *method* for reproducing them on your own bucket is written
alongside — see [Diagnosing a run](#diagnosing-a-run),
which also explains why absolute numbers should not be ported between buckets.
A release-candidate bundle (multiple machines, repeated runs, in-region
vantage) is still outstanding, and where a section has no data at all it says
so plainly.

## The machine these figures ran on

Every field observation on this page comes from one host. Stated in full,
because several of its properties materially shape the numbers:

| | |
|---|---|
| Instance | GCP `c4a-highcpu-32`, zone `us-east1-b` |
| CPU | **arm64** — Google Axion, ARM Neoverse-V2, 32 vCPU |
| SMT | none (1 thread per core, 32 physical cores) |
| Memory | 62 GiB |
| Disk | 193 GB SSD-backed root volume (non-rotational); staging and output both landed here |
| Kernel | Linux 6.17 (`aarch64`) |
| JDK | Temurin 25.0.3+9 LTS |
| swath | 0.2.2-dev |
| Date | 2026-08-07 |

**Why each of these can move a number:**

- **arm64, not x86.** The merge phase leans on CRC32C, LZ4, and Parquet
  encode/decode, all of which have architecture-specific paths. Merge-phase
  timings in particular should not be assumed to carry to x86 unchanged.
- **No SMT.** 32 vCPU here means 32 *physical* cores, so CPU-efficiency figures
  are not diluted by sibling contention the way they would be on a
  32-vCPU/16-core x86 instance.
- **Cross-cloud, and partly cross-continent.** The client sat in GCP
  `us-east1`; `pds-css-archive` is in AWS `us-west-2` (cross-cloud *and*
  coast-to-coast) and `noaa-gefs-retrospective` is in AWS `us-east-1`
  (cross-cloud, same coast). Latency is the denominator in
  `throughput ≈ in-flight ÷ latency`, so this vantage inflates every concurrency
  figure on the page — but by less than the distance suggests, because most of a
  LIST's latency is not travel time. Measured per request (`probe_latency`,
  `worker_page`, p50) against the ICMP round trip to the same endpoint:

  | client → bucket | network RTT | `ttfb` | `total` | ⇒ S3's own page work |
  | --- | ---: | ---: | ---: | ---: |
  | GCP `us-east1` → AWS `us-east-1` | 17.9 ms | 115.3 ms | 119.5 ms | ~97 ms |
  | GCP `us-east1` → AWS `us-west-2` | 79.9 ms | 163.6 ms | 172.0 ms | ~84 ms |

  The time S3 spends enumerating and serialising a 1000-key page — ~85–97 ms — is
  the **majority of the latency, and it is the same in both regions**. Moving
  in-region removes the ~62 ms RTT delta, which by
  `throughput ≈ in-flight ÷ latency` cuts the in-flight needed for a given rate by
  roughly a third. Worth having; not the step change "coast-to-coast" implies.
- **Local disk for staging.** `--sort` staging and the final output shared one
  local SSD. A network-attached or slower volume changes the merge phase's
  profile.
- **Single host, n = 1 per point.** See [Methodology](#methodology).

## Scaling behavior

How listing time and LIST-call count grow as object count rises across bucket
shapes (deep prefix trees, flat key spaces, skewed distributions).

**LIST-call growth is essentially optimal and shape-insensitive.** Across every
run below, `efficiency.api_calls_per_1k_objects` sat between **1.016 and 1.089**
against a theoretical floor of 1.0 (1 000 keys per LIST response), with
`page_fill_ratio` at 1.0000 and `overfetch_ratio` 1.008–1.050. Call count is therefore ~linear in object count with a small constant across the
buckets measured, and the interesting variable is not calls but **wall clock**,
which is governed by how many requests the engine can keep in flight. Two
buckets is a thin basis for calling this shape-INSENSITIVE, so read it as "no
shape effect was visible here", not as a general property.

**What is NOT established here:** a like-for-like sweep of object count at fixed
shape. The two buckets measured differ in shape as well as size, so no
size-scaling exponent should be inferred from them. That remains an RC-bundle
item.

> _Buckets: `noaa-gefs-retrospective` (9.9 M objects, AWS us-east-1) and
> `pds-css-archive` (96.0 M, AWS us-west-2). Host: see
> [The machine these figures ran on](#the-machine-these-figures-ran-on)._

## Memory behavior and current evidence

The implementation bounds active page, queue, writer, and merge buffers with
configuration. That is not a constant-memory claim for the complete process:
unsorted Parquet retains `O(parts)` finalized-part metadata, rewrites an
`O(parts)` manifest on each finalize (cumulative `O(parts²)` serialization),
and sorted output retains `O(segments)` staging metadata. Larger parts and sort
segments reduce those counts.

The current public PERF-2 gate covers 100,000 keys and asserts peak heap below
1 GB for default Parquet settings. It does not establish the same peak at
million- or billion-object scale, and there is not yet a documented, enforced
maximum part/segment-count envelope. Publish a stamped larger-scale measurement
and an operating envelope before making a stronger bounded-memory claim.

**Field observation: peak heap tracked `--concurrency` far more strongly than
object count.** (Two buckets, no fixed-shape object-count sweep — the very
measurement this page still lists as outstanding — so this is directional
evidence for the design intent, not a demonstration of it.)
Unsorted Parquet output, default settings, one bucket of 96 022 559 objects
(960× the PERF-2 gate's scale):

| `--concurrency` | peak heap | peak RSS |
|---:|---:|---:|
| 32 | **0.92 GB** | 1.27 GB |
| 64 | 2.24 GB | 2.51 GB |
| 128 | 3.24 GB | 3.52 GB |
| 256 | 4.80 GB | 4.92 GB |
| 512 | 5.25 GB | 5.66 GB |

At `--concurrency 32` a 96 M-object listing held peak heap **under the 1 GB
PERF-2 figure at 960× the gate's object count**, and a 9.9 M-object listing at
`--concurrency 256` peaked *higher* (2.70 GB) than the 96 M one at
`--concurrency 32`. Both point the same way: the in-flight page buffers dominate,
so the operating envelope is a function of the concurrency ceiling and not of how
many objects the bucket holds.

This supports the design intent but does **not** discharge the caveat above. It
is one bucket, one machine, one run per point, unsorted only; `--sort` adds
staging and merge buffers whose sizing is deliberately heap-adaptive (a function
of `-Xmx`, see [`configuration.md`](configuration.md#jvm-system-properties)), and
no maximum part/segment-count envelope is enforced yet.

> _Bucket: `pds-css-archive` (96.0 M, AWS us-west-2), unsorted Parquet. Host:
> see [The machine these figures ran on](#the-machine-these-figures-ran-on)._

## Resume cost

The overhead of resuming an interrupted run: time to reopen the checkpoint and
the LIST calls a resume spends versus a run that completes in one pass.

> _No measurement. This one genuinely has no data behind it yet — it was not
> exercised in any run on this page, and nothing here should be read as evidence
> about resume cost._

## Throughput

Sustained keys listed per second, and the LIST-request rate that throughput
implies, under representative concurrency settings.

Observed peak on the measured buckets: **~655 000 keys/s** (≈ 666 LIST/s) at
`--concurrency 256`, on a keyspace that could not fill a higher ceiling. The full
sweep, and why 512 is *slower* than 256 on that bucket, is in
[Diagnosing a run](#field-observations-stamped-and-shape-specific).

The second, ten-times-smaller bucket reached ~426 000 keys/s in its listing phase
at the same concurrency — **lower**, not higher, and not a like-for-like
comparison: its whole listing lasted 23 s (so ramp-up is a large fraction of it)
and it produced only 148 splits against the larger bucket's ~4 900, so there was
far less parallel work to spread. It is reported here only to show that keys/s
does not follow object count.

Two caveats that matter more than the numbers:

- **These are cross-cloud** (GCP client → AWS S3). Per-request latency was ~172 ms
  and flat, of which only ~80 ms is the network round trip — the rest is S3's own
  page-production time, which an in-region client still pays. Moving in-region cuts
  the in-flight needed for a given rate by roughly a third, not by the factor the
  distance suggests, because throughput ≈ in-flight ÷ latency and latency does not
  collapse. See [The machine these figures ran on](#the-machine-these-figures-ran-on)
  for the measured breakdown.
- **Throughput is shape-bound, not size-bound.** The ceiling here was the
  engine's ability to manufacture splittable ranges on a deep-divergence
  keyspace, not the remote and not CPU.

> _Host and buckets: see [The machine these figures ran on](#the-machine-these-figures-ran-on)._

## The 0.2.0 scheduling defaults (`rate_anchored_sensing` + `tail_floor`)

0.2.0 turns on two engine mechanisms that previously shipped opt-in: the
rate-anchored position sensor and the `reach_floored` owner-split child-tail
floor. They are documented individually in
[usage.md](usage.md#new-mechanism-performance-toggles--defaults-and-cost-profile);
this section is the evidence for making them the default, and the reason a
rollback path exists.

**What they fix.** On a *wide-flat* tail — a range with a large number of keys
left but a very thin trailing density — the pre-0.2.0 child-tail floor scores the
child as `est × max(0, min(1, densityRatio) − f)`. With the trailing ratio at
~3e-4 against `f`≈0.5 the product is exactly zero *regardless of the estimate*, so
the owner is refused every time it tries to carve, and the range drains on one
worker for the rest of the run. The pair makes that estimate honest and the floor
reachable.

**Measured, on a real store.** A 13.5M-key public dataset bucket in `us-east-2`,
arms run serially and alternating, two repetitions each, all four runs completing
the full key set:

| arm | wall | keys/s | avg in-flight | API calls |
| --- | --- | --- | --- | --- |
| pre-0.2.0 default, rep 1 | 702.9s | 19,263 | 2.2 | 15,066 |
| pre-0.2.0 default, rep 2 | 672.1s | 20,147 | 2.1 | 14,946 |
| 0.2.0 default, rep 1 | **62.4s** | 216,985 | 24.1 | 15,009 |
| 0.2.0 default, rep 2 | **57.7s** | 234,537 | 26.6 | 14,879 |

≈11.3× and ≈11.6× end-to-end on the two repetitions, at slightly *fewer* API calls —
the fleet stops idling, it does not start over-fetching. Comparing the worst new run
against the best old one, the least favourable reading available, it is still 10.8×.

**Breadth, and the correctness claim.** Both arms were then replayed over a
123-fixture capture corpus (465M keys total, every fixture ≤20M keys) against the
real engine. **114 of the 123 completed.** Across those 114, the emitted key set was
byte-identical between arms on every one — the pair changes scheduling, never output —
and total API calls moved −0.4%. Ten fixtures gained ≥1.5×. **Twelve fixtures
regressed 5–23%** under the new defaults, verified serially on a quiet box: one shared
mechanism (on those shapes the pair splits work more aggressively than the extra
parallelism pays back). Output is byte-identical on every one; an opt-in recovery
mechanism has been measured and is tracked separately.

Nine further fixtures initially produced no result on either arm. The cause was a
replay-*harness* defect, not the engine — the server answered a root delimiter rollup
by walking ~1,000 per-prefix queries (~70s) against a ~3s client probe budget — and
fixing it (the server's native skip-scan now serves open-upper-bound rollups; 70.34s →
0.054s, responses byte-identical to the walk's) restored all nine: **18/18 runs
complete, zero keyset differences**, so the byte-identical claim covers the full
123-fixture corpus. Eight of the nine are roughly arm-neutral; one deep-nested
forecast bucket is near-serial pre-flip and **2.0×** under the 0.2.0 defaults.

**Honest limits.** The pair cures the wide-flat tail; it does not make every
bucket parallel. Keyspaces that are deep and narrow enough that a pivot has almost
no alphabet to cut against remain serial, identically so on both arms — the
mechanism there is unsplittability, not the floor, and it is tracked separately.
Buckets above 20M keys are not covered by the corpus panel; the largest live proof
point is the 13.5M-key run above.

**Rollback.** Pre-0.2.0 engine behaviour is exactly reachable with
`--engine-toggle rate_anchored_sensing=off --engine-toggle tail_floor=current`.
No output changes either way, so switching arms is safe mid-campaign.

_Measured 2026-07-28 on swath 0.2.0-SNAPSHOT, 8-core/26GB Linux box; live arm
against S3 `us-east-2`, replay arm against the bundled replay server at a
uniform latency profile. Public data only._

## The 0.2.0 seed change: the scope-closing sentinel

The seed pass tiles its cut-points as `(⊥,c1], …, (c_last, null]`, and the owner-split
governor refuses to split any range whose upper bound is open — so whatever mass sorts
past the last cut drains on **one worker, serially, however large it is**. On one real
4.97M-key geoscience bucket, 95.2% of the keys sat under the *last* top-level prefix:
the seed's own tiling made almost the whole bucket unsplittable.

0.2.0 closes the scope when doing so is provable: if the top level was listed to
completion and its greatest returned item is a common prefix `p/`, every key in scope
is strictly below `prefixCeil(p/)`, and that bound is appended as one final cut — zero
extra probes, one extra cut, the mass-bearing range gains a finite upper bound and the
runtime splitter takes it from there. The bound is verified, never assumed (a direct
object sorting past the last prefix declines it), and every decline reason is a named
counter.

On that bucket, replayed from a captured listing at a compressed latency profile
(64 workers): open-frontier key share **0.952 → 0.000**, serial fraction 0.945 →
0.001, runtime splits 15 → **482**, wall **−36%**, at *fewer* API calls (6,736 →
5,797). Emitted key sets are byte-identical, there and across a 9-fixture control
smoke whose walls all sit within measurement noise. The one disclosed cost: the final
tile is empty by construction but still exists — one LIST spent, and a known small
distortion of victim scoring, tracked separately.

_Measured 2026-07-29 on swath 0.2.0-SNAPSHOT, 8-core/26GB Linux box, bundled replay
server over captured public-bucket listings; wall change is relative within the
bench's compressed latency profile, never an absolute claim. Public data only._

<a id="diagnosing-a-run"></a>

## Diagnosing a run — and why these numbers do not transfer between buckets

This section is **method, not a claim**. The figures in it are stamped field
observations from a single dev machine (see the stamp), included because the
*shape* of the reasoning transfers even where the numbers do not. Nothing here
is a release-candidate measurement.

### The one number to look at first: in-flight utilisation

`--concurrency N` sets a **ceiling** (`Tmax`), not an achieved rate. What a run
actually sustains is `engine.avg_in_flight`, and the ratio that matters is:

```
utilisation = engine.avg_in_flight / <the --concurrency you set>
```

Use the **configured** ceiling, not `engine.peak_in_flight`. A starved run whose
in-flight briefly touched 10 under `--concurrency 512` would look 90 % utilised
against its own peak while being 2 % utilised against what you asked for — and
the second number is the one that tells you whether the setting is doing
anything. (The tables below use configured concurrency: 29.9/32 = 93 %,
93.2/512 = 18 %.)

By Little's law, `throughput ≈ in-flight ÷ per-request latency`. So if
utilisation is high, the ceiling is binding and raising it may help. If it is
low, the engine could not keep the ceiling full and **raising `--concurrency`
makes things worse, not better** — you add scheduling overhead to fetch work
that does not exist yet.

One caveat when reading a `--sort` run: `avg_in_flight` is averaged over the
WHOLE run, and in-flight is zero for the entire merge phase, so a sorted run's
figure is diluted. Rescale it to the listing phase before judging:
`avg_in_flight × duration_ms / (duration_ms − sort.merge_ms)`. An unsorted run
needs no correction.

### Is the wall the remote, or you?

Derive per-request latency across a concurrency sweep:

```
latency ≈ engine.avg_in_flight ÷ (engine.pages ÷ wall_seconds)
```

- **Latency rises with concurrency** ⇒ *something downstream of the client is
  saturating*. Most often that is the remote or the AIMD controller reacting to
  it — but this figure is derived from in-flight and request rate, so client CPU
  exhaustion and queueing inflate it too. Check `cpu_efficiency` against your
  core count before concluding it is the remote.
- **Latency is flat while in-flight stops growing** ⇒ the limit is on your side:
  the work-stealing engine is not producing splittable ranges fast enough. Look
  at `engine.splits` and `engine.steals` — if `splits` plateaus and `steals`
  stays flat as you raise concurrency, extra worker slots have nothing to take.

Flat throttle/recovery counters are consistent with an unstressed remote but do
not prove one; they are evidence, not proof.

Corroborating signals for a genuinely throttling remote:
`recovered_errors.latency_freezes` climbing, `recovered_errors.min_effective_t`
falling, non-zero `recovered_errors.connection_aborted`. If those are flat while
throughput stalls, it is not the remote.

### Sizing CPU

`efficiency.cpu_seconds ÷ (objects ÷ 1e6)` gives **CPU-seconds per million
keys**. In field observation it is nearly invariant. Across a controlled 16-arm sweep of
1, 2, 4 and 8 client cores at concurrency 8–64 — run against a **local replay
fixture** (a 9.9 M-key capture of `noaa-gefs-retrospective` served back over
HTTP with 100 ms of injected per-request latency, client and server pinned to
disjoint cores), not against S3 — throughput per BUSY core
(`keys_per_sec ÷ cpu_efficiency`) stayed within **118 000–136 000 keys/s, mean
~129 000 (±7 %)**, and the same CPU-per-key figure appeared on unrelated
cross-cloud runs against real S3. So a workable model is:

```
throughput ≈ min( cores × ~129 000 ,  concurrency × 1000 ÷ latency_seconds )
```

**It is not perfectly flat, and the drift has a direction.** CPU-per-key rises
with core count — ~7.5 CPU-s/Mkey at 1–2 cores, ~7.7 at 4, ~8.2 at 8 — a
coordination cost (more GC and JIT threads, more scheduling) of roughly 7–8 %
from 1 core to 8. Sizing a large-core box from a small-core measurement will
therefore under-estimate slightly; add headroom accordingly.

Both terms come from a run's own summary, and **whichever is smaller tells you
which knob to turn**: if the CPU term binds, add cores; if the latency term
binds, raise `--concurrency` (or reduce latency by moving in-region).

Two caveats. The constant is workload- and CPU-specific — it was measured on
arm64 for `objects`-mode Parquet output, and key length, filters, and output
format all move it, so derive your own from one run rather than adopting this
number. And it degrades once concurrency overshoots: the one arm in that sweep
that regressed also showed CPU-per-key rising, which is the signal below.

### Recognising "concurrency is set too high"

The signature is the same whatever resource actually ran out — remote capacity,
client CPU, or the engine's supply of splittable ranges:

1. **In-flight utilisation collapses** (`avg_in_flight` falls away from the ceiling),
2. **measured latency inflates** (now including queueing, not just service time),
3. **throughput falls**, and
4. **CPU-per-key rises** — you are paying more to do less.

Observed twice from unrelated causes: against real S3 at `--concurrency 512`,
where the keyspace could not supply splits fast enough (throughput −21 %,
utilisation 46 % → 18 %); and against a fixture at `--concurrency 64` pinned to a
single core, where the core was the scarce resource (throughput −7 %, utilisation
73 % → 58 %). **You do not need to know which resource bound to know you have
overshot** — back the ceiling off until utilisation recovers.

### Predicting split supply from the summary's shape block

`shape` describes the keyspace the run actually saw:

- `divergence_depth_histogram` — how deep into the key a split point becomes
  distinguishable. Mass in the DEEP buckets means each new range costs a deeper
  probe.
- `mass_skew_gini` — how unevenly objects are distributed across ranges.
- `delimiter_fanout` — how much branching a structure probe returns.

**Treat these as hypothesis-generating, not diagnostic — that is what the
evidence here supports.** Both buckets measured on this page put their entire
divergence histogram in the deepest bucket, so that signal did not discriminate
between them, and no shallow-divergence bucket was measured at all. The claim
that shallow divergence fills a high ceiling is a *prediction* from how the
split/steal engine works, not something these runs establish.

What the runs DO establish is the direct evidence of split starvation, and it
comes from the engine counters rather than the shape block: `engine.splits`
plateauing and `engine.steals` staying flat as `--concurrency` rises, while
per-request latency does not move. Read those first; use `shape` to form a guess
about *why*.

### Field observations (stamped, and shape-specific)

Bucket `pds-css-archive` (96 022 559 objects, AWS us-west-2), `--no-sort`, one
run per point, from the arm64 host described in
[The machine these figures ran on](#the-machine-these-figures-ran-on) — GCP
us-east1, so cross-cloud and coast-to-coast, which inflates the concurrency
needed per unit of throughput relative to an in-region client:

| `--concurrency` | keys/s | avg in-flight (utilisation) | CPU-s per Mkey |
|---:|---:|---:|---:|
| 32 | 165 831 | 29.9 (93 %) | 7.42 |
| 64 | 327 275 | 56.9 (89 %) | 7.28 |
| 128 | 511 429 | 89.0 (70 %) | 7.51 |
| 256 | 655 346 | 118.4 (46 %) | 8.50 |
| 512 | 519 286 | 93.2 (18 %) | 11.06 |

Read it as the METHOD working, not as a recommended setting: throughput peaks at
256 and regresses at 512, utilisation collapses from 93 % to 18 %, per-request
latency stays flat at ~175 ms throughout — that is total residence time, of which
only ~80 ms is the network, so the remote was never the wall — and
CPU-per-key stays ~7.4 until the ceiling outruns the keyspace and then climbs.
The direct evidence that it is split-starved rather than remote-limited is in
the engine counters: `engine.splits` plateaued at ~4 900 and `engine.steals`
stayed flat at ~300–400 across the whole sweep, while latency never moved. Its
`shape.divergence_depth_histogram` sits entirely in the deepest bucket, which is
consistent with expensive splitting — but so does the other bucket measured
here, so that signal alone does not distinguish them.

**Do not port the peak.** A differently shaped bucket of the same size will have
a different one. Run the sweep on your own bucket, or read the shape block and
in-flight utilisation from a single run and infer from those.

### The merge phase grows with cores, not with object count

On `--sort` runs, `sort.merge_ms ÷ duration_ms` is the merge's share of wall
clock. The listing phase parallelises across cores and the serial merge does
not, so **the same bucket shows a larger merge share on a bigger machine** —
adding cores speeds the listing half and leaves the other half where it was.
Judge the share against the machine, not against the object count, and remember
Amdahl: a serial fraction `f` caps any core increase at `1/f`.

## Where swath is slow (honest limits)

The bucket shapes and workloads where swath does not shine — where throttling,
probe overhead, or an adversarial distribution costs more than the ideal.

**Deep-divergence keyspaces starve the work-stealing engine.** When split points
only become distinguishable far down the key (the run summary's
`shape.divergence_depth_histogram` concentrated in its deepest buckets), every
additional parallel range costs a deep probe. The engine then cannot keep its
concurrency ceiling full, and throughput plateaus well below what the remote
would serve. Measured on `pds-css-archive`: `engine.splits` plateaued at ~4 900
and `engine.steals` stayed flat at ~300–400 no matter how high `--concurrency`
went, in-flight utilisation fell from 93 % to 18 %, and throughput *regressed*
21 % from c=256 to c=512 while per-request latency never moved. Setting a higher
ceiling on such a bucket is actively counterproductive.

**The sorted merge is serial, and its share grows with core count.** `--sort`
adds a single-threaded merge after the listing. Because the listing half
parallelises and the merge half does not, the merge's share of wall clock rises
on bigger machines — the same bucket that spends a modest fraction of its wall
in the merge on a small instance spends much more of it on a large one. A
range-parallel merge exists behind
`-Dswath.sort.merge-parallelism` but is **off by default and unreleased** (see
[`usage.md`](usage.md#parallel-range-merge-off-by-default)).

**Cross-cloud vantage.** All figures here were taken from a different cloud than
the bucket. That inflates the concurrency required per unit of throughput and is
not representative of an in-region client.

> _Host and buckets: see [The machine these figures ran on](#the-machine-these-figures-ran-on)._

## Methodology

The evidence bar for every figure on this page: each number is stamped with the
swath version, the measurement date, the machine it ran on, and the bucket it
listed. Measurements come from publicly available tooling only. We publish no
leaderboard and make no self-favorable comparative framing — the numbers
describe swath, not rivals.

**What the current figures are, precisely.** Every number on this page outside
the 0.2.0 scheduling sections is a *field observation*, not a release-candidate
measurement. Specifically:

- **n = 1 per point.** No repeats, so no variance is reported and small
  differences between adjacent points are not significant.
- **One machine, one vantage** — the arm64 host in
  [The machine these figures ran on](#the-machine-these-figures-ran-on), listing
  AWS S3 cross-cloud. Per-request latency was ~172 ms and flat, only ~80 ms of it
  network; an in-region client needs roughly a third less concurrency for the same
  throughput (not less than that — S3's own page-production time dominates and does
  not move), and an x86 host may differ on the CPU-bound phases.
- **Two public buckets**, differing in shape as well as size, so no size-scaling
  law is inferable from them.
- **Serial arms.** Concurrency points were run one at a time, because concurrent
  arms against the same bucket contend remotely and depress exactly the in-flight
  and freeze counters under test.
- Runs were driven by the shipped CLI and read from the run summary
  (`--report`), so every figure is reproducible from a run's own artifacts.

**What would raise these to RC grade:** repeats with reported variance, an
in-region vantage, more than two keyspace shapes, and a like-for-like object-count
sweep at fixed shape. Until then, treat the numbers as illustrations of the
diagnostic method rather than as swath's performance envelope.
