# swath performance

This page covers scaling, memory, resume cost, throughput, the sorted merge, and
known slow paths.

Every number here is a **field observation**: a single-machine, single-vantage run
against a public bucket, stamped with version, date, machine and bucket per the
[Methodology](#methodology) bar. None is a release-candidate measurement — an RC
bundle (multiple machines, repeated runs, in-region vantage) is still outstanding,
and where a section has no data at all it says so.

They are reported because they beat an empty placeholder, and because the *method*
for reproducing them on your own bucket is written alongside — see
[Diagnosing a run](#diagnosing-a-run), which also explains why absolute figures
should not be ported between buckets.

For a head-to-head against other S3 listing tools, see the
[S3-listing comparison study](https://github.com/varveio/s3-listing-study). Its
comparative runs have not started, so it carries a
[methodology](https://github.com/varveio/s3-listing-study/blob/main/docs/methodology.md)
and a tool roster but no results yet.

## The machine these figures ran on

Every field observation on this page comes from one host. Stated in full,
because several of its properties materially shape the numbers:

| | |
|---|---|
| Instance | GCP `c4a-highcpu-32`, zone `us-east1-b` |
| CPU | **arm64** — Google Axion, ARM Neoverse-V2, 32 vCPU |
| SMT | none (1 thread per core, 32 physical cores) |
| Memory | 62 GiB |
| Disk | 200 GB GCP Hyperdisk Balanced NVMe persistent disk, ext4 root; staging and output both landed here |
| Kernel | Linux 6.17 (`aarch64`) |
| JDK | Temurin 25.0.3+9 LTS |
| swath | 0.2.2-dev |
| Date | 2026-08-07–08 |

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
- **Storage caveat.** `--sort` staging and final output shared one Hyperdisk
  Balanced network-attached persistent disk, not a local SSD. Absolute merge
  times are storage-sensitive; sequential comparison arms used the same idle
  filesystem, so their ratios remain useful, but should not be ported to a
  different storage class.
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

The highest directly observed `keys_per_sec` value in the five-arm, `--no-sort`
`pds-css-archive` concurrency sweep was **655,346 keys/s** (≈ 666 LIST/s) at
`--concurrency 256`. It is an n = 1 whole-session mean and only the peak **in that
sweep** — not swath's global throughput ceiling. The full sweep, and why 512 is
*slower* than 256 on that bucket, is in
[Diagnosing a run](#field-observations-stamped-and-shape-specific).

The second, ten-times-smaller bucket reached ~426 000 keys/s in its listing phase
at the same concurrency — **lower**, not higher, and not a like-for-like
comparison: its whole listing lasted 23 s (so ramp-up is a large fraction of it)
and it produced only 148 splits against the larger bucket's ~4 900, so there was
far less parallel work to spread. It is reported here only to show that keys/s
does not follow object count.

**Throughput is shape-bound, not size-bound.** The limiting factor visible in the
`pds-css-archive` sweep was the engine's ability to manufacture splittable ranges
on that deep-divergence keyspace, not the remote and not CPU — see
[Where swath is slow](#where-swath-is-slow-honest-limits).

> _Host, buckets, and the cross-cloud latency breakdown: see
> [The machine these figures ran on](#the-machine-these-figures-ran-on)._

<a id="diagnosing-a-run"></a>

## Diagnosing a run — and why these numbers do not transfer between buckets

This section is **method, not a claim**. The figures in it are stamped field
observations from a single dev machine (see the stamp), included because the
*shape* of the reasoning transfers even where the numbers do not. Nothing here
is a release-candidate measurement.

### The one number to look at first: in-flight utilisation

`--concurrency N` sets a **ceiling** (`Tmax`), not an achieved rate. What a run
actually sustains is `engine.avg_in_flight`, and the ratio that matters is:

```text
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
WHOLE run, and in-flight is zero for the entire post-listing merge/publish tail,
so a sorted run's figure is diluted. Since 0.2.4 the summary carries
`listing_duration_ms` — the listing-only clock — directly, so rescale with
`avg_in_flight × duration_ms / listing_duration_ms`. Against a summary written
by an older build that lacks the field, fall back to
`avg_in_flight × duration_ms / (duration_ms − sort.merge_ms)`. The same dilution
hits `keys_per_sec` and `cpu_efficiency` on a sorted run — both divide by the
whole-run `duration_ms`, merge included — so the honest listing-phase rate is
`(objects − recovered_objects) ÷ (listing_duration_ms / 1000)`, not
`efficiency.keys_per_sec`: the `/1000` matters (`listing_duration_ms` is
milliseconds, and dividing by it directly gives keys per MILLISECOND, 1000×
low), and so does excluding `recovered_objects` (the summary's own top-level
resume-backfill count — a `--sort --resume` that recovered rows would otherwise
have its listing rate overstated by the whole backfill, which this process
never listed). See
[metrics-and-observability.md](metrics-and-observability.md#2-list_run_summary-one-line-at-run-end)
for both fields. An unsorted run needs no correction to the DENOMINATOR:
`listing_duration_ms` equals `duration_ms`.

### Is the wall the remote, or you?

Derive per-request latency across a concurrency sweep:

```text
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

Read `latency_freezes`/`growth_freezes` as a **rate**, not a raw count:
`recovered_errors.freeze_gate_checks` is the number of page successes that
actually reached the growth-freeze gates (a success at `Tmax`, or inside a
throttle cool-down, returns before ever checking, and so can never freeze). A
healthy saturated run legitimately reads `latency_freezes: 0` by design, and raw
freeze counts are not comparable across runs sitting at different saturation —
compare `latency_freezes / freeze_gate_checks` (and `growth_freezes /
freeze_gate_checks`) instead.

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

```text
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

### Reading the merge's share of the wall

On `--sort` runs, `sort.merge_ms ÷ session_duration_ms` is the merge's share of
full-session wall clock. `duration_ms` is not the right denominator for that
FULL-SESSION share — it excludes seeding — and, despite reading as though it
were listing-only, it was never that either: it runs through the merge tail on
a sorted run just like `session_duration_ms` does, so `sort.merge_ms ÷
duration_ms` is a legitimate merge share of the post-seed run, seeding excluded
(`listing_duration_ms`, since 0.2.4, is the field to reach for instead if what
you actually want is the listing phase's own span, seeding AND merge excluded).
The listing phase parallelises across cores, and eligible large runs now
use the core-derived parallel merge by default. The explicit
`-Dswath.sort.merge-parallelism=1` baseline — or a run forced onto the serial path
by its staged-size/resource gates — still leaves the merge unchanged as listing
gets faster, so **the same bucket shows a larger serial-merge share on a bigger
machine**. Judge the share against the machine, not against the object count, and
remember Amdahl: a serial fraction `f` caps any core increase at `1/f`. The
default, its clamps, and the measured comparison are in
[The sorted merge](#the-sorted-merge).

## The sorted merge

For staged input of at least 256 MiB, the final sorted merge defaults to
`max(1, min(8, availableProcessors / 2))` contiguous ranges. Runtime clamps can
reduce that ceiling against the configured `swath.sort.fan-in`, per-stream heap
budget, staged-segment count, and descriptors needed by the input streams plus
one initial output part per range; later rolled parts are hard-bounded against
the remaining descriptor allowance. A result below 2 uses the serial path. Set
`-Dswath.sort.merge-parallelism=1` for an explicit serial run.
See [configuration](configuration.md#jvm-system-properties) and the
[parallel-range merge guide](usage.md#parallel-range-merge) for the knobs and
decline reasons.

The final PR #99 field campaign ran sequential **serial A → shipped default → serial B** brackets
at fixed `-Xmx12g` and `--concurrency 256`. The serial columns below are the bracket means; session
ratio is default session wall divided by that serial mean, so lower is better.

| bucket | objects | segments | serial merge mean | default merge (`R=8`) | merge speedup | serial session mean | default session | session ratio |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `noaa-gefs-retrospective` | 9,915,173 | 2 | 15.1 s | 5.4 s | 2.81× | 26.0 s | 16.9 s | 0.649 |
| `pds-css-archive` | 96,022,559 | 16 | 139.5 s | 38.4 s | 3.63× | 300.5 s | 194.9 s | 0.649 |
| `noaa-mrms-pds` | 823.70–823.72 M | 128 | 1123.6 s | 282.8 s | 3.97× | 1564.6 s | 723.2 s | 0.462 |

These observations were recorded on 2026-08-08 at tested SHA
`2bd24c2f33df35341a497a91e24e7633a224b941`, on the 32-core arm64 host above. The host used its
Hyperdisk Balanced network persistent disk rather than local SSD; every bracket arm used that same
idle filesystem, so the ratios are controlled for storage within each bucket but the absolute merge
times carry that caveat.

PDS was the focused correctness gate: the default had zero bidirectional full-row `EXCEPT ALL`
mismatches against serial A, zero descending physical-key transitions, eight effective ranges, and
no clamp, cascade, or failure reason. Peak heap was 3.598 GiB of `-Xmx12g` (about 30%); its
serial-relative delta is descriptive, not a release criterion. RSS also passed the documented gate.
GEFS and MRMS are corroborating scale observations. MRMS was mutable across arms, and physical
ordering of its default output was not evaluated, so the MRMS row is not additional focused-gate
correctness evidence.

Do not mix the rates represented by this campaign. MRMS's merge-object rate was 2,912,876 objects/s
for the default merge versus a 733,123 objects/s serial-bracket rate; that is objects processed per
merge wall, not listing throughput. The separate `823.7 M / 723.2 s ≈ 1.14 million objects/s` figure
is a **derived full-session object/wall quotient**, spanning listing plus merge. Neither value is the
run summary's `keys_per_sec` (itself a whole-run figure on a sorted run, merge included — see
[metrics-and-observability.md](metrics-and-observability.md#2-list_run_summary-one-line-at-run-end)),
and neither supersedes the directly observed 655,346
keys/s unsorted sweep above. No exact 1.5 million keys/s result was measured.

## Where swath is slow (honest limits)

The bucket shapes and workloads where swath does not shine — where throttling,
probe overhead, or an adversarial distribution costs more than the ideal.

**Deep-divergence keyspaces starve the work-stealing engine.** When split points
only become distinguishable far down the key (the run summary's
`shape.divergence_depth_histogram` concentrated in its deepest buckets), every
additional parallel range costs a deep probe. The engine then cannot keep its
concurrency ceiling full, throughput plateaus well below what the remote would
serve, and **raising `--concurrency` makes it worse** — the measured sweep is in
[Field observations](#field-observations-stamped-and-shape-specific).

**The sorted merge still has serial fractions and serial fallback paths.** Its
boundary-sampling prologue is serial; runs below the 256 MiB staged floor, runs
whose resource clamp lands below two ranges, genuinely unsplittable keyspaces,
and explicit `-Dswath.sort.merge-parallelism=1` runs all use the serial merge.
Eligible large runs use core-derived range parallelism by default — see
[The sorted merge](#the-sorted-merge).

> _Host, buckets, and the cross-cloud caveat: see
> [The machine these figures ran on](#the-machine-these-figures-ran-on)._

## Methodology

The evidence bar for every figure on this page: each number is stamped with the
swath version, the measurement date, the machine it ran on, and the bucket it
listed. Measurements come from publicly available tooling only. We publish no
leaderboard and make no self-favorable comparative framing — the numbers
describe swath, not rivals.

**What the current figures are, precisely** — field observations, not
release-candidate measurements:

- **Mostly n = 1 per point.** The final parallel-merge comparisons bracket one
  default arm with serial A and serial B and report the serial mean; the earlier
  concurrency points remain single observations. Small differences between
  adjacent points are not significant.
- **One machine, one vantage** — the arm64 host in
  [The machine these figures ran on](#the-machine-these-figures-ran-on), listing
  AWS S3 cross-cloud. An x86 host may differ on the CPU-bound phases.
- **Serial arms.** Points were run one at a time. Concurrent arms against the
  same bucket contend remotely and depress exactly the in-flight and freeze
  counters under test — and a benchmark process left running from an earlier
  sweep does the same thing to CPU, so check the box is quiet before trusting a
  number.
- **Live buckets mutate.** Two arms of the same A/B can legitimately return
  different object counts on an actively-written bucket, which makes a
  cross-arm key-set digest meaningless there. Verify identity only on buckets
  whose counts match exactly.
- Runs were driven by the shipped CLI and read from the run summary
  (`--report`), so every figure is reproducible from a run's own artifacts.

**What would raise these to RC grade:** repeats with reported variance, an
in-region vantage, and a like-for-like object-count sweep at fixed shape. Until
then, treat the numbers as illustrations of the diagnostic method rather than as
swath's performance envelope.
