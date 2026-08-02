# swath performance

This page defines the measurements required before swath makes quantitative
performance or scale claims: scaling, memory, resume cost, throughput, and
known slow paths. There is no published release-candidate measurement bundle
yet, so the sections below are test plans and design targets, not results.

For a head-to-head against other S3 listing tools, see the
[S3-listing comparison study](https://github.com/varveio/s3-listing-study),
which is built for exactly that and commits to what it measures before it
measures it. Its comparative runs have not started yet, so it carries a
[methodology](https://github.com/varveio/s3-listing-study/blob/main/docs/methodology.md)
and a tool roster but no results so far.

With one exception, every figure below is pending a release-candidate measurement
pass and the numbers are not filled in yet. The exception is
[The 0.2.0 scheduling defaults](#the-020-scheduling-defaults-rate_anchored_sensing--tail_floor),
which reports completed measurements and states its own limits.

## Scaling behavior

How listing time and LIST-call count grow as object count rises across bucket
shapes (deep prefix trees, flat key spaces, skewed distributions).

> _Numbers pending RC measurement._

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

> _Numbers pending RC measurement._

## Resume cost

The overhead of resuming an interrupted run: time to reopen the checkpoint and
the LIST calls a resume spends versus a run that completes in one pass.

> _Numbers pending RC measurement._

## Throughput

Sustained keys listed per second, and the LIST-request rate that throughput
implies, under representative concurrency settings.

> _Numbers pending RC measurement._

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

## Where swath is slow (honest limits)

The bucket shapes and workloads where swath does not shine — where throttling,
probe overhead, or an adversarial distribution costs more than the ideal.

> _Numbers pending RC measurement._

## Methodology

The evidence bar for every figure on this page: each number is stamped with the
swath version, the measurement date, the machine it ran on, and the bucket it
listed. Measurements come from publicly available tooling only. We publish no
leaderboard and make no self-favorable comparative framing — the numbers
describe swath, not rivals.

> _Numbers pending RC measurement._
