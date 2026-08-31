# swath — field investigations (contributor reference)

> **You don't need this to use swath.** This is where investigations against *specific real buckets*
> are written up: the raw evidence, the bucket, the numbers, and what changed as a result. Design
> docs and code comments stay bucket-neutral and cite an entry here rather than naming a bucket
> inline — a tuning decision should be legible from the mechanism, with the field data one link away.

Add an entry when a real-bucket run drives a code change. Keep the bucket name, the flags, and the
before/after numbers: the point of this file is that someone can re-run it.

---

## 2026-07-25 — probe-timeout storm on a deep genomics bucket

**Bucket:** `s3://genomeark/` (public, `us-east-1`, ~8.8 M objects, `mass_skew_gini` 0.81, very deep
`working/…` tree with several enormous flat directories).

**Command:**

```bash
swath list s3://genomeark/ --region us-east-1 --no-sign-request --format parquet -o <dir>
```

### Symptom

Throughput decayed ~20× over the run (147 k → 7.3 k keys/s), `in_flight` collapsed from 18.5 to 2.0
against a target of 64, and the log filled with ~1300 `ApiCallAttemptTimeoutException` warnings. The
run had listed 6.6 M objects in 280 s and was still going when it was interrupted.

### Diagnosis

Every one of the 1308 attempt timeouts was a **probe**, and within probes, only the
`delimiter=/` structure class. The per-call-class histograms were unambiguous — same client, same 3 s
budget, same concurrency:

| class | calls | timeouts |
|---|---|---|
| `pivot_probe` | 3169 | **0** |
| `structure_probe` | 2612 | **~1308** |
| `worker_page` | 6819 | **0** |

Measured directly against the bucket with the AWS CLI, one structure probe on that keyspace costs
~1.15 s standalone, ~1.68 s at 16-way and ~5.4 s at 64-way — i.e. the shared 3 s point-probe budget
sat *below the call's real cost at the concurrency the run itself was using*. Structure probes are
the thief's pivot source, so the fallout was work starvation rather than merely wasted calls: 55
splits and 39 stolen children for 6.6 M keys, 2.5 % steal success, 31762
`swath.idle_backoff.slot_denied`.

Two aggravating factors, both fixed:

- Each timeout aborted its connection (1308 `connection_aborted`, 1:1) forcing a fresh TLS handshake
  (1392 `handshakes`) — the storm paid for its own reconnection churn.
- The engine's escalation ladder (20 s → 40 s) is authored against the 10 s scan base, so a 3 s probe
  escalated 6.7× straight to 20 s. With `PROBE_TRANSIENT_RETRY_CAP=1`, 556 probes burned 3 s + 20 s
  and returned nothing.

**AIMD correctly did nothing.** Only 4 genuine 5xx occurred in the whole run — S3 was never unhappy.
Attempt timeouts deliberately don't vote `T` down, and *probe* timeouts are excluded from every AIMD
down path (`ConcurrencyGauge#onTransientTimeout(false)` early-returns; the run recorded 1308
`GROWTH/probe_timeout_excluded`). The sustained-timeout shed gates on worker-class timeouts, of which
there were none. This was swath hurting itself in a traffic class AIMD does not govern, so no
concurrency setting would have fixed it — lowering `--concurrency` would only have masked it by
shrinking probe fan-out.

### Changes it drove

- Per-attempt budgets sized by **call-class cost shape** (point vs scan) rather than by "is it a
  probe" — `docs/internals/probe-budgets.md` §1.
- Escalation **re-expressed against each call class's own base** — `probe-budgets.md` §3.
- Retryable fault log lines carry `call_class`/`prefix`/`start_after`, so a storm is attributable
  from the log alone (previously it could only be reconstructed from the JSON summary).
- Run-summary distribution statistics made **run-scoped** — see below, found while investigating.

### Before / after

Cumulative counters are directly comparable. **Percentiles are not**: before the run-scoped-window
fix, `max` and all percentiles were Micrometer's rolling 2-minute window while `count`/`total` were
cumulative, so the "before" percentiles described only that run's final ~2 minutes.

| | before | after |
|---|---|---|
| run | 6.6 M objects in 280 s, interrupted, still going | **8.8 M objects, complete, 173 s** |
| throughput | 23.8 k keys/s | **50.8 k keys/s** |
| attempt timeouts | 1308 | **216** |
| connections aborted / handshakes | 1308 / 1392 | **216 / 441** |
| splits | 55 | **380** |
| `latency_inflation` freezes | 1176 | **32** |

Run-scoped per-class latency after the fix:

| class | calls | p50 | p90 | p99 | max |
|---|---|---|---|---|---|
| `worker_page` | 9229 | 107 ms | 149 ms | 258 ms | 321 ms |
| `pivot_probe` | 15363 | 34.6 ms | 45.1 ms | 74.4 ms | 119 ms |
| `structure_probe` | 4169 | 43.0 ms | 10,200 ms | 15,568 ms | 17,372 ms |

### Residual, and the right fix for it

`structure_probe` keeps a heavy **tail** (p90 10.2 s) and 216 attempt timeouts remain. These
concentrate in a few enormous flat directories — `working/staging/all_vs_all_alignments/FastGA/10k/`
is the reproducible one — where a `delimiter=/` scan must cross a very large number of keys before it
can return a single `CommonPrefix`. Even the 10 s scan budget is not enough there, and the ladder
correctly escalates those to 20 s/40 s. This is inherent to the call shape on that keyspace, not a
budget error: p50 is 43 ms, the run completes, and the thief gets pivots again.

**Fixed** (see "Structure-probe timeout suppression" below). **No budget can fix it, but a feedback gap can.** `Thief#structureProbesEnabled` already suppresses
structure probes per-victim after `STRUCTURE_ZERO_FANOUT_SUPPRESS_THRESHOLD` consecutive zero-fanout
probes — precisely the enormous-flat-directory defense. But a structure probe that **times out** never
returns its zero-fanout answer, so `consecutiveZeroFanoutProbes` never increments and the suppressor
never engages: *the timeout destroys the very evidence that would stop the next probe*. Folding a
structure-probe timeout streak into that same per-victim suppression counter turns the residual from
"pay this cost repeatedly per flat region" into "pay once or twice, then fall back to bisection
pivots". That is per-victim-scoped like the existing futility pacing, trivially instrumentable per the
§5 engagement-counter doctrine, and it generalizes to flat buckets far better than any budget policy —
it stops issuing the wrong probe *shape* instead of re-sizing its deadline. This, not budget tuning
and not the probe-pacing latch, is the right home for the residual.

**Related open follow-up — a leaked pacing latch, not missing pacing.** Probes are `slotGated=false`,
so they bypass the AIMD slot gate, but they are *not* unbounded by design: every thief steal attempt
must pass `IdleStealBackoff#tryAcquireAttemptSlot` (`WorkStealingScan.java:507`), whose stated
contract is **at most one speculative steal attempt in flight fleet-wide**. The bound leaks:
`IdleStealBackoff#reset()` clears `attemptInFlight` unconditionally, and it is called on every
ordinary claim (`WorkStealingScan.java:496`) and every non-empty page commit
(`WorkStealingScan.java:723`) — i.e. by unrelated workers, while an attempt is still running. So
effective probe concurrency is roughly `1 + (reset rate × probe duration)`: with multi-second probes
and tens of commits/sec it reaches tens in flight. The class javadoc concedes "a progress reset can
briefly let a second through"; under this load both "briefly" and "a second" understate it.

The follow-up is therefore to make `IdleStealBackoff` honor its own invariant — e.g. generation-stamp
the in-flight attempt so `reset()` clears backoff state without releasing a genuinely running
attempt's slot — **not** to add a second probe-concurrency controller. Still touches steal pacing, so
it needs its own design + adversarial review. Its acceptance gate should be split rate: strict
one-in-flight pacing risks capping splits near ~0.3–0.5/s when probes are slow, where the healthy run
needed 380 splits in 173 s (~2.2/s). Fixing starvation from the other side would be no better.

(An earlier revision of this note claimed "64 workers can fire ~64 simultaneous probes". That
overstates the mechanism — the design intends one, and the real number is set by the leak rate, not
by worker count. Corrected here so the follow-up targets the leak.)

### Found while investigating

`swath.rate_limit.wait` reported `count=6819, total_ms=143045, max_ms=0.001117` — 21 ms of average
slot wait against a sub-microsecond max. Not a unit bug: Micrometer's default
`DistributionStatisticConfig` is a rolling window (`expiry=2m`, `bufferLength=3`), so `max` and every
published percentile decay while `count`/`totalTime` stay cumulative. Slot contention stopped once
concurrency collapsed, and the rolling max had decayed to nothing by the time the summary was
written. Every `probe_latency[]` percentile and `shape.regime.api_latency_p*` shared the defect on
any run longer than two minutes. Fixed in `RunMetrics#DISTRIBUTION_WINDOW`.

---

## 2026-07-25 — structure-probe timeout suppression (follow-up to the above)

Closed the feedback gap identified above: a structure probe that times out reports nothing, so it
threw past the fan-out accounting and left every per-victim suppression counter untouched — the
timeout destroyed the evidence that would have stopped the next probe. `Thief#probeStructure` now
attributes an `ATTEMPT_TIMEOUT` to the victim (and only that kind — a 503 is store backpressure, not
a statement about the keyspace) before rethrowing, and `structureProbesEnabled` suppresses on a
timeout streak of 2 as well as the existing zero-fan-out streak of 8.

Interleaved A/B against the immediately preceding commit, same bucket, alternating runs:

| round | build | result | keys/s | timeouts | splits | `structure_probe` p90 |
|---|---|---|---|---|---|---|
| 1 (unbounded) | before | **did not finish** in 260 s | 34,669 | 631 | 302 | 10,736 ms |
| 1 (unbounded) | after | **complete in 163 s** | 53,965 | 177 | 204 | 2,683 ms |
| 2 (110 s cap) | before | 4.64 M objects | 46,875 | 301 | 42 | 10,199 ms |
| 2 (110 s cap) | after | 6.56 M objects | 66,750 | 176 | 146 | 5,636 ms |
| 3 (110 s cap) | before | 4.45 M objects | 44,594 | 301 | 42 | 10,199 ms |
| 3 (110 s cap) | after | 5.65 M objects | 58,773 | 204 | 154 | 8,589 ms |

Throughput +32–42 % in every round, timeouts roughly halved, and the structure-probe p90 tail cut by
1.2–4×.

**On split count — read it per unit time.** The unbounded round shows *fewer* absolute splits after
the change (204 vs 302), which looks like a regression and was initially recorded as one. It is an
artifact of comparing runs of different length: the "before" run was still going at 260 s, so it had
far longer to accumulate splits. The equal-duration rounds are the honest comparison, and there
splits went **up 3.5×** (146/154 vs 42/42 in the same 110 s). Suppressing hopeless probes does not
cost parallelism — it buys it, because the thief stops spending its steal attempts on regions that
cannot answer.

---

## 2026-08-24 — sorted replay-server capacity

This investigation sized the standalone replay server before using it as a benchmark dependency.
It is replay capacity evidence, not a comparative Swath result and not a throughput SLA.

### Environment and method

- Host: 32 logical CPUs / 16 physical Intel Xeon Platinum 8581C cores, 62 GiB RAM.
- Server: Temurin JDK 25.0.4+7, pinned to eight physical cores; the byte-counting load client was
  pinned to the other eight physical cores.
- Large fixture: 1,049,162,031 object rows, 32 Parquet files, 27.997 GiB. Its routing index contained
  3,874 row-group entries; deriving it took 2.691 seconds in the startup measurement.
- Each reported capacity point followed a warm-up. The storage page cache was warm, so the results
  characterize serving and decode capacity, not cold-disk bandwidth.
- Worker responses used `max-keys=1000`. The client parsed HTTP framing without allocating a UTF-16
  response String, and server metrics plus process CPU verified that it did not cap the eight-core
  server.

### Request shapes

Capacity pressure covered the three shapes Swath emits:

| Shape | Request | Important server path |
| --- | --- | --- |
| `worker_page` | ordinary listing page, normally 1,000 objects | continuation walk and prefetch |
| `pivot_probe` | delimiter-free request with `max-keys<=1` | random/far key seek, normally no useful prefetch |
| `structure_probe` | `delimiter=/` request | routing bounds plus native row-group skip-scan when needed |

The mixed test used 93% worker pages, 5% far pivots outside the active worker windows, and 2% real
parent-prefix structure probes. This corrected an earlier locality-optimistic test whose pivots were
usually cache hits and whose delimiter probes targeted the empty root prefix.

This is request-*shape* coverage, not a claim that one synthetic ratio represents every Swath run.
Protocol correctness is covered separately and more broadly by the conformance harness and
adversarial tests: prefix, delimiter, start-after, continuation token, truncation, empty results,
URL encoding, owner/no-owner responses, and XML escaping. A future trace-driven load command should
replay a captured Swath arrival stream when an exact production distribution matters.

### Capacity results

All rows below used eight physical server cores, reader pool 8, and zero HTTP errors:

| Fixture/workload | Prefetch | Heap/windows | Result |
| --- | --- | --- | ---: |
| 9,919,142-row fixture, distributed warm random pages | off | 4 GiB / n/a | **6.126M rows/s** |
| 1.049B-row fixture, 16 sequential continuation walks | on | 8 GiB / 24 | **5.517M rows/s** |
| 1.049B-row fixture, corrected 93/5/2 mixed shapes | on | 8 GiB / 24 | **4.153M rows/s**, 4,464 requests/s |

The large-fixture result shows that total cardinality is not itself a throughput cliff. Physical
page geometry and access locality matter: sequential workers reuse prefetched windows, while far
pivots and delimiter probes pay more seek/decode work.

### Heap and reader-pool knees

At fixed eight cores, pool 8, 16 continuation walks, and a 60-second measurement:

| Heap / windows | Sustained rows/s | RSS before forced GC | Post-force-GC used heap |
| --- | ---: | ---: | ---: |
| 2 GiB / 16 | 4.661M | 2.35 GiB | 432 MiB |
| 3 GiB / 24 | 4.852M | 3.35 GiB | 431 MiB |
| 4 GiB / 24 | **5.468M** | 4.31 GiB | 557 MiB |
| 8 GiB / 24 | 5.517M (30-second point) | 6.47 GiB after prior mixed load | 626 MiB |

Four GiB was the practical knee. Two and three GiB retained little live data after forced GC but
lost throughput to allocation/collection pressure. Forcing two-MiB G1 regions at a two-GiB heap
reduced throughput further, so automatic G1 region sizing remains the recommendation.

At fixed four-GiB heap and 24 windows:

| Reader pool | Sustained rows/s | Peak readers | Mean page-read latency |
| ---: | ---: | ---: | ---: |
| 8 | **5.468M** | 8 | 12.18 ms |
| 12 | 5.509M | 12 | 12.06 ms |
| 16 | 5.072M | 16 | 14.29 ms |

Pool 12's 0.75% edge was within single-run noise and required 50% more open readers and decoded
footers. Pool 16 was 7.2% slower and increased GC pressure. This drove the sorted default from two
readers per visible CPU to one, bounded to 8–32.

### Correctness and limits

The optimized and untouched implementations produced byte-identical complete responses over all
9,920 pages and 9,919,142 objects of the smaller fixture, including owner fields: 2,886,109,739
response bytes with SHA-256
`20078b857c86ff394ec9794da39e231507658004bb678058f05443ef75993c1e` on both sides. The full build,
replay integration tests, HAR conformance, sorted-vs-DuckDB differentials, and adversarial XML tests
also passed.

The saturation client used for this investigation was diagnostic and is not a supported release
command: its distributed anchors were fixture-derived and its mixed ratio was synthetic. The
built-in `bench` command remains the reproducible correctness/warm single-walk tool; it does not
saturate an eight-core server. Do not use these figures to infer cold-storage performance, remote
network capacity, or an exact production request distribution.

---

## 2026-08-25 — canonical timestamp arithmetic fast path

An R8 sorted-finalization CPU profile attributed 4,256 of 9,097 merge-thread samples
(46.8%) to timestamp parsing or formatting. Page-run staging already stored epoch microseconds,
but the then-current path rebuilt a text-backed entry and converted the canonical value again for
typed Parquet output.

A byte-exact arithmetic fast path for canonical UTC text retained the general parser for every
alternate accepted spelling. Two matched merges moved from 21.5–21.6 seconds to 14.0 seconds;
whole-run report time moved from 57.6–57.8 seconds to 44.2–44.9 seconds, and peak RSS from
3.27–3.31 GiB to 3.21–3.22 GiB. Both candidate arms passed exact row, manifest, inventory, MD5,
sorted-readback, and replay-error checks. The source text model, page-run encoding, and Parquet
schema did not change. These are dated observations from the topology and corpus used for PR #140,
not a portable speedup claim.

---

## 2026-08-30 — cached timestamps and served column indexes

This investigation checks the two cheap sorted-finalization changes against the retained SOREL
PageRun corpus. It is a warm, local R=1 observation, not a portable throughput claim.

### Evidence and provenance

- **Baseline production code:** `771fecb85db0225e8ba185f4bb1a801a52e234ad`. The profile-only retained-corpus
  harness change was applied to a detached baseline worktree too, so both arms used identical
  corpus materialization, validation, fingerprinting, and output-size reporting.
- **Candidate:** branch `perf/cheap-finalization-wins`, based on that revision, with typed-entry
  timestamp caches, lazy caching for source-text entries, and 1,024-byte served-file column-index
  bounds.
- **Host:** GCP x86_64 VM, 4 physical / 8 logical Intel Xeon Platinum 8581C CPUs, 15 GiB RAM, no
  swap, and a 193 GiB ext4 root disk.
- **Runtime:** OpenJDK 25.0.4+7-LTS test worker, `-Xmx2g`, JFR `profile` settings.
- **Fixture:** checkpoint-authorized retained SOREL staging, 9,919,142 rows in 23 PageRun segments;
  corpus ID `f77e226ed3da9ed8ee4c375f61275d23257091ca9a772da8d7988ed5b75a6728`.
- **Clock:** the R=1 `SortedDatasetCoordinator.transform` window only. Corpus validation and hard-link setup run
  before JFR starts; output validation runs after it stops. Reading the immutable corpus to validate
  it primes the filesystem cache, so these are warm-cache results.

The retained-corpus profile used the opt-in harness:

```bash
JAVA_TOOL_OPTIONS="-Dswath.profile=on \
  -Dswath.profile.jfr=<arm>.jfr \
  -Dswath.bench.staging-dir=<retained-output>/_staging" \
  ./gradlew :swath-core:test \
  --tests 'io.varve.swath.sort.MergeCpuProfileHarness' -Pperf
```

### Timestamp result

JFR execution samples were classified as timestamp parsing when their stack contained
`LastModified.epochMicrosFromText`, `canonicalEpochMicros`, or `parseWireInstant`.

| Arm | R=1 wall | execution samples | timestamp-parse samples | parse share |
| --- | ---: | ---: | ---: | ---: |
| baseline | 17,533 ms | 913 | 26 | 2.85% |
| combined candidate | 17,230 ms | 889 | 0 | 0.00% |

Both arms produced logical fingerprint
`5c2e617ee4f4b3f782a77d894fb012254a373182e2d9f2b32b7f625424bdab53` and multiset digest
`4a59f16404b8660c89b2460831b634095f0c0ac0f39ac57feb314d7273f3d9244c6d017bea94e211cf54c8ca793bba74284f64f17f877116abdc3135db1a32ab`.
The candidate arm includes both timestamp caching and the column-index change, so its wall time does
not isolate either change. Both refreshed arms also fall below the generated harness's approximate
20-second stability floor. The wall values are diagnostic only; the zero parse samples and identical
fingerprints are the timestamp acceptance evidence, not a portable speedup claim. Warm candidate
observations elsewhere in this investigation span 17,230–18,648 ms, so the paired 303 ms wall
difference is not directionally resolved against the observed run-to-run spread.

The classifier is intentionally parse-only. PageRun decode still constructs canonical
`lastModifiedText` through `LastModified.textFromEpochMicros`, so 0.00% parse share does not mean that
all timestamp work disappeared. Avoiding that format half would require lazily materializing model
text or changing the sorted representation and is outside this change.

### Served column-index result

The 1,024-byte truncation length is the maximum general-purpose S3 key length, so a supported key's
page bound remains complete even when many pages share a long prefix. The regression test checks
exact one-page pruning both above Parquet's 64-byte default and at the exact 1,024-byte key limit.

On the retained corpus, the baseline final was 540,516,272 bytes and the candidate final was
540,923,714 bytes: 407,442 bytes (0.0754%) larger. Timestamp caching does not alter encoded output, so
this is the observed physical-size cost of the larger column-index bound on that corpus. Read-side
memory depends on actual key lengths and concurrently cached reader/block indexes; this single
merge-only run does not measure that serving footprint.

### Key and etag dictionary probe

One uncommitted diagnostic arm disabled Parquet dictionary encoding for only `key` and `etag`; the
branch keeps the current policy. Two consecutive dictionary-on/off pairs observed 17,466/17,220 ms
and 18,648/17,511 ms. The second pair, which also recorded output size, wrote 540,923,714 bytes with
the current policy and 540,857,441 bytes with those two dictionaries disabled (66,273 bytes, or
0.012%, smaller). Broad Parquet dictionary/fallback stack samples moved from 160/961 (16.65%) to
70/896 (7.81%) in the first pair, but that classifier includes the low-cardinality columns whose
dictionaries remain useful.

The probe points toward a separate repeated experiment, but two same-order pairs on one warm corpus
are not enough to change the writer policy. No dictionary default changes in this work.

---

## 2026-08-31 — ZSTD page-content checksum cost

Before enabling ZSTD's frame content checksum for page-run v4, a JDK 25 / zstd-jni microcheck used
the raw 44,144-byte front-coded payload produced by packing 1,000 representative object rows. After
3,000 warmups, 20,000 checksum-on and 20,000 checksum-off level-1 compressions were interleaved in
alternating order with a fresh compression context per operation, matching `PageCodec` lifecycle.

Checksum-off took 1.687868 seconds and checksum-on took 1.678835 seconds (0.9946×; a −0.45 µs/page
difference, within noise). Stored output grew from 3,487 to 3,491 bytes per page: the expected four
checksum bytes. A separate 5,000-page full pack with the checksum enabled took 250.4 µs/page on the
same host, so the measured checksum delta was below 0.2% of pack time. This is a targeted adoption
check, not a general ZSTD throughput claim. Page-run v4 therefore enables the checksum and pins its
verification with a checksum-only corruption test; the outer CRC32C continues to cover the plain
PageBlock header and the `NONE`/`LZ4` payload cases.
