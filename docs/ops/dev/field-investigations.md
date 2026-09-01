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
  --tests 'io.varve.swath.sort.finalize.MergeCpuProfileHarness' -Pperf
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
alternating order with a fresh compression context per operation, matching `PageCompression` lifecycle.

Checksum-off took 1.687868 seconds and checksum-on took 1.678835 seconds (0.9946×; a −0.45 µs/page
difference, within noise). Stored output grew from 3,487 to 3,491 bytes per page: the expected four
checksum bytes. A separate 5,000-page full pack with the checksum enabled took 250.4 µs/page on the
same host, so the measured checksum delta was below 0.2% of pack time. This is a targeted adoption
check, not a general ZSTD throughput claim. Page-run v4 therefore enables the checksum and pins its
verification with a checksum-only corruption test; the outer CRC32C continues to cover the plain
PageBlock header and the `NONE`/`LZ4` payload cases.

---

## 2026-09-01 — replay delimiter page reseek on AWS Public Blockchain

This local isolation check diagnosed and then re-ran the sorted-Parquet replay server's native
`delimiter=/` path. A common-prefix successor that remained in one physical row group kept a
forward key cursor, so `advanceTo` decoded every key in the subtree it was meant to skip. The change
reopens through the Parquet page index when the successor is beyond the cursor's current data page;
same-page successors keep the cursor to avoid repeated page decodes for dense small directories.

### Fixture and command

- **Baseline:** `11d49b02d25572074e9ed65102e4e9627c52e0cf`.
- **Candidate:** PR #194 head after the page-aware hybrid (exact commit recorded by the PR).
- **Host:** 4 physical / 8 logical CPUs, 15 GiB RAM; OpenJDK 25.0.4+7, `-Xmx4g`.
- **Fixture:** four sorted Parquet parts, 143,008,674 rows, 2.96 GiB, manifest SHA-256
  `f4a839f23a1be94047491339dfd1ae5dd40d075c5a7c2eac1e07156dd985d4b0`.
- **Cache state:** filesystem cache was warm from fixture inspection. “Cold” below means a fresh
  server and fresh reader pools, not cold storage.
- **Injection:** off. Prefetch: off. Both arms used eight Parquet readers and at most eight benchmark
  requests in flight; `max-concurrent-requests=64` left the store's eight-reader pool as admission.

From each arm's worktree, after `./gradlew :swath-replay:installDist`:

```bash
/home/sagi_varve_io/.jdks/jdk-25.0.4+7/bin/java \
  -Xmx4g -Dswath.replay.prefetch.enabled=false \
  -cp 'swath-replay/build/install/swath-replay/lib/*' \
  io.varve.swath.replay.server.ReplayServerApp serve \
  --fixture /home/sagi_varve_io/work/replay-fixtures/aws-public-blockchain-current-eba31d9 \
  --bucket aws-public-blockchain --host 127.0.0.1 --port 19091 --metrics-port 19193 \
  --serving-mode sorted --parquet-connections 8 --max-concurrent-requests 64
```

The client first issued one cold `ledgers/pubnet` request. It then obtained a real continuation
token from a `max-keys=4` request. For every table row it sent eight concurrent warmups, followed by
16 measured requests in two eight-request waves. Percentiles are nearest-rank over those 16 values.
Every request used `list-type=2&delimiter=/`; ordinary rows used `max-keys=32`, while the continuation
row used the captured token and `max-keys=4`.

### Results

| Shape / prefix | baseline mean / p99 ms | candidate mean / p99 ms | bytes | response SHA-256 |
| --- | ---: | ---: | ---: | --- |
| root | 28.808 / 48.320 | 6.237 / 8.880 | 1,300 | `5d6d6014ccad75fa2ee06876a326860a7a4d63c89f660ef8616de97bd4f829f6` |
| `v1.0/` | 24.086 / 33.813 | 11.334 / 17.953 | 679 | `56b5595f3bcd53591c81e2f0998b94d29428775f34612d6a9432cf55888f0abd` |
| `v1.0/btc/` | 12.716 / 25.208 | 5.128 / 9.315 | 420 | `342ad23796c5d54b3d5ec533097327ce4c9aed55caecc27f84c3889c6476e826` |
| `v1.0/btc/blocks/` | 3.068 / 5.972 | 10.250 / 18.375 | 3,007 | `1286b23bd5c9efb652e4dd3f9e35ed4126cedff60c953a6a13c0ee3ce7c443bb` |
| `v1.0/btc/blocks/date=2009-01-03/` (leaf) | 6.182 / 9.254 | 8.430 / 13.134 | 685 | `662f9d9b0ac2db36622faa14c74a0d900037876ebfa1b601d737f02437a9f5ca` |
| `v1.1/` | 7.804 / 12.911 | 8.606 / 15.039 | 866 | `e1116fbb496b7baf9c215f7b47ceae1151b303685ad935dad9e709c59aef2e92` |
| `v1.1/stellar/` | 2.189 / 2.894 | 3.468 / 5.715 | 428 | `982e4232ead8dd94598459209a8828d7664ae55c4ae9f2f3d0782e44cb994736` |
| `v1.1/stellar/ledgers/` | 3.775 / 6.751 | 3.364 / 5.906 | 451 | `4ea55e5513c9117741be7fd7a7d7460f889bb8582d3dd7dabd1ef8183b653322` |
| `v1.1/stellar/ledgers/pubnet/` | 428.979 / 537.214 | 19.724 / 32.684 | 4,033 | `1c5130167bbb9c4fab62b9ec717b1f4a49b86c9dcf6aae03fb0fc2ff14a2af73` |
| same, continuation after page 1 | 63.555 / 79.441 | 6.975 / 11.778 | 967 | `745efc985c20d8502f1178de0221a7536cdab90ba1f7bc508dd30f9f22ab33a5` |
| `v1.1/stellar/ledgers/testnet/` | 7.760 / 12.200 | 6.611 / 10.739 | 972 | `30d330a535d28efc839876fe4f7eecfad287cb0ef75bccae5b7b274e3a0202c9` |
| `v1.1/stellar/parquet/pubnet/` | 3.211 / 5.124 | 4.079 / 7.268 | 382 | `9c57526d5bb8dee3065ca61245bff947fb1793bbd908ae1aa0ab096d77ccba79` |
| `v1.1/stellar/parquet/testnet/` | 2.270 / 3.007 | 4.123 / 7.436 | 384 | `8aabe29f50c37e0ee66eca7cf41ce768892f0c39735b55e4c682c07eb19e2d4d` |
| `v1.1/stellar/parquet/pubnet/v1/` | 102.525 / 113.010 | 14.425 / 23.732 | 3,522 | `a9d0087b7c591e128122f98b593bc50eff0263fe1af62bac905fe5bf1ba91299` |

The byte count and SHA-256 in each row were identical between baseline and candidate.

The cold `ledgers/pubnet` request moved from 694.677 ms to 425.623 ms with the same 4,033-byte
response and hash. Its remaining cost includes synchronous opening of eight delimiter readers plus
first-use page/index warming; this change does not redesign that pool. Across the measured candidate
process, 338 delimiter rollups decoded 1,275,506 key rows and performed 1,659 actual page-index
reseeks; peak acquired readers was eight.

The three high-subtree shapes improved 7.1–21.8× in mean latency, and the pubnet continuation
improved 9.1×. Several already-cheap shapes moved by 1–7 ms in either direction under concurrent
local scheduling, including a 7.2 ms increase for `btc/blocks`; this R=1 corpus is not evidence of a
small-shape regression or improvement. Unit coverage therefore carries the dense-small-directory
claim directly: one large row group with 120 singleton prefixes on a shared page is decoded once
with zero replacement-cursor reseeks. This is a local implementation-path check, not a production
S3 latency model or a portable throughput claim.

## 2026-09-01 — replay `delimiter=/` structure probes under concurrent burst load

A follow-up to the entry above, asking why the same probes that cost 3–20 ms in isolation cost a
114 ms mean in a campaign. It measured first and changed nothing: the measurement did not reproduce
the hypothesised cause on this host, and the candidate it motivated regressed the phase it was
supposed to fix, so no serving change was made.

### Fixture, host, and command

- **Arm A (baseline):** `origin/main` `6835bd5` — includes PR #194.
- **Arm B (candidate, measured then discarded):** the lazy delimiter reader pool grown one reader
  per unsatisfied borrow, opened off the pool monitor, instead of opening the whole
  `--parquet-connections`-wide fleet inside the first delimiter request for a file.
- **Host:** 8 logical CPUs, 15 GiB RAM; OpenJDK 25.0.4+7; `-Xmx4g`. The load driver ran on the same
  host, so client CPU competes with the server — see the limits below.
- **Fixture:** four sorted Parquet parts, 143,008,674 rows, 2.96 GiB, manifest SHA-256
  `f4a839f23a1be94047491339dfd1ae5dd40d075c5a7c2eac1e07156dd985d4b0`.
- **Cache state:** filesystem cache warm from earlier runs. "Cold" means a fresh server and fresh
  reader pools, not cold storage.
- **Injection:** off. Prefetch: off. `--serving-mode sorted --parquet-connections 32
  --max-concurrent-requests 512`, `-Dswath.replay.slow-request-log-ms=0` so every request logs its
  own pre-injection server cost.

```bash
/home/sagi_varve_io/.jdks/jdk-25.0.4+7/bin/java -Xmx4g \
  -Dswath.replay.prefetch.enabled=false -Dswath.replay.slow-request-log-ms=0 \
  -cp '<arm>/swath-replay/build/install/swath-replay/lib/*' \
  io.varve.swath.replay.server.ReplayServerApp serve \
  --fixture /home/sagi_varve_io/work/replay-fixtures/aws-public-blockchain-current-eba31d9 \
  --bucket aws-public-blockchain --host 127.0.0.1 --port 19091 --metrics-port 19193 \
  --serving-mode sorted --parquet-connections 32 --max-concurrent-requests 512
```

### Request set and driver

A throwaway Python driver (not committed) derived the request set once by a breadth-first
`delimiter=/` walk of the fixture from the root to depth 4, capped at 600 prefixes: **601 requests**
(1 root, 2 at depth 1, 7 at depth 2, 37 at depth 3, 554 at depth 4), including
`v1.1/stellar/ledgers/`, `v1.1/stellar/parquet/`, `v1.0/btc/`, `v1.0/btc/blocks/` and its `date=`
leaves. Every request was `GET /aws-public-blockchain?list-type=2&delimiter=%2F&max-keys=1000&prefix=<p>`.
Each phase fired the whole set as a burst at a fixed number of connections in flight, recording each
response body's SHA-256. The page-load phases first started 200 background `max-keys=1000`
continuation walkers from 40 anchors spread across the keyspace. Percentiles below are over the 601
`STRUCTURE_PROBE` slow-request lines in each phase's window — that is the pre-injection server cost,
the same quantity `swath.replay.request.latency{shape=structure_probe}` records.

### Result: arms alternated, two repetitions, `--parquet-connections 32`

Structure-probe server cost, p50 / p99 / mean ms. Bursts run back to back on one server, in order.

| Phase | base r1 | base r2 | cand r1 | cand r2 |
| --- | ---: | ---: | ---: | ---: |
| cold, 256 in flight (1st burst) | 316 / 1077 / 558 | 303 / 1053 / 546 | 568 / 1721 / 656 | 541 / 1649 / 637 |
| warm, 256 (2nd burst) | 1.3 / 227 / 10.0 | 77.5 / 182 / 69.4 | 1.4 / 149 / 7.6 | 1.4 / 151 / 8.1 |
| warm, 256 (3rd burst) | 1.3 / 160 / 8.0 | 1.3 / 176 / 7.5 | 47.5 / 103 / 46.5 | 1.3 / 86 / 6.2 |
| warm, 256 (4th burst) | 1.3 / 89 / 6.7 | 1.2 / 135 / 6.5 | 1.2 / 115 / 6.3 | 1.2 / 66 / 4.4 |
| warm, 256 (5th burst) | 1.2 / 65 / 4.7 | 1.2 / 173 / 7.5 | 1.3 / 57 / 4.1 | 1.2 / 58 / 3.7 |
| warm, 64 in flight | 1.1 / 57 / 3.8 | 1.1 / 44 / 3.5 | 1.2 / 51 / 3.8 | 1.2 / 49 / 3.7 |
| warm, 256 + 200 page walkers | 164 / 226 / 168 | 138 / 182 / 141 | 123 / 169 / 124 | 157 / 220 / 158 |
| repeat | 1.3 / 71 / 5.8 | 139 / 172 / 139 | 173 / 217 / 170 | 81 / 174 / 80 |
| repeat | 1.3 / 41 / 4.8 | 1.3 / 60 / 4.5 | 137 / 204 / 141 | 130 / 159 / 131 |

Reader-pool meters over the cold burst:

| Meter | base r1 / r2 | cand r1 / r2 |
| --- | --- | --- |
| `delimiter.reader_pool.open.latency` count | 4 / 4 (one per file) | 48 / 45 (one per reader) |
| …sum ms | 1,648 / 1,586 | 3,593 / 4,282 |
| …max ms | 654 / 595 | 198 / 243 |
| `delimiter.reader_pool.readers_opened` max | 32 / 32 (fleet width) | 27 / 27 (widest pool grew) |

An earlier uncontrolled sweep at `--parquet-connections 8` put the same four fleet opens at 510 ms
total (mean 127 ms), with a cold-burst structure-probe p50 of 218 ms.

13,222 response bodies were hashed across 23 phase captures on both arms; **zero mismatches**.

### Attribution

1. **The burst is throughput-bound on this host, not lock-bound.** Comparing the store's own timer
   with the request timer over the same phase: at 8 in flight `request.latency` mean 2.1 ms versus
   `parquet.query.latency` mean 2.1 ms (no queueing); at 64 in flight, 29.3 ms versus 3.2 ms; at 256
   in flight, 120 ms versus 4.1 ms. The store work per probe barely moves; what grows is the wait for
   one of 8 CPUs. 601 probes at roughly 25 ms of cold CPU each is about 15 core-seconds on an
   8-core box, which is the ~2 s cold burst and the ~550 ms mean latency it implies.
2. **Most of the "warm versus campaign" gap is JIT warm-up of the delimiter path, not I/O.** With
   nothing changed between them, the same 601-probe burst repeated on one server fell from
   10.0 → 8.0 → 6.7 → 4.7 ms mean and 227 → 160 → 89 → 65 ms p99. A campaign issues only a few
   hundred structure probes in total against millions of worker pages, so this path never leaves the
   interpreter/C1 there. Isolation measurements that warm up first cannot see this.
3. **The lazy fleet open is real but second-order here.** It is 1.6 s of the cold burst's ~15
   core-seconds at `--parquet-connections 32`. Hypothesis (a) predicted it would dominate; it does
   not, on this host.
4. **Growing the pool incrementally made the cold burst worse** (p50 316 → 568 ms, p99 1077 →
   1721 ms, both repetitions). Serialising four fleet opens under a monitor costs 1.6 s of
   thread-time; letting ~45 opens race on a saturated 8-core box costs 3.6–4.3 s, and every probe
   pays for the contention. The candidate was therefore discarded, not committed.
5. **The page-load phases are host-saturation, not a delimiter signal.** 200 walkers plus 256 probes
   plus the co-located driver oversubscribe 8 cores; the phase is bimodal across repetitions on both
   arms (1.3 ms p50 in one repetition, 140 ms in the next) with no separation between the arms. The
   campaign ran 64 vCPUs at ~48 busy, which is not this regime.
6. **Per-probe work is small and unchanged:** `delimiter.skipscan.decoded_key_rows` mean 494 rows per
   probe (max 10,201), `page_reseeks` mean 0.23, `row_group_opens` 757 per 601 probes,
   `whole_group_shortcuts` 0 on this fixture.

### Limits

- The driver is co-located with the server on the same 8 cores, so every 256-in-flight number
  includes client CPU. Only same-host arm-versus-arm comparisons are meaningful.
- The acceptance target this investigation was given (p50 ≤ 27 ms, p99 ≤ 55 ms at 256 in flight)
  is already met by the baseline once the JVM is warm (p50 1.2 ms, p99 57–65 ms by the fifth burst,
  still falling), and is unreachable in the cold burst on 8 cores at any lock granularity: 256 in
  flight against 8 cores needs a sub-millisecond service time to hold a 27 ms p50.
- Nothing here tests the campaign's regime — 64 vCPUs, `--parquet-connections 128`, an unsaturated
  server. Four serialized 128-reader fleet opens extrapolate to roughly 6 s of blocking there, and
  that remains the one untested reason the campaign's structure probes could be slow. Testing it
  needs a host with that core count, not this one.
- Hypothesis (b), sharing the immutable per-row-group column and offset indexes across pooled
  readers, was not attempted: `ParquetFileReader.readFilteredRowGroup` resolves offset indexes
  through its own per-reader `getColumnIndexStore`, so sharing them would need an upstream seam
  rather than a cache in `SortedParquetRowGroupReader`.
