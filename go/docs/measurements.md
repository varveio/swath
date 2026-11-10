# Measurements

The measurements the other docs and the pull request cite, each with its configuration and the harness that
reproduces it. Scripts are in [`../bench/`](../bench/); how to build the replay oracle and the fakes is in
[testing.md](testing.md).

**Host.** Unless a section says otherwise: 4 vCPU (Intel Xeon @ 2.10 GHz), 15 GiB RAM, ext4 on a virtio disk,
Linux 6.18 (Firecracker guest), Go 1.26. The host was shared and its speed drifted during sessions (steal time
seen in `vmstat`), so **compare absolute times only within one table**; each table's arms ran interleaved in
one session. Ratios held across sessions.

**Commands as run.** Commands and flags are quoted as the build of the day took them. Since report version 2
and the command-line cleanup ([upgrade.md](upgrade.md#command-line)): `swath list --count-only` is
`swath diag count` (`swath count` from 2026-11-07 to 2026-11-09); `--seed`, `--seed=false`, `--limiter-slow-start`, `--spill-plain` and `--spill-compress`
are `swath diag list` (or `swath diag count`) flags, and the
seed is on by default, so `--seed` alone is the default; `--spill-max` is `--spill-budget`; the report's
`output.rows_listed` is `output.rows_emitted`, `output.bytes` `output.output_bytes`, `cost.api_calls`
`requests.http_attempts`.

**Conventions.** "median (min–max)" is over the stated repetitions. "Synthetic" marks a fake bucket, fixture
or tree; each names its generator. "Replay" is swath-replay serving a recording made by the Java CLI
(`swath list --format parquet --sort`, anonymous, live) with a fixed injected latency on every request shape;
replay answers instantly otherwise, so it measures coordination and concurrency, not S3. "Requests" are HTTP
requests counted by the server (replay, fake or fault proxy). Java is the Java CLI at its defaults
(concurrency 64, seed `shallow`), version 0.3.3, JDK 25.

## Output identity with the Java tool

Every comparison was row-identical on the 13 canonical columns, with identical minimum and maximum keys
(`bench/diffds.py`: row sets both ways, strict ascending key order within and across parts):

| Comparison | Datasets |
|---|---:|
| Live buckets, Go listing directly against the Java recording of the same bucket | 21 |
| Synthetic fixtures served by replay (10k keys; 1,000,020 keys from `bench/gen_fixture_1m.py`), each in 5 Go modes (sequential, 4 and 8 workers, with and without `--seed`) | 2 × 5 |
| Java listing a Go dataset served by replay's sorted mode (1 part and 6 parts); historical: run on format-1 Go output, before format 2, which Java's reader does not read ([upgrade.md](upgrade.md)) | 2 |

The 21 live buckets are noaa-normals-pds, noaa-nws-fourcastnetgfs-pds, real-changesets and 18 open buckets
chosen for the Java policy features they engage (among them noaa-eri-pds and pansurg-curation-raw-open-data),
from 1,018 to 13.9 M objects.

**Size.** Go datasets are 0.4–6.9 % larger than Java's after writing `etag` as `DELTA_BYTE_ARRAY` (1M fixture
1.050 → 1.027 of Java's size; noaa-normals-pds 1.089 → 1.069). The rest is `last_modified` on buckets with few
distinct timestamps: parquet-go cannot switch from dictionary to delta mid-chunk as parquet-mr does
([internals/parquet-output.md](internals/parquet-output.md)).

**Reproduce:** [testing.md, "Replay oracle"](testing.md#replay-oracle), then
`python3 bench/diffds.py JAVA_DATASET GO_DATASET` (`--ts-ms` when one side was listed directly).

## Replayed recordings: Go against Java defaults

**Run `recordings1`, 2026-11-07.** One n4-highcpu-16 VM in GCP us-east1 (150 GB disk). The Java CLI and
swath-replay were built from this repository at `62b224f8` with Temurin 25.0.5. The four recordings were made
again that day by listing the live buckets anonymously with that Java CLI (64 concurrency, sorted Parquet), and
their object counts equal the earlier recordings'. `bench/policy_replay.sh` then served each recording at its
worker-page p50 latency (99, 164, 99 and 103 ms on every request shape). Four arms ran interleaved, 3
repetitions each, all with sorted Parquet output:

- Java at its defaults;
- Go at main `62b224f8`;
- Go at `7b05605`;
- Go at this head, `4ad9eecc`.

The Go arms use 64 workers and `--seed`. The table gives the median, with (min–max) on wall. Requests are the
replay server's HTTP count, and requests per 1k keys come from the medians. `7b05605` is within a few percent
of the head on every column and is left out of the table; the head adds only telemetry.

**Row identity.** For each Go build and recording, one more run without injected latency wrote a dataset.
`bench/diffds.py` compared it with the Java recording, and all 12 were row-identical.

| Recording (objects, latency) | Arm | Wall s | Requests | Requests per 1k keys | Splits | Empty children | Overfetched keys | Structure probes | Mean active |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| noaa-eri-pds (845,693, 99 ms) | Java | 42.0 (42.0–42.5) | 1,356 | 1.60 | 94 | 100 | 148,904 | 355 | 7.6 |
| | Go main | 3.6 (3.4–3.6) | 1,234 | 1.46 | 475 | 154 | 384,597 | 0 | 35.8 |
| | Go head | **3.1** (3.1–3.1) | 1,389 | 1.64 | 223 | 109 | 342,612 | 142 | 42.3 |
| pansurg-curation-raw-open-data (501,262, 164 ms) | Java | 68.2 (65.1–69.7) | 1,188 | 2.37 | 83 | 99 | 308,056 | 362 | 6.0 |
| | Go main | 5.8 (4.9–5.9) | 793 | 1.58 | 390 | 164 | 288,042 | 0 | 23.1 |
| | Go head | **5.2** (5.0–5.2) | 839 | 1.67 | 167 | 54 | 247,439 | 40 | 25.5 |
| noaa-nws-fourcastnetgfs-pds (4,081,170, 99 ms) | Java | 82.8 (75.8–85.1) | 4,954 | 1.21 | 313 | 61 | 182,414 | 672 | 8.1 |
| | Go main | 14.3 (14.1–15.2) | 5,844 | 1.43 | 2,445 | 1,123 | 1,761,578 | 0 | 42.3 |
| | Go head | **11.6** (11.3–11.6) | 5,503 | 1.35 | 1,129 | 442 | 809,578 | 616 | 46.4 |
| nara-1950-census (13,540,310, 103 ms) | Java | 102.3 (102.3–103.2) | 14,929 | 1.10 | 990 | 166 | 708,051 | 679 | 19.9 |
| | Go main | 53.7 (50.4–54.3) | 20,547 | 1.52 | 9,410 | 4,181 | 7,002,326 | 0 | 40.4 |
| | Go head | **29.6** (29.6–30.4) | 14,396 | 1.06 | 918 | 260 | 781,326 | 60 | 51.5 |

How the columns are counted. Java's splits are its owner splits plus steals, and its empty children are
`CHILD_MASS/empty`. Its overfetched keys are `swath.page.raw_keys` minus objects. Its probes are all of its
probe kinds, while Go's are structure probes, which are delimiter requests.

**Wall.**
- Against Java, the head is 13.5x and 13.1x faster on the shallow recordings and 7.1x and 3.5x on the deep
  ones.
- Against main it is 0.86x, 0.90x, 0.81x and 0.55x the wall.
- The deep recordings gain the most. On nara, splits fall from 9,410 to 918 and empty children from 4,181 to
  260. Overfetched keys fall from 7.0M to 0.78M, against Java's 0.71M, and mean active rises from 40 to 51 of
  64.

**Requests.** They fall where the old policy over-split: −30 % against main on nara, now 3.6 % below Java's,
and −6 % on fourcastnetgfs, 11 % above Java's. On the shallow recordings they rise:
- +12.6 % against main on eri, to 1.64 per 1k keys, 2.4 % above Java;
- +5.8 % on pansurg, still 29 % below Java.

The rise is the structure probes the head sends on its evidence (142 and 40 per run), which more than cancel
the requests saved by fewer splits. Replay charges nothing for a request, so these runs show the wall gain and
the probe cost side by side but not what the probes cost under throttling.

Replay inflates Java's serial tail, so the live runs below are smaller wins.

**Reproduce:** build the replay oracle and put the recording under `$BENCH_DATA/recordings/BUCKET/`, then:

```bash
printf 'java-default\tjava\t\ngo-head\tgo\t/path/to/swath\n' > arms.tsv
REPLAY_ENV=/path/to/env.sh bench/policy_replay.sh arms.tsv noaa-eri-pds 99 19300 3 out/eri
python3 bench/policy_table.py out/eri
```

The harness runs Go arms as `swath diag list` with 64 workers and `--seed`. The latencies are 99, 164, 99 and
103 ms for the four recordings.

### Before the engine campaign (pre-merge build)

The port measured these recordings before it merged as `01dd55b`, on a container that no longer exists, using
the recordings of that time. That build had the spec pivot with a clamped density refinement and a single-page
seed, and none of the engine campaign's work: no anchor reflection, gallop, admission gates or seed descent.
Provenance is incomplete: the exact Go revision, the full command lines and the reports of these runs were
not kept, and the harness templates cannot recover them. Same configuration, 3 interleaved repetitions, median (min–max):

| Recording (objects, latency) | Arm | Wall s | Requests | Requests per 1k keys | Splits | Empty children | Overfetched keys | Mean active |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| noaa-eri-pds (845,693, 99 ms) | Java | 43.4 (43.0–43.5) | 1,353 | 1.60 | 94 | 100 | 148,904 | 7.7 |
| | Go | 3.7 (3.7–3.8) | 1,228 | 1.45 | 466 | 150 | 378,597 | 35.6 |
| pansurg-curation-raw-open-data (501,262, 164 ms) | Java | 66.4 (66.1–68.8) | 1,174 | 2.34 | 77 | 99 | 310,056 | 6.8 |
| | Go | 6.0 (5.1–6.2) | 803 | 1.60 | 395 | 168 | 298,042 | 22.9 |
| noaa-nws-fourcastnetgfs-pds (4,081,170, 99 ms) | Java | 83.2 (80.8–92.4) | 4,913 | 1.20 | 296 | 55 | 171,414 | 8.3 |
| | Go | 14.8 (14.6–15.6) | 5,803 | 1.42 | 2,337 | 1,106 | 1,720,578 | 43.6 |
| nara-1950-census (13,540,310, 103 ms) | Java | 109.4 (108.8–111.3) | 14,942 | 1.10 | 994 | 170 | 715,051 | 19.6 |
| | Go | 57.2 (55.9–58.1) | 20,928 | 1.55 | 9,847 | 4,504 | 7,383,588 | 40.4 |

`recordings1`'s main arm (`62b224f8`) reproduces these rows to within a few percent.

## Splits, overfetch and requests on the deep recordings

These figures come from `recordings1`'s medians, above.

| Recording | Arm | Splits | Overfetched keys per split | Empty children per split | Requests against Java |
|---|---|---:|---:|---:|---:|
| fourcastnetgfs | Java | 313 | 583 | 0.19 | – |
| | Go main | 2,445 | 720 | 0.46 | +18 % |
| | Go head | 1,129 | 717 | 0.39 | +11 % |
| nara-1950-census | Java | 990 | 715 | 0.17 | – |
| | Go main | 9,410 | 744 | 0.44 | +38 % |
| | Go head | 918 | 851 | 0.28 | −4 % |

- **Main.** It split 7.8x and 9.5x as often as Java. Each split adds a trimmed last page and a child's first
  page, so its requests and overfetched keys followed.
- **The head.** It splits 3.6x as often as Java on fourcastnetgfs and slightly less often than Java on nara.
  It holds 46–52 ranges active against Java's 8–20, so the remaining gap to Java is concurrency, not split
  count. Its overfetch per split is higher than main's on nara because it has fewer splits over a similar
  number of range ends.
- **Reading the ratios.** Overfetch per split is total overfetch over all splits. It includes every range's
  last page, seeded and split ranges alike, so it is not the marginal cost of a split. The empty-children
  count adds lower and upper children together.
- **S3 against GCS.** S3 has no end bound, so a range's last page reads past it. GCS ranges send `endOffset`
  and overfetch nothing ([GCS route parity](#gcs-route-parity)).

A second session of the pre-merge build gave Go 6,078 and 19,128 requests and 2,464 and 7,131 splits on
fourcastnetgfs and nara, in line with `recordings1`'s main arm.

**Reproduce:** run the commands above on `noaa-nws-fourcastnetgfs-pds` (99 ms) and `nara-1950-census` (103 ms).

## Engine campaign, 2026-11-07 (prod key lists through a fake at 50 ms)

**What it is.** Production key lists: the keys of anonymous public S3 buckets of 1M to 150M objects, taken from
earlier Java runs' sorted output, served as ListObjectsV2 by a private key-list fake (`keyfake`: start-after,
continuation tokens and delimiter pages, a flat 50 ms per page). No request reached S3 (the live runs below say so).
One run per arm and list, 64 workers, `--count-only` unless an arm says otherwise, a 600 s cap per arm unless
stated, on n4-highcpu-16 Spot VMs (GCP Batch). These are diagnostic
runs outside the study harness: no receipts. The ideal is `ideal64 = (pages/64 + 7 + seed round trips) × 50 ms`
(one seed round trip in the candidate's ideal, none in the baseline's).

- **Baseline:** `577c7c2` (`01dd55b`, the Go port as merged, plus the `split.max_depth` counter), on 251 lists.
- **Candidate 1:** `091c4534`: the anchor reflection and the gallop, the refused sliver, structure probes on
  their evidence, the seed's pagination, descent and resume seeding with `--seed` on, and the order-stage stop
  latch. Not in it: the retained-bound copies and `order.Count`, the review fixes, the admission gates. Run on
  the 201 lists of the iteration set (the 251 less every fifth by size, held out).
- **Java:** the Java CLI at the same latency, `--format=discard --checkpoint=none --concurrency=64`. The
  baseline's Java comparison is a 41-list subset (every fifth list of the iteration set) run with `2df5b331`, the baseline plus
  instrumentation, whose decisions are the baseline's.

| Gate | Campaign bar | Baseline | Candidate 1 | Candidate 1, `--seed=false` |
|---|---|---|---|---|
| completed within the cap | all | 232 of 251 | **201 of 201** | 201 of 201 |
| wall / ideal64, median | ≤ 2.0 | 5.32 | **1.31** | 1.42 |
| wall / ideal64, p90 | ≤ 3.0 | 14.2 | **1.82** | 1.93 |
| wall / ideal64, worst | ≤ 5 | 45.7 | **2.5** | 5.2 |
| requests per 1k keys, median / p90 | ≤ 1.20 / 1.40 | 1.58 / 1.69 | **1.10 / 1.41** | 1.16 / 1.45 |
| `split.max_depth` within 4 log2(pages) + 8 / 3 log2(pages) + 8 | every list | 3 of 232 | **185 / 150 of 201** | 136 / 60 of 201 |
| mean active ranges, lists of 10M keys or more | ≥ 40 of 64 | 20 (median) | **56.8** | 55.5 |
| regressions against the baseline: wall > 1.05x / requests > 1.10x | 0 / 0 | – | **0 / 2** (noaa-ghcn-pds +12 %, mogreps-g +11 %, both about 1M keys) | 0 / 2 |
| lists where Go is slower than Java | 0 | 24 of 41 | **2 of 200** (fcp-indi 183 against 130 s, noaa-goes19 154 against 147 s); Go / Java wall median 0.31 | 4 of 200 |
| wall on lists under 2M keys, against the baseline | not slower | – | **0.34x** | 0.35x |
| peak RSS, count-only | – | median 0.48 GB, p90 2.4 GB, max 17.3 GB | median 0.52 GB, p90 0.87 GB, max 2.9 GB | – |

- The split chain is gone at breadth: depth, wall and mean active move from a chain of dependent requests to
  near the ideal on most lists, and every list completes.
- The seed on by default is a win on these lists: 185 against 136 lists within the depth bound, worst 2.5x
  against 5.2x.
- Still open in candidate 1: requests on small lists (the lists above 1.40 requests per 1k keys are almost all
  under 3M keys: empty children of pivots in key gaps), 16 lists above the depth bound, the two request
  regressions, and the two large lists where Java is faster. Peak RSS in this binary still includes the
  retained-page leak ([Memory behind a held-up frontier](#memory-behind-a-held-up-frontier)).

**Builds after candidate 1.** Each row of the tables below names the build it measured. `7b05605` (merged to
main as PR #255) adds to candidate 4 a consistent total order for the remainder rank (a decision change: three
ranges could each outrank the next) and cancellation of `order.Count` between children; its own sweep of the
whole corpus is `head1`, below, and the scale, one-core and study runs further down name it too.

- **Candidate 3:** `c48ea228`: candidate 1 plus the retained-bound copies and `order.Count`, the review fixes and
  the admission gates.
- **Candidate 4:** `26f363ea`: candidate 3 plus the admission override (a gate refuses a split at most 4
  consecutive pages while workers want work) and the rank's fallback to age on an unknowable estimate.

**Candidate 3 on the iteration set** (2026-11-07, 201 lists, same fake and VMs, 600 s cap per arm). Arms:
count-only; sorted Parquet, whose keys were compared byte for byte with the key list served (the keys of the
Java run's sorted output); the Java CLI as above; a kill (SIGKILL to the process group) at 40 % of the keys
then `swath resume`, compared row for row; and `--spill-max` at a tenth of the default.

| Gate | Campaign bar | Candidate 3 |
|---|---|---|
| completed within the cap | all | **201 of 201**, every arm |
| wall / ideal64, median / p90 / worst | ≤ 2.0 / 3.0 / 5 | **1.19 / 1.76 / 8.1** (haisslab, an admission regression) |
| requests per 1k keys, median / p90 | ≤ 1.20 / 1.40 | **1.07 / 1.35** |
| `split.max_depth` within 4 log2(pages) + 8 / 3 log2(pages) + 8 | every list | 181 / 156 of 201 |
| mean active ranges, lists of 10M keys or more | ≥ 40 of 64 | **61.9** |
| regressions against the baseline: wall / requests | 0 / 0 | **3 / 0** (haisslab 18 → 48 s, common-screens 115 → 192 s, spacenet 13 → 13 s) |
| lists where Go is slower than Java | 0 | **3 of 199** (common-screens, haisslab, nexrad-arco 86 against 67 s) |
| peak RSS, count-only | – | median 0.08 GB, max 0.1 GB |
| sorted Parquet keys byte-identical to the key list | every list | **201 of 201** (see below) |
| kill at 40 % then resume, rows identical | every list | **201 of 201** |
| `--spill-max` at a tenth, wall against unconstrained, six largest lists | ≤ 2x | pds-css-archive 2.87x, genome-browser 1.94x, logan-pub 1.60x; hrrr, rap, pmc 0.99–1.07x; output identical on 193 of 194 lists |

The first sorted-Parquet pass matched on 200 of 201: 17 keys of nifs-lhd end in a bare carriage return, which
the fake sent raw although the client asks for `encoding-type=url` (S3 percent-encodes such keys), and XML
line-end normalization turned it into a newline. With the fake fixed, that list's 22,156,937 rows are
byte-identical too. The three regressions are one admission gate (`split.skipped.upper_tiny`: the gauge
underestimated the upper remainder and held a tile's remainder serial); on the 50 held-out lists the same gate
took argoverse (19.1M keys) from 121 s to 267 s at 4 in flight (`idle_demand_ns` 16,000 s over the run), which
the admission override of candidate 4 removes.

**Candidate 4 on the iteration and held-out sets** (2026-11-07, same fake and VMs, count-only and the Java CLI;
no sorted-Parquet or kill arm ran on this build's 201 and 50):

| Gate | Campaign bar | Candidate 4, iteration (201) | Candidate 4, held-out (50) |
|---|---|---|---|
| completed within the cap | all | **201** | **50** |
| wall / ideal64, median / p90 / worst | ≤ 2.0 / 3.0 / 5 | **1.20 / 1.67 / 2.3** | **1.19 / 1.50 / 2.2** |
| requests per 1k keys, median / p90 | ≤ 1.20 / 1.40 | **1.08 / 1.35** | **1.09 / 1.37** |
| `split.max_depth` within 4 log2(pages) + 8 / 3 log2(pages) + 8 | every list | 186 / 154 | 49 / 42 |
| mean active ranges, lists of 10M keys or more | ≥ 40 of 64 | **60.7** | **61.1** |
| regressions against the baseline: wall / requests | 0 / 0 | **0 / 0** | **0 / 0** |
| lists where Go is slower than Java | 0 | **0 of 199** (Go / Java wall median 0.29) | **0 of 50** (median 0.26) |
| wall on lists under 2M keys, against the baseline | not slower | 0.32x | 0.37x |
| peak RSS, count-only | – | max 0.2 GB | max 0.1 GB |

What separates candidate 4 from the stricter bar (requests p90 ≤ 1.25, every list within the depth bound):
empty children at folder gaps on small lists, which only a structure probe sees, and 16 lists above the depth
bound that still run at 1.2–1.5x the ideal.

**`7b05605` on the whole corpus** (run `head1`, 2026-11-07; the 251 lists, same fake and VMs, 64 workers; arms:
count-only, sorted Parquet compared byte for byte with the key list, SIGKILL at 40 % then `swath resume`, and the
Java CLI as above):

| Gate | Campaign bar | `7b05605` |
|---|---|---|
| completed within the cap | all | **251 of 251** |
| wall / ideal64, median / p90 / worst | ≤ 2.0 / 3.0 / 5 | **1.18 / 1.62 / 2.4** |
| requests per 1k keys, median / p90 | ≤ 1.20 / 1.40 | **1.078 / 1.353** |
| mean active ranges, lists of 10M keys or more | ≥ 40 of 64 | **62** |
| lists where Go is slower than Java | 0 | **0 of 251** (Go / Java wall median 0.26) |
| sorted Parquet keys byte-identical to the key list | every list | **251 of 251** |
| kill at 40 % then resume, rows identical | every list | **251 of 251** |
| peak RSS, count-only | – | at most 0.1 GB |
| peak RSS, sorted Parquet | – | median 0.46 GB, at most 0.78 GB (fcp-indi, 113M keys) |

**Small lists** (run `small1`, 2026-11-07; 318 production lists of 10k to 1M objects never swept before,
`7b05605`, n4-highcpu-4, 64 workers, fake at 50 ms; medians):

| Arm | Wall s (p90) | Wall / ideal64 median, p90 | Against Java, median | Slower than Java | Requests / 1k | Parquet keys identical |
|---|---:|---|---:|---:|---:|---:|
| count-only | 0.86 (1.8) | 1.73, 2.77 | 0.063 | 0 of 318 | 2.20 | – |
| sorted Parquet | 0.90 (2.0) | 1.89, 2.88 | 0.068 | 0 of 318 | 2.19 | 318 of 318 |
| Java `--format=discard` | 17.8 (22.1) | | | | | |

On a list of a few hundred pages the seed (up to 64 delimiter requests), the structure probes and the empty
children of the first splits are a large share of all requests (up to 5.7 per 1,000 keys on a 60k-key list).
Nothing here is slow in absolute terms; this run predates the seed's first data page
([red-team fixes](#red-team-fixes-2026-11-07)).

**The giants** (2026-11-07; 28 lists of 160M to 593M keys never swept before, n4-highcpu-32 with a 150 GB
boot disk, 64 workers, fake at 50 ms). Count-only on `7b05605` (run `huge1b`): every list at 1.05–1.09x the
64-worker ideal, mean active 63 of 64, 1.00–1.03 requests per 1k keys, RSS 78–135 MB. Sorted Parquet: 25 of 28
at 1.16–1.72x the ideal, output identical; three crawled under the spill cap with plain frames (about 150 B per
row against half the free disk). The six worst again on the delta+zstd build `bf5e6d52` (run `huge2`), at the
default spill cap:

| List | Keys | `7b05605` (plain frames) | `bf5e6d52` (delta+zstd) | Spill written | Refused | Identical |
|---|---:|---:|---:|---:|---:|---|
| meeo-s3 | 267M | 359 s (1.72x) | 285 s (1.36x) | 0.3 GB | 0 | yes |
| gnss-ro-data | 305M | 377 s (1.58x) | 310 s (1.30x) | 0.9 GB | 0 | yes |
| oedi-data-lake | 353M | 992 s (3.6x) | 360 s (1.31x) | 0.5 GB | 0 | yes |
| bhl-open-data | 461M | 544 s (1.51x) | 492 s (1.37x) | 0.2 GB | 0 | yes |
| noaa-goes16 | 484M | 1,454 s (3.85x) | 539 s (1.43x) | 1.9 GB | 0 | yes |
| cellpainting-gallery | 523M | stopped at 3,001 s, 80M of 523M rows | 549 s (1.34x) | 1.1 GB | 0 | yes |

Peak RSS 0.6–0.7 GB on the delta+zstd build. The fake's payload is constant (one ETag, time and size), so its
spill bytes understate real ones; the frame benchmark under
[delta-coded spill frames](#delta-coded-spill-frames) has real payloads.

**The integration build** (run `e2a`, 2026-11-07; `b309ad41`, `7b05605` with the text sink and delta+zstd spill
frames; the 251 lists, same fake and VMs): count-only 251 of 251, wall / ideal64 1.16 median, 1.57 p90, 2.3
worst; 1.08 requests per 1k keys (p90 1.34); mean active 62 of 64 on lists of 10M keys or more; peak RSS at most
0.1 GB; sorted Parquet 251 of 251, byte-identical to the key list.

**After the red-team fixes** (run `e2b`, 2026-11-07; `7fb447f8`; the 251 lists, same fake and VMs, 64 workers;
count-only, sorted Parquet and a kill at 40 % then `swath resume`): 1.08 requests per 1k keys (median); sorted
Parquet peak RSS median 0.45 GB, at most 0.90 GB (ooni-data, 15.5M keys; its resume 0.89 GB).

**The sorted run's working set** (candidate 4, 2026-11-07; the 64 lists of 20M keys or more, same fake,
n4-highcpu-16, 64 workers, sorted Parquet, the default 128 MiB `--row-budget`, 900 s cap; medians over the
lists):

| Arm | Completed | Wall s | Rows spilled | Rows ahead of the frontier at peak | Lists with spill refusals | Budget wait s (summed) | Requests / 1k | Keys byte-identical |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| defaults | 64 of 64 | 51 | 96 % | 71 % | 0 | 0 | 1.02 | 64 |
| `--spill-max` 2 × the row budget | **8 of 64** | 275 | 62 % | 13 % | 8 | 15,442 | 1.02 | 8 |
| kill at 40 % then `swath resume` | 64 of 64 | 71 | 95 % | 67 % | 0 | 0 | 1.02 | 64 |

Under today's ordered emission a healthy sorted run of a large list holds most of its rows ahead of the
publish frontier and spills nearly all of them once: work stealing finishes ranges in roughly key-uniform
order, so the leftmost unfinished range holds the drain until listing is nearly over (an earlier sweep of
candidate 3 put `peak_ahead_rows` at 0.90–0.98 of the dataset on skewed tiles, and its pq arm spilled a median
86 %, p90 97 %, of all rows, at 11–111x the Parquet output's bytes). The set is bounded by `--spill-max`, not
by `--row-budget`. A tight cap does not degrade gracefully: at 2 × the budget, 56 of 64 lists did not finish
within 900 s. And a kill relists most of the run: the 40 % kill on candidate 3 relisted 90–100 % of the keys
on every list of 20M or more (0–12M rows durable at the kill), since the durable frontier is the published
parts' ([Known gaps](#known-gaps)). A frontier-priority variant of demand and claiming cut the ahead set to
10 % of rows and completed 39 of 64 spill-capped runs, but ran a median 3.2x slower on the defaults and was
not adopted.

**Live S3** (candidate 3, `c48ea228`, 2026-11-07). One n4-highcpu-16 Spot VM in GCP us-east1 listing the two
buckets in AWS us-east-1: cross-cloud, the same geography, one run each, Go then Java. Anonymous,
`--count-only --max-duration 15m` at the defaults (64 workers); Java `--format=discard --concurrency=64`. Attempt
latency as Go measured it: p50 84 ms, p99 168 ms on nbm; p50 117 ms, p99 268 ms on fourcastnetgfs.

| Bucket | Objects | Go wall | Go requests (per 1k keys) | Go mean in flight / `split.max_depth` / RSS | Java wall | Java requests | Java in flight |
|---|---:|---:|---:|---|---:|---:|---:|
| noaa-nbm-grib2-pds | 69,132,677 | **91 s** | 69,772 (1.01) | 63 / 16 / 74 MB | 99 s | 70,454 | 56 |
| noaa-nws-fourcastnetgfs-pds | 4,081,170 | **12 s** | 5,446 (1.33) | 47 / 59 / 64 MB | 57 s | 5,008 | 9 |

No throttle, no retry, both counts exact. At 64 wide and the measured p50 the nbm ideal is 69,133 pages / 64 ×
84 ms = 91 s; the baseline build crawled at 4 in flight on this key list and was killed at 710 s through the
fake. fourcastnetgfs still sends 9 % more requests than Java (empty children on a small bucket). One run per
tool: an observation on that VM and that hour, not a rate.

**A billion live objects** (runs `cogslive1`, `cogslive2` and `cogslive3`, 2026-11-07). sentinel-cogs in AWS
us-west-2 (about 1.09 billion objects), listed anonymously from one n4-highcpu-32 in GCP us-east1 with a 400 GB
disk, arms one after another, each capped at 40 minutes; the delta+zstd build `bf5e6d52` unless named. Java's
rows are the scale study's release rows on the same bucket (Java 0.3.2, sorted Parquet, 16 GB heap).

| Arm | Result |
|---|---|
| count-only, 64 workers | stopped at the 40-minute cap with 1.066B objects listed: listing-bound at about 144 ms per request, 1.00 requests per 1k keys |
| **count-only, 1,024 workers** | **1,088,794,418 objects in 177 s** (6.1M keys/s), 1,117,553 requests (1.03 per 1k), in flight avg 982, peak 1,024, split depth 55, RSS 1.1 GB, no retries |
| sorted Parquet, 64 workers | stopped at the cap: about 1.05B rows listed and spilled (31 GB), 182M published |
| sorted Parquet, 64 workers, `7b05605` (plain frames), `--spill-max 300GiB` | stopped at the cap: 149 GB spilled, 182M rows published |
| **sorted Parquet, 256 workers** | **1,088,794,418 rows in 1,115 s**, 1,094,847 requests (1.01 per 1k), spill 32 GB (about 30 B per row), RSS 1.2 GB, 488 parts, mean active 141 of 256, no retries |
| sorted Parquet, 1,024 workers | 846 s, 1,115,517 requests (1.02 per 1k), in flight avg 210 of 1,024, 480 parts, spill 32.5 GB, RSS 2.2 GB, 5 encoders, `encoder_wait` 318 s, `budget_wait` 0 |
| the same, `--row-budget 1GiB` (32 encoders admitted) | 941 s, RSS 4.5 GB |
| Java, sorted Parquet, 1,024 / 2,048 concurrency | 654 s / 253 s |

At 1,024 workers the sorted run is bound by its output: after listing ends, the rows drain through one open
Parquet part at a time, and neither a larger row budget nor more encoders moved the wall. Java's sorted run at
the same concurrency was faster on this bucket. One run per arm, cross-cloud: an observation on that VM and
that day.

**Workers above 64** (`7b05605`, 2026-11-07; the 64 lists of 20M keys or more, fake at 50 ms, n4-highcpu-16;
medians over the lists):

| Arm | Wall s | Against count-only at 64 | Mean active | `split.max_depth` | Requests / 1k |
|---|---:|---:|---:|---:|---:|
| count-only, 64 workers | 41.5 | 1.00 | 63 | 38 | 1.019 |
| count-only, 256 | 12.9 | 0.31 | 237 | 49 | 1.074 |
| count-only, 512 | 11.9 | 0.27 | 458 | 51 | 1.124 |
| sorted Parquet, 256 | 29.3 | 0.69 | 114 | 50 | 1.075 |

The engine scales near-linearly to 256 workers and still gains at 512 (requests +10 %, the fan-out's cost).
Sorted output at 256 runs at 114 active and 2.3x the count-only wall: about half its wall is a single-threaded
drain after listing ends (at peak 94 % of rows are ahead of the frontier), so on 16 vCPU the order stage, spill
and encoders are the limit above 64 workers, not the engine.

**One core, sorted Parquet** (n4-highcpu-2, 2 vCPU and 4 GB, `GOMAXPROCS=1` so one encoder, fake at 50 ms, the
three largest iteration lists; Java pinned with `-XX:ActiveProcessorCount=1 -Xmx2g`, `--format=parquet`; one
run each, 2026-11-07):

| List | Keys | Java, 1 core | Go candidate 4 (`26f363ea`) | Go `7b05605` | Spill written (`7b05605`) |
|---|---:|---:|---:|---:|---:|
| noaa-goes19 | 144.8M | 515 s | 559 s | **465 s** | 22.2 GB |
| chalmerscloudiceclimatology | 139.1M | 362 s | 365 s | **347 s** | 16.0 GB |
| deafrica-sentinel-5p-cloud | 129.8M | 446 s | 469 s | **415 s** | 20.4 GB |

On candidate 4 Java was ahead by 1–8 % on one core; on `7b05605` Go is ahead by 4–17 %. Both tools are bound
by the one core and the disk there (Go writes and reads back 86–98 % of its rows through spill). Go's output
was byte-identical to the key list on candidate 4's runs. The same lists take 187, 154 and 120 s on
n4-highcpu-16.

**Through the study harness** (`7b05605`, 2026-11-07). The scale study's replay fixtures and harness (its
replay image, fixture digests, latency deadlines, warm-up, machines, container memory and subject vCPUs as in
the release rows), with Java 0.3.2 beside Go at the same concurrency, output written. Every row completed with
the fixture's row count; content was not compared in these rows.

| Fixture | Keys | Machine (subject) | Mode | Workers | Java s | Go s | Go / Java | Go RSS |
|---|---:|---|---|---:|---:|---:|---:|---:|
| aws-public-blockchain | 143.0M | n4-highcpu-80 (8 vCPU, 8 GB) | sorted Parquet | 256 | 218 | 170 | 0.78 | 1.2 GB |
| | | | sorted Parquet | 64 | 324 | 258 | 0.80 | 0.6 GB |
| | | | zstd TSV (Java: a dataset, 3 writers; Go: one file) | 256 | 154 | **284** | **1.84** | 1.1 GB |
| idc-open-data (no injected latency) | 56.3M | n4-highcpu-16 (6 vCPU, 16 GB) | sorted Parquet | 64 | 125 | 82 | 0.66 | 0.6 GB |
| noaa-nbm-grib2-pds | 66.4M | n4-highcpu-32 (4 vCPU, 8 GB) | sorted Parquet | 64 | 159 | 105 | 0.66 | 0.5 GB |
| | | | TSV (one file; no Java row) | 64 | – | 112 | – | 0.4 GB |
| nara-1950-census | 13.5M | n4-highcpu-32 | sorted Parquet | 64 | 79 | 27 | 0.34 | 0.5 GB |
| real-changesets | 13.9M | n4-highcpu-32 | sorted Parquet | 64 | 63 | 34 | 0.54 | 0.4 GB |
| noaa-nws-fourcastnetgfs-pds | 4.1M | n4-highcpu-16 | sorted Parquet | 64 | 48 | 12 | 0.25 | 0.4 GB |

The one loss is the zstd text output at 256 workers: the engine waited on the output (mean active 76 of 256
against 118 for the Parquet row on the same fixture), spilled 21.5 GB under `$TMPDIR`, which in the container is
its overlay filesystem, and one zstd writer compressed about 21 GB of rows where Java runs three text writers.
The study's timing grades for these rows describe its instrument, equally for both tools in a group.

## Warm start from the previous dataset (`--seed-from`)

**Synthetic, local.** The first 2,000,000 keys of the argoverse production key list (hierarchical: hex scene
ids, then frame files) served by `keyfake` at a flat 50 ms per page, 64 workers, sorted Parquet output to the
local disk, on the 4-vCPU, 7 GB build VM (GCP n4, shared with nothing else). Three runs per arm, interleaved by
repetition. *Cold* is the default (delimiter seed); *seeded* lists the same keys with `--seed-from` the first
cold run's dataset (one part, its key page index read: 1,954 cuts merged into 256 tiles); *grown* replaces keys
800,001–850,000 by ten keys each (2,450,000 keys, one segment 10×) and lists it cold and seeded from the stale
dataset. Rows equal keys in every run; the seeded datasets are byte-identical to the cold ones (same parts, same
digests).

| Arm | Wall, s (3 runs) | Requests / 1k keys | `split.max_depth` | Empty children (lower+upper) | Accepted splits | Mean active | Prior error within / grown |
|---|---|---|---|---|---|---|---|
| cold, 2.0M | 4.03, 4.03, 4.01 | 1.140–1.148 | 25–27 | 49–56 | 263–279 | 39.0–39.7 | – |
| seeded, 2.0M | 2.68, 3.85, 3.11 | 1.112–1.113 | 2 | 0 | 29–34 | 57.8–60.5 | 255 / 0 |
| cold, grown 2.45M | 5.35, 4.65, 6.44 | 1.133–1.146 | 28–29 | 65–87 | 330–359 | 30.1–45.0 | – |
| seeded from the stale dataset, 2.45M | 3.83, 3.90, 3.85 | 1.118–1.122 | 9 | 23–27 | 134–147 | 49.9–50.5 | 248 / 7 (max 10×) |

The seeded runs send no seed request (24 cold) and split only at the tail; the requests above 1 per 1,000 keys
are each tile's last page (S3 has no end bound, so a tile's final request is a partial page) and the frontier's.
On the stale prior the 7 tiles of the grown segment are counted `grown` (max 10×) and split inside it (depth 9
against 2), and the run still beats the cold one. The seeded walls vary with the Parquet encoders on 4 cores
(the ideal at 64 workers is about 1.9 s).

**The whole corpus** (run `warm1`, 2026-11-07; `fe7347e7`, the 251 production lists, 64 workers, fake at 50 ms,
sorted Parquet, n4-highcpu-16): a cold run to a dataset, then the same list seeded from it.

| Arm | Wall / ideal64 median, p90 | Against cold, median, p90 | Requests / 1k median, p90 | `split.max_depth` median | Mean active | Identical |
|---|---|---|---|---:|---:|---:|
| cold | 1.47, 1.96 | – | 1.076, 1.349 | 29 | 48 | 251 of 251 |
| seeded from the cold dataset | 1.11, 1.19 | 0.76, 0.96 | 1.019, 1.153 | 3 | 60 | 251 of 251 |

One list of 251 was more than 5 % slower seeded than cold.

**A stale prior** (run `stale4`, 2026-11-07; `fe7347e7`, ten held-out lists of 44M to 131M keys): a cold
listing to a dataset, then rows 40–45 % of the key list each replaced by ten keys (the bucket about 45 % larger
than the prior describes, all the growth in one region), then `--seed-from` the stale dataset. Every run listed
at 1.11–1.15x the 64-worker ideal of the grown list, 1.001–1.006 requests per 1k keys, split depth 5–12, and
its keys byte-identical to the grown key list: the grown tiles are found on their first page and split inside.

## Red-team fixes, 2026-11-07

The red team's shapes suite (a set of synthetic key lists, each an adversarial shape such as `%` in every key
or random throttling, served by a local static S3 fake; 64 workers unless named; the suite is project bench
tooling outside this repository) run before (`4ad9eecc`) and after (`04cbff74`) on the shared 4-core box, one
arm at a time. Walls on a loaded box; the ratio is wall over the arm's ideal (pages / workers + 7 + seed round
trips, × RTT). Every arm listed rows == keys; the `-o` arms' keys are byte-identical to the key file.

| arm | before wall s (× ideal) | after wall s (× ideal) | what changed |
|---|---|---|---|
| `percent_keys` (2M, `%` in every key, 50 ms) | 111.3 (51.5) | 3.1 (1.5) | S3 sends a real key's `%` as start-after |
| `pq.percent_keys.d200` (sorted Parquet, 200 ms) | serial, ~430 | 11.4 (1.4) | same; byte-identical |
| `throttle5.baseline` (5 % random 503) | 112.7 (56.0) | 2.5 (1.25) | the limiter tolerates a background under 15 % (before the merge-review fix: 10 %) |
| `throttle1.baseline` (1 %) | 60.2 (29.9) | 2.3 (1.1) | same |
| `rps300.baseline` (300 req/s cap) | 23.9 (11.9) | 8.6 (4.3; `--limiter=false` 9.2) | one cut per round trip; plateau at the cap |
| `tiny100.d200` / `tiny1k.d200` | 0.81 / 0.83 | 0.20 / 0.20 (= `--seed=false`) | the seed's first data page lists them alone |
| `tiny100` requests per 1k keys | 70 | 20 | same |
| `hotcold.narrow.d200` (one folder at 2 s pages) | 22.0 (2.74) | 22.2; `--slow-split` 18.0 (2.24) | slow-range split, off by default |

Over the 74 arms in both runs the after/before ratio's geometric mean is 0.59; the largest rise is
`wide_top.d200` at 1.08x (box noise at its size; its requests per 1k keys are unchanged). `--slow-split` on
`hotcold.half`, `jitter.baseline`, `zipf_folders.d200` and `baseline.d200` moved none by more than 1 %. On
2M-key lists the first data page costs nothing measurable (`baseline.d200` 9.3 to 8.5 s, requests per 1k
unchanged). In the rejected implementation, a truncated first page was emitted and the first tile
started after it. On the 12.6M-key megascenes list this left a serial tail: wall time rose from
12 s to 20 s and mean active workers fell from 57 to 35, with the same requests and splits.
The retained implementation discarded that page and started the tiles at the scope's start.
The flat-leaf arms spent 4–6 delimiter waves finding no cut before flat chains stopped being
followed. In the earlier Java seed measurement, a serial 256-probe descent spent 13.5 s of a
39 s run on its chain. These are dated observations, not current performance bounds.

## Split admission on small lists

The keyfake S3 fake (`keyfake-static`, 50 ms a page, ListObjectsV2, path-style, anonymous) on 4 shared cores,
64 workers, `--count-only`, 3 interleaved repetitions, median; before is `go/engine` at `2cb204c2`, after adds
the admission gates and the remainder rank ([range-engine.md](internals/range-engine.md)).
The synthetic lists follow the shapes of the sweep's worst small lists: `mogreps` 1.04M objects flat at the
root (`prods_op_mogreps-g_YYYYMMDD_RR_MM_TTT.nc`), `urma` 1.77M under 18,400 day folders
(`akurma.YYYYMMDD/akurma.tHHz.<product>.grb2`), `zarr` 1.26M zarr chunks seven levels down under two top
folders; `fourcastnetgfs` is the recorded 4.1M-key list. Depth bound: 4 log2(pages) + 8.

| List | Arm | Requests / 1k | Splits | Empty children | Wall s | Depth (bound) | Mean active |
|---|---|---:|---:|---:|---:|---:|---:|
| mogreps 1.04M | before | 1.407 | 626 | 275 | 2.32 | 31 (48) | 34.8 |
| | after | 1.202 | 323 | 88 | 2.23 | 30 | 31.0 |
| urma 1.77M | before | 1.308 | 765 | 371 | 3.48 | 43 (51) | 38.7 |
| | after | 1.282 | 776 | 317 | 3.63 | 48 | 37.7 |
| zarr 1.26M | before | 1.715 | 695 | 446 | 3.01 | 35 (49) | 36.4 |
| | after | 1.478 | 401 | 181 | 2.77 | 31 | 35.3 |
| fourcastnetgfs 4.1M | before | 1.422 | 1,426 | 687 | 6.45 | 58 (55) | 49.1 |
| | after | 1.343 | 1,126 | 442 | 6.07 | 56 | 48.2 |

- Requests fall 2–15 %, empty children 15–68 %; wall is within the runs' spread (before 3.29–3.60 s on urma,
  after 3.40–3.92 s; the others fall). fourcastnetgfs was over the depth bound before and stays at it.
- On `urma` the gates hardly engage (0–8 refusals): its empty children are upper children of pivots placed in
  the gap after a month's last day (`akurma.202113` before `akurma.202114`), which a page's density reads as
  keys. The same holds for most of the empties left on the other lists. Only a structure probe sees a folder end.
- An earlier gauge reading the whole page's alphabet (letters of the shared prefix included) and the page's
  density across folders read a remainder holding whole folders as 0 on `zarr` and refused its largest ranges:
  5–8 s against 3.0 s. The run under the interval's prefix and the +∞ reading of a bound in another folder
  are the fix.

**The refusal limit.** On held-out 6M-key slices (same setup, median of 2), the first version of the gates refused
one pivot for a range's life on `e11bio-prism` (the midpoint `…/0/1>` below `hi` `…/0/2`; 1,815 upper-child
refusals): 66.2 s at 5.1 in flight, against 7.4 s before the gates. Admitting the split after 4 refusals in a
row (`split.admission.overridden`):

| Slice | before gates: req/1k, wall s, in flight | gates without limit | gates with limit |
|---|---|---|---|
| e11bio-prism 6M | 1.173, 7.46, 51.9 | 1.047, 66.2, 5.1 | 1.062, 6.51, 54.0 |
| argoverse 6M | 1.127, 7.10, 55.0 | 1.044, 7.12, 50.6 | 1.051, 6.74, 54.1 |

With the limit the small lists keep their savings (mogreps 1.21, urma 1.27, zarr 1.47, fourcastnetgfs 1.34
requests per 1k). The argoverse slice did not reproduce the full list's slowdown (267 s against 121 s), which
the limit bounds the same way.

**Reproduce:** `keyfake-static -keys FILE -addr 127.0.0.1:19600 -delay 50ms`, then `swath list s3://b --endpoint
http://127.0.0.1:19600 --path-style --anonymous --region us-east-1 --count-only --report R.json` (now `swath
count s3://b ...` with the same flags).

## Policy switches

Pre-merge build, in the session of [Before the engine campaign](#before-the-engine-campaign-pre-merge-build), Go
arms only; not rerun on the current engine, which sends structure probes by default on their evidence
(`auto`; 40–616 per run in `recordings1`, [above](#replayed-recordings-go-against-java-defaults)); wall s / requests,
median of 3. `--floors` and `--structure` are `swath diag list` switches (`--structure` here is that build's
boolean switch, before the `auto` mode and the structure probes of the engine campaign); the
seed descent was measured as `--seed` with the hybrid descent, since deleted.

| Arm | noaa-eri-pds | pansurg | fourcastnetgfs | nara |
|---|---|---|---|---|
| baseline (`--seed`) | 3.7 / 1,228 | 6.0 / 803 | 14.8 / 5,803 | 57.2 / 20,928 |
| `--floors` | 4.1 / 1,196 | 6.2 / 709 | 15.9 / 5,663 | 59.0 / 18,448 |
| seed descent (deleted) | 4.3 / 1,591 | 6.6 / 1,280 | 16.0 / 6,083 | 57.8 / 19,664 |
| `--structure` | 3.1 / 2,066 | 4.3 / 1,584 | 14.7 / 10,383 | 41.0 / 34,535 |
| all three | 5.9 / 2,079 | 26.7 / 1,444 | 28.4 / 9,704 | 43.1 / 28,433 |

- Floors: 2–12 % fewer requests and 8–34 % fewer overfetched keys at 3–11 % more wall.
- Seed descent: no gain on any recording; 30–60 % more requests on the shallow ones. Deleted.
- Structure probes: fastest on three of four recordings at 1.6–2.0x the requests (probes are delimiter
  requests). Off: on replay requests cost nothing and nothing throttles.
- All three: slower than structure probes alone everywhere.

These conclusions hold for that pre-merge build and its boolean policy switches only, not for the current
engine: today the seed is on, structure probes are on their evidence (`auto`) and splits pass the admission
gates by default; the floors beside admission are measured below, the current defaults in the
[engine campaign](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms) (candidate 1 and later).

**Floors beside split admission** (synthetic, 2026-11-07). Whether the floors still earn their place now
that admission gates splits: `swath diag list s3://bkt --endpoint http://127.0.0.1:PORT --path-style
--anonymous --region us-east-1 --count-only --floors=false|true --report FILE -q`, built from `go/p2-simplify`
at `e36ec98d`, against `keyfake-static -delay 50ms` serving the first 2,000,000 keys of each production key
list; 64 workers, seed on, structure auto; 2 runs per arm, interleaved; a GCE VM, 4 vCPU, 7 GB, Linux 7.0.
The reports were kept only in the session's scratch space (not archived). Wall s / attempts / leaves /
idle-demand s, the two runs:

| List | floors off | `--floors` |
|---|---|---|
| argoverse | 3.6, 3.3 / 2,290, 2,302 / 537, 559 / 44, 39 | 9.6, 10.1 / 2,247, 2,260 / 484, 499 / 440, 455 |
| e11bio-prism | 5.5, 5.7 / 2,368, 2,371 / 554, 592 / 109, 113 | 6.4, 5.0 / 2,345, 2,342 / 437, 436 / 151, 115 |
| noaa-nbm | 3.2, 3.0 / 2,256, 2,271 / 322, 328 / 18, 15 | 3.1, 3.0 / 2,210, 2,194 / 282, 268 / 20, 20 |
| sorel-20m | 4.2, 3.6 / 2,203, 2,192 / 327, 321 / 65, 52 | 5.5, 4.3 / 2,157, 2,152 / 229, 227 / 103, 78 |

Observed: with admission on, the floors save 1–3 % of attempts and 10–30 % of ranges, and on argoverse cost
2.8x the wall (idle demand tenfold: the remaining-estimate floor refused about 1,000 splits workers were
waiting for). On these runs they add nothing admission does not already do and can starve workers; they stay
a diagnostic switch, off by default, until a production-scale comparison decides their removal.
The floors and `--floors` were later removed on this evidence (2026-11-10).

**1-key pivot probe** (deleted), deep recordings, the pre-merge build's second session ([above](#splits-overfetch-and-requests-on-the-deep-recordings)): with the probe, empty
children fell 1,093 → 201 and 2,908 → 443, but it sent 2.8 and 2.3 probes per split, 31 % and 27 % more
requests, and took 2.3x and 2.2x the wall (fourcastnetgfs 38.0 s, nara 119.9 s), because the owner waits for
the probe. For a fixed stream of split proposals, an asynchronous probe would save at most one request per
empty child it catches (0.41–0.44 per split, lower and upper together) and spend at least one per probe. That is
an accounting bound, not a limit on a policy whose later splits change with what its probes learn.

## Live shallow buckets

Live S3, anonymous, from a GCP container (noaa-eri-pds in us-east-1, pansurg in eu-west-2), Go
`--workers 64 --seed` against Java with the recording's argv (`--concurrency 64 --format parquet --sort`),
interleaved, 2 repetitions each, nothing else running. Process wall from `/usr/bin/time`; Java also reports its
own duration, which leaves out JVM start-up, shutdown and post-run work.

| Bucket (objects) | Arm | Process wall s | Java's reported s (listing) | Requests | Mean active | Splits | Empty children |
|---|---|---:|---:|---:|---:|---:|---:|
| noaa-eri-pds (845,693) | Go | 4.95, 5.20 | – | 1,244, 1,253 | 37.6, 36.1 | 486, 493 | 152, 158 |
| | Java | 30.45, 31.16 | 13.1, 13.5 (10.2, 10.4) | 1,434, 1,437 | 7.5, 7.6 | 73, 68 | – |
| pansurg (501,262) | Go | 11.66, 10.40 | – | 795, 813 | 17.4, 17.5 | 394, 403 | 165, 186 |
| | Java | 54.31, 52.66 | 21.7, 19.9 (19.5, 17.9) | 1,208, 1,198 | 7.9, 8.3 | 42, 37 | – |

- Against Java's own reported duration Go is 2.6x and 1.8x faster (listing phase alone 2.1x and 1.7x); against
  process wall 6x and 5x.
- Go makes 13–33 % fewer requests; the gain is concurrency.
- No 503 and no timeout; one Go retry in four runs. Every dataset is row-identical to the recording.
- Pansurg's live wall varied 1.4x between runs minutes apart (earlier non-interleaved runs: 15.07, 14.75 s).

**Reproduce:** the same argv against the live buckets, alternating the tools; `bench/diffds.py` against the
recording.

## Throttling through the fault proxy

History: the pre-merge build, whose limiter cut on every throttle answer and grew only after 10 s without one
(Java's rule). The current rule (a cut at most once per round trip, only at 15 % throttles or more) is measured
under [red-team fixes](#red-team-fixes-2026-11-07); these arms have not been rerun on it.

Replay at each recording's p50, 64 workers, `--seed`, sorted Parquet; every arm through a fresh
`bench/faultproxy` (seed 1), whose counts give requests and throttles. 3 interleaved repetitions, median
(min–max). Faults: **storm** = 50 % of requests answered 503 SlowDown from 1 s to 6 s after the first request;
**sustained** = 20 % 503 for the whole run; **cap** = 503 beyond 200 requests per trailing second. "off" is
`--limiter=false`, "on" the limiter (the default). Every run exited 0 with a committed dataset.

**Unperturbed and storm** (wall s / requests / throttled):

| Recording | off | on | off, storm | on, storm |
|---|---|---|---|---|
| noaa-eri-pds | 4.5 / 1,586 / 0 | 4.6 / 1,592 / 0 | 9.5 / 2,655 / 1,044 | 29.8 / 1,634 / 52 |
| pansurg | 6.7 / 1,295 / 0 | 6.8 / 1,281 / 0 | 9.5 / 1,915 / 653 | 33.5 / 1,333 / 40 |
| fourcastnetgfs | 16.3 / 6,081 / 0 | 16.7 / 6,138 / 0 | 18.9 / 6,880 / 795 | 50.4 / 6,118 / 49 |
| nara | 56.0 / 19,683 / 0 | 57.2 / 19,638 / 0 | 62.8 / 19,339 / 74 | 96.0 / 19,612 / 37 |

- Unperturbed, the limiter costs nothing measurable: walls within the repetitions' spread, the limit never
  moves. A later parity re-run with the default build (2 repetitions) was within about 0.8 s of off.
- In the storm the limit falls from 64 to 1 (9 decreases) in under 0.1 s. The limiter sends 11–38 % fewer
  requests and draws 16–20x fewer throttle answers, at 1.5–3.5x the wall: recovery is a 10 s throttle-free
  window, then +1 per second. On nara, in-flight requests were back at 58 or more 68.1 s (67.7–68.5) after the
  storm ended.

**Sustained and cap** (wall s / requests / throttled; attempts/s):

| Recording | off, sustained | on, sustained | off, cap | on, cap |
|---|---|---|---|---|
| noaa-eri-pds | 6.8 / 2,006 / 400; 292 | 158.3 / 1,999 / 399; 12.7 | 13.9 / 2,606 / 1,014; 187 | 26.3 / 1,668 / 82; 63.5 |
| pansurg | 8.6 / 1,606 / 317; 186 | 202.9 / 1,584 / 314; 7.8 | 9.1 / 1,779 / 508; 194 | 29.5 / 1,319 / 23; 44.8 |

- **Sustained 20 %:** the limit spends the run at 1–2, about 23x the wall without the limiter. It leaves 1
  after each clean window (15 and 20 increases) and the next throttle takes it back (24 and 29 decreases).
  This is Java's gauge rule (x0.7 per throttle answer, growth only after 10 s without one), not a port
  defect; Java was not run under this fault.
- **Cap 200/s:** served rate is a quarter to a third of the cap (a half on fourcastnetgfs: 6,207 requests, 102
  throttled, 107 attempts/s). `--request-rate` is the fixed-rate alternative.
- Without the limiter both faults cost little on replay because replay answers a throttle instantly; real S3
  was not measured.

**Live, single run:** `noaa-hrrr-bdp-pds`, prefix `hrrr.202401`, anonymous, 64 workers, `--seed`, limiter on:
1,279,184 objects in 5.96 s, 1,683 attempts, no retry, no throttle. No live throttling has been observed at
64 workers.

**Reproduce:** build `bench/faultproxy`; arms file lines are `NAME<TAB>BINARY [diag list flags]<TAB>[proxy flags]`,
for example `on-storm<TAB>/path/swath<TAB>-window throttle:1s-6s:0.5`, `off-sus<TAB>/path/swath --limiter=false<TAB>-window throttle:0s-1h:0.2`,
`on-cap<TAB>/path/swath<TAB>-rate-cap 200`; then
`FAULTPROXY=… REPLAY_ENV=… bench/fault_replay.sh arms.tsv noaa-eri-pds 99 19300 3 out/fault`.

## GCS route parity

History: the merged port's build, before the engine campaign.

swath-replay serving one recording on both its S3 and GCS routes, the same latency on every request shape of
both (the recording's p50), Go `--seed` at the default workers of the time (12 on this host), 3 interleaved
repetitions (S3 then GCS), median (range):

| Recording | Rows | S3 wall s | GCS wall s | GCS / S3 | S3 requests | GCS requests | S3 overfetched keys | GCS overfetched keys |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| noaa-eri-pds | 845,693 | 11.90 (11.10–12.31) | 13.54 (11.69–13.72) | 1.14 | 1,127 | 1,173 | 232,750 | 0 |
| pansurg | 501,262 | 12.87 (11.74–13.21) | 12.74 (12.73–12.75) | 0.99 | 703 | 709 | 152,437 | 0 |
| fourcastnetgfs | 4,081,170 | 51.01 (48.77–58.37) | 51.53 (51.24–52.20) | 1.01 | 4,933 | 4,950 | 802,821 | 0 |

In all nine pairs the GCS dataset equals the S3 dataset (keys and sizes) and is ascending. GCS sends 0–4 % more
requests and overfetches nothing (`endOffset`). The noaa-eri-pds gap is two slow GCS repetitions on a shared
host; an earlier build on the same server took 11.67–11.77 s on GCS against 11.15–12.06 s on S3.

Live GCS, measured privately on one 5.1M-object bucket in-region (2026-11-06), by API and clock: Go on the JSON
API 29.5-32.4 s end to end; Go on the XML API 34.4-39.8 s end to end; the Java tool on the XML API 49-52 s end to
end, of which its listing phase 23-28 s. Both Go arms finish first end to end, though each takes longer than
Java's listing phase alone; mean in flight about 16 of 64 workers; split tree 398 deep where a balanced one needs 13.
That run used the build of the merged port, before the pivot policy and seed of the
[engine campaign](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms); see
[Known gaps](#known-gaps).

**Reproduce:** swath-replay with GCS latency injection (varveio/swath#250) serving `--protocols s3,gcs`; list
`s3://BUCKET` and `gs://BUCKET` through it alternately ([testing.md](testing.md#replay-oracle)).

## Recovery and part count

**Verification read rate** (resume reads every retained part and checks its digest; MD5 when measured,
SHA-256 since journal version 9): a 16 GiB, 4-vCPU VM, virtio disk, 1 GiB in 16 files of 64 MiB: 247, 306 and
376 MiB/s with the page cache dropped before each of three repetitions, 511 MiB/s warm, where MD5 on one core
was the limit. SHA-256 on one core of a Xeon Platinum 8581C (SHA extensions, 2026-11-08): 1,504 MiB/s, against
692 for MD5; a CPU without the SHA extensions hashes SHA-256 slower than MD5. Not remeasured end to end. 10,000 retained 64 MiB parts take roughly
30–45 minutes. Reproduce: `SWATH_VERIFY_DIR=DIR go test -run TestVerifyThroughput -v ./internal/listing`.

**Part-count growth** (synthetic GCS fake, 8 workers, one-row parts, one run each):

| Parts | Wall | Journal | Manifest | Peak Go heap | Heap retained after the run |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 1.39 s | 232,238 B | 252,569 B | 52.5 MiB | 1.2 MiB |
| 10,000 | 13.0 s | 2,338,240 B | 2,520,569 B | 92.1 MiB | 3.4 MiB |

About 233 and 252 bytes per part with 22-byte keys (near 3 KB per part with 1,024-byte keys, computed); live
heap about 1.2 KB per part. `TestPartCountGrowth` gates it on every run but `-short`; `SWATH_GROWTH=1` adds the
10,000-part run.

## Client CPU per object and binary size

**S3** (CPU profile of one sequential listing of a synthetic fake bucket: `bench/s3fake` serving the 971,927
keys of the `realistic` tree; "client" is samples under the client or `net/http`):

| | AWS SDK client | Hand-rolled client |
|---|---:|---:|
| Client CPU | 8.7 µs/object (73 % of process CPU) | **1.0 µs/object** (26 %) |
| of which response parsing | 8.5 µs/object | 0.61 µs/object |
| Process CPU (user + sys, median of 3) | 12.4 µs/object | 4.7 µs/object |

Reproduce: `SWATH_CPUPROFILE=cpu.out swath diag list --sequential s3://bench --endpoint URL --anonymous -o DIR`
against `go run ./bench/s3fake` ([testing.md](testing.md#the-fake-bucket)).

**GCS** (synthetic, 1,000,000 objects in an in-process fake, 1,000 per page, median of 3 interleaved; a
baseline that only reads the bodies is subtracted): ours 2.5 µs/object and 5,061 allocations per page against
Google's generated client 3.1 µs and 10,132. Reproduce:
`SWATH_CLIENT_COST=1000000 go test -run TestListClientCost -v ./internal/source/gcs/`.

**Binary size** (linux/amd64, `CGO_ENABLED=0 go build -trimpath -ldflags='-s -w'`):

| Build | Bytes |
|---|---:|
| full AWS SDK with its S3 service client | 24,617,122 |
| hand-rolled S3 client, SDK auth modules | 19,464,354 |
| the same with GCS (and protobuf 1.36.12) | **17,731,746** |
| with Google's generated GCS client linked instead | 26,124,450 |

The protobuf upgrade that came with GCS removed 3.0 MB stripped; GCS itself added 0.7 MB. Cross-builds:
[`../build.sh`](../build.sh).

### SDK-backed clients (2026-11-09)

Clients built with `-tags swath_sdkclient`, measured on branches `exp/measure-preview` (S3) and
`exp/gcs-sdkclient` (GCS). Synthetic inputs; the numbers describe those runs. Since then the SDK-backed clients
are the only clients ([clients.md](internals/clients.md)) and the build tag, `SWATH_S3_DECODER` and the
hand-rolled clients are gone; the numbers stand as measured, and the arms named "default" and "hand-rolled"
below describe the clients that were replaced.

**S3 decoders** (`BenchmarkDecode` in `internal/source/s3/sdkclient`: one full client call per iteration through
the real SDK stack and `objhttp.Guard` over an in-memory `RoundTripper`; a 1,000-entry page with URL-encoded keys,
owners and checksums; `-benchtime=2000x -count=3`; the range is the lowest to highest of the three):

| Decoder | List µs/object | List allocations/page | Versions µs/object | Versions allocations/page |
|---|---:|---:|---:|---:|
| scanner (production) | 1.66 to 1.92 | 392 | 1.93 to 2.11 | 392 |
| SDK decoder (`SWATH_S3_DECODER=sdk`) | 14.6 to 16.4 | 102,464 | not applicable (versions always use the scanner) | |
| scanner, body only | 1.14 to 1.21 | 3 | 1.12 to 1.28 | 3 |
| `encoding/xml` walk (deleted) | 12.5 to 15.1 | 80,497 | 11.7 to 12.3 | 85,187 |

A decoder on `encoding/xml` allocates per token and lands near the SDK's cost; a `RawToken` variant reached
8.5 to 9.3 µs per object.

**GCS** (`TestListClientCost`, 200,000 objects, `GOMAXPROCS=4`; a baseline that only reads the bodies is
subtracted):

| | Hand-rolled | Generated client, direct | `sdkclient` |
|---|---:|---:|---:|
| Client CPU over baseline, list loop | 2.37 µs/object | 2.96 | 3.11 |
| Allocations per page over baseline | 5,065 | 10,142 | 10,149 |
| Bytes per page over baseline | 1.03 MiB | 1.36 MiB | 1.65 MiB |

**Azure** (`BenchmarkList` in `internal/source/azblob/sdkclient`: one full `List` call per iteration, request,
bounded read, decoding, conversion and page validation, over an in-memory 5,000-entry page, the SDK client's Guard
in place; `-benchtime=500x -count=3` on a shared 4-CPU box, so the ratios are the result, not the absolute numbers;
a hierarchy page is half blobs and half prefixes):

| Page | Client | ns/object | Allocations/page | Bytes/page |
|---|---|---:|---:|---:|
| flat | hand-rolled | 16,700 to 17,900 | 710,127 | 36.2 MB |
| flat | `sdkclient` | 21,500 to 22,400 | 865,260 | 46.1 MB |
| hierarchy | hand-rolled | 9,800 to 9,950 | 397,632 | 20.9 MB |
| hierarchy | `sdkclient` | 12,300 to 13,300 | 485,276 | 25.0 MB |

Both clients decode with `encoding/xml`, so the SDK client costs 1.2 to 1.3 times the hand-rolled client per object
and 22 % more allocations; the envelope check is within noise. Stripped binaries on branch `exp/azblob-sdkclient`
(Azure only tagged): default 24,301,730 bytes, tagged 24,445,090, +143 KB and 15 packages (azcore and azidentity
are already linked in the default build; it links no `azblob` package at all).

**Binary size** (linux/amd64, `CGO_ENABLED=0 -trimpath -ldflags='-s -w'`, branch `exp/gcs-sdkclient`; GCS only
is tagged here):

| Build | Bytes |
|---|---:|
| default | 21,426,338 |
| `-tags swath_sdkclient` | 30,474,402 |

The tagged build is 9.0 MB larger and links 188 more packages, 64 of them under `google.golang.org/grpc` and 30
OpenTelemetry. The cost is inherent to the generated client: `google.golang.org/api/storage/v1` itself depends on
gRPC through `google.golang.org/api/internal/settings.go`, however the service is constructed. These sizes
are of a later tree than the table above, so they do not compare with it.

**Whole-process comparison (2026-11-09, commit 915e0c0 of `exp/measure-preview`, 4 vCPU, 975,077 keys shaped like the
`realistic` tree, medians; the fake server's CPU is not included).** Three S3 arms: the default build; the tagged build
with the scanner decoding (production); the tagged build with the SDK decoding current-object pages
(`SWATH_S3_DECODER=sdk`, measurement only). "Client CPU" is the share of profile samples with a frame in the client or
SDK packages or `net/http`.

Sequential `swath diag list --sequential` against `bench/s3fake` (976 pages):

| S3 arm | wall s | process CPU s | µs CPU/object | peak RSS MiB | client CPU µs/object | parsing µs/object |
|---|---:|---:|---:|---:|---:|---:|
| default, `-o` | 3.81 | 3.15 | 3.23 | 65 | 1.06 | 0.62 |
| tagged, scanner, `-o` | 3.83 | 3.72 | 3.81 | 71 | 1.27 | 0.52 |
| tagged, SDK decoder, `-o` | 8.78 | 8.79 | 9.01 | 69 | 7.09 | 6.49 (`encoding/xml` 5.68) |
| default, `count` | 4.13 | 3.18 | 3.26 | 27 | 1.11 | 0.75 |
| tagged, scanner, `count` | 4.52 | 4.21 | 4.31 | 35 | 1.34 | 0.65 |
| tagged, SDK decoder, `count` | 9.63 | 10.47 | 10.74 | 34 | 7.81 | 7.10 (`encoding/xml` 6.05) |

Sequential GCS against `scalefake` at zero latency:

| GCS arm | wall s | process CPU s | µs CPU/object | peak RSS MiB | client CPU µs/object | `encoding/json` µs/object |
|---|---:|---:|---:|---:|---:|---:|
| default, `-o` | 5.81 | 6.05 | 6.20 | 59 | 3.12 | 2.67 |
| tagged (generated client), `-o` | 5.83 | 6.59 | 6.76 | 68 | 4.75 | 3.85 |
| default, `count` | 5.57 | 5.20 | 5.33 | 24 | 3.87 | 3.27 |
| tagged, `count` | 6.89 | 7.49 | 7.68 | 34 | 4.65 | 3.58 |

Parallel `swath count`, 64 workers, `scalefake` at 20 ms latency (3 runs; no retries or throttles):

| arm | wall s median (min to max) | process CPU s | µs CPU/object | peak RSS MiB |
|---|---|---:|---:|---:|
| S3 default | 1.06 (0.92 to 1.41) | 1.53 | 1.57 | 85 |
| S3 tagged, scanner | 1.40 (1.16 to 1.71) | 2.53 | 2.60 | 92 |
| S3 tagged, SDK decoder | 3.58 (3.43 to 7.04) | 11.56 | 11.85 | 90 |
| GCS default | 2.09 (2.00 to 2.15) | 4.91 | 5.04 | 58 |
| GCS tagged | 2.24 (2.11 to 2.37) | 5.58 | 5.72 | 95 |

At 20 ms latency and 64 workers the SDK decoder makes the S3 listing CPU-bound on 4 cores: 11.6 CPU-seconds cannot
fit in the 1 s the network allows, so wall time triples. The scanner through the SDK costs about 0.6 to 1.0 µs more
process CPU per object than the hand-rolled client (the SDK's request machinery, about 0.6 µs of AWS frames, plus
`net/http` and GC), and its wall-time range overlaps the default's. The generated GCS client adds 0.5 to 2.3 µs per
object and 0.15 s of wall time here. Every arm produced byte-identical Parquet output. Stripped binaries at this
commit: default 21,434,530 bytes; tagged (both SDK service clients) 35,778,722 bytes, +205 packages and +25 modules.
S3 versions were not measured: gofakes3 answers the versions listing of an unversioned bucket with a truncated page
and no marker. Raw logs, profiles and scripts: swath-notes `runs/2026-11-09-sdk-clients/`.

## Local filesystem walker

Trees are synthetic (`bench/gen_tree.py`, seed 20261105): `realistic` is 971,927 non-directory entries and
99,892 directories (project trees, Hive partitions, home directories with spaces and non-ASCII names; sparse
files); `wide` is 1,000,000 empty files in 1,000 directories. Warm cache, 3 repetitions interleaved, median,
`/usr/bin/time`.

These were measured on the walker before the pipeline was restructured (the same `getdents64` and
dirfd-relative `fstatat` path; its `--engine`, `--fields` and `--order` flags no longer exist, and
`bench/run.sh` still drives them; `bench/compare.sh` uses the current flags). The current pipeline on the same tree, in one
interleaved session against that walker: stat count-only 1.81 s against 1.64 s and names 0.58 s against 0.51 s
(within that session's noise for stat), and a full run writing the dataset 1.77–1.78 s.

**Against find, fd and naive walks** (`realistic`, stat tier, 12 workers; `bench/run.sh warm`):

| Configuration | Wall s | User / sys s |
|---|---:|---:|
| walker, stat | **1.18** | 0.97 / 3.18 |
| walker, names only | **0.48** | 0.58 / 1.08 |
| `find -type f -printf '%s %T@ %p\n'` | 3.96 | 0.92 / 2.93 |
| `find -type f \| wc -l` (no stat) | 1.59 | 0.41 / 1.19 |
| `fdfind --type f --hidden --no-ignore` (no stat) | 0.92 | 1.07 / 1.71 |
| Go `filepath.WalkDir` + `os.Lstat` | 6.30 | 2.00 / 4.19 |
| Python `os.walk` + `os.lstat` | 5.80 | 1.50 / 4.01 |
| full run, sorted Parquet, 4 encoders | 2.36 | 3.31 / 3.72 |

The win is parallelism plus dirfd-relative stat: single-threaded the walker (3.57 s) is close to
`find -printf`, and the same parallel walker with full-path `os.Lstat` takes 2.01 s. Guest-cold (single runs,
`drop_caches` before each; the host may still cache the disk image): walker 5.24 s against `find -printf`
19.99 s.

**Against disk-usage tools** (a later, slower session: compare only within this table; `bench/compare.sh`;
output to `/dev/null`):

| Tool | `realistic` wall s | user / sys s | RSS MiB | `wide` wall s |
|---|---:|---:|---:|---:|
| `pdu` 0.24.0 | 2.26 | 0.73 / 8.19 | 4 | 0.72 |
| `diskus` 0.9.0 | 2.45 | 0.87 / 8.69 | 7 | 0.75 |
| `dust` 1.2.7 | 3.10 | 1.74 / 9.10 | 377 | 1.15 |
| `gdu` | 3.57 | 1.90 / 9.23 | 26 | 1.22 |
| `dua` 3.0.0 | 4.03 | 2.12 / 9.94 | 11 | 3.86 |
| `fdfind` with a time filter (stat) | 2.83 | 1.32 / 9.13 | 8 | 1.02 |
| `fdfind` (no stat) | 0.96 | 1.05 / 1.75 | 9 | 0.34 |
| `find -printf` (1 thread) | 6.48 | 0.58 / 5.46 | 3 | 2.38 |
| walker, names, count only | **0.66** | 0.67 / 1.58 | 42 | **0.17** |
| walker, stat, count only | 2.31 | 1.19 / 7.00 | 45 | 0.78 |
| walker, full run to sorted Parquet | 2.94 | 3.01 / 6.99 | 123 | 1.21 |

The names tier is the fastest lister measured; the stat tier ties the fastest Rust disk-usage tools and spends
less kernel time than every full-path stat tool. The disk-usage tools only sum sizes; the walker builds a full
row per entry. Not established: cold-cache behavior of these tools, hard-link de-duplication, network or
overlay filesystems.

### Against fd at equal output, huge directories, cold count (2026-11-08)

**Synthetic, local.** 4-vCPU, 15 GiB VM, ext4 on virtio; trees from `bench/gen_tree.py` and FTZZ 4.0.0
(`ftzz -n 10M -r 100 -d 5 --files-exact`: 10,000,000 files in 131,492 directories); hyperfine, fd 9.0.0,
bfs 3.1.2. Absolute times on this VM drift between sessions; compare within a table only.

**fd at equal output.** With fd's paths sent to `/dev/null` (not counted through a pipe, which our first pass
did and which made the walker look 2x faster), warm, the two are even: `wide`, names 262 ms (fd 297, `-j 16`
232), stat 728 ms (fd 861, `-j 16` 694); `ftzz-10m`, names 2.39 s (fd 3.15, `-j 16` 2.44, bfs 3.88, find
4.13). Both make one `getdents64` per buffer and one stat per entry (`strace -c`); fd stats by absolute path,
the walker relative to the directory fd. Warm, about 83 % of the walker's CPU is `fstatat` in the kernel.

**This change** (`localfs`: pointer-free entries, prefix-skipping and parallel sort, presized buffers; a
stat-tier count at 4 processors per CPU):

| | before (`d9d8ee1`) | after | fd |
|---|---:|---:|---:|
| `huge` (200,000 files in one directory), warm, `count`, 15 runs | 324–332 ms | **189 ms** | 264 ms (`-j 16`: 162) |
| `huge`, warm, `list -o` (sorted Parquet), 15 runs | 488 ms | **309 ms** | – |
| `realistic`, cold (`drop_caches` before each), `count`, 3 runs | 3.89 s | **2.92 s** | 4.39 s (`-j 64`: 2.83) |
| `wide`, warm, `count`, 20 runs | 642 ms | 590 ms | – |
| `deep`, warm, `count`, 20 runs | 285 ms | 293 ms | – |
| `realistic`, warm, `count`, 20 runs | 1.51 s | 1.53 s | – |
| `ftzz-10m`, `count`, 2 runs | 77.0 ± 14.1 s | 71.4 ± 4.5 s | `-j 64`: 79.4 ± 12.3 s |

Before the change the huge directory's read and sort ran on one goroutine ahead of its stats (profile: sort
~100 ms, slice growth ~70 ms of 325). Cold, the walker used about 2.8 of 4 CPUs at any `--workers` (16, 64 and
256 all 3.7–3.8 s) and fd `-j 64` about 3.8: a stat waiting on the disk holds its Go processor, and GOMAXPROCS
8 gave 3.17 s, 16 gave 3.07 s. The warm rows of ordinary trees are within noise. The 10M tree does not fit
this VM's inode cache (`ext4_inode_cache` 9.1M objects, 9.8 GiB; page cache 0.2 GiB): every tool spends
265–290 s of kernel time in 70–80 s of wall, CPU-bound on reclaim, and the arms are within noise of each
other. The `huge` Parquet output is identical to the baseline's in content and order.

**Names tier, and a paired re-check** (2026-11-09). A directory read that waits on the disk holds a Go processor as a
stat does: cold, a names-tier count took 2.18 s at GOMAXPROCS 4, 1.59 s at 8 and 1.46 s at 16 on `realistic`
(Chromium 119 with submodules, 1.93M files: 4.29 / 3.42 / 3.03 s); warm, 8 was no slower and 16 up to 8 % slower.
A names-tier count now runs at 2 processors per CPU. The same change stops `sortEntries` reading GOMAXPROCS (which
takes the scheduler lock) for every directory. Paired and interleaved runs (each round runs every arm in rotating
order; the median of the per-round differences against the baseline `d9d8ee1`, with a 95 % bootstrap interval;
the box was shared with a build, so absolute times are higher than above):

| names tier unless noted | rounds | change against `d9d8ee1` |
|---|---:|---:|
| Chromium, cold | 4 | −21.5 % [−30.5, −3.0] (fd `-j 64`: −52.9 %) |
| `realistic`, cold | 4 | −19.1 % [−25.0, −15.4] |
| Chromium, warm | 15 | −6.8 % [−21.7, −1.3] |
| `realistic`, warm | 10 | −0.5 % [−10.4, +9.9] |
| Chromium, stat tier, warm | 10 | −3.6 % [−9.0, +1.4] |

Cold names-tier counts remain about 2x behind fd `-j 64`.

**A count does not sort** (2026-11-09; dev box, 4 vCPU, 7 GiB, ext4 on a 200 GB persistent disk). Each directory's
sort is skipped in a count (`order.Options.Unordered`); paired and interleaved against #339's head `3f9c894`:

| count | rounds | change |
|---|---:|---:|
| Chromium 119 (1.93M files), names, warm | 10 | −15.4 % [−16.8, −14.1] (0.805 → 0.679 s) |
| Chromium 119, stat, warm | 10 | −6.5 % [−7.2, −6.0] (1.566 → 1.468 s) |
| `realistic` (~1M entries), names, warm | 15 | −10.6 % [−13.7, −9.1] (0.396 → 0.354 s) |
| `realistic`, stat, warm | 15 | −5.5 % [−7.0, −3.7] (0.761 → 0.717 s) |
| `huge` (200,000 in one directory), stat, warm | 15 | −19.7 % [−21.7, −19.0] (0.152 → 0.121 s) |
| `realistic` and Chromium, both tiers, cold | 4 | within ±1 % (`realistic` ~25.5 s, Chromium ~50 s) |

Cold, this box's disk caps the walk (a persistent disk's IOPS limit: ten times the local-SSD VM's cold times
above), so CPU saved does not show. Warm, the remaining gap to fdu (stat tier) and bfs (names tier) is in user
space: kernel CPU and syscall counts are the same (`strace -c`), and after this change syscalls are about
65 % of a names-tier count's CPU, the rest spread over row emission, keys and allocation at a few percent each.

**NFS: READDIRPLUS hints** (2026-11-09; same box, Linux 7.0 client, loopback NFS 4.2 over an ext4 image in a
tmpfs). A cold count of
a 200,000-file directory made 199,722 LOOKUP RPCs (`nfsstat -c`): the Linux client fetches attributes with the
names only on a directory's first read unless more than 8 of its entries were looked up between reads, and swath
read the whole directory before any stat. Statting 9 entries after each full read keeps READDIRPLUS on: no
LOOKUPs, 95 READDIRs. Paired, 6 cold rounds: 5.85 s to 2.42 s, −57.9 % [−64.3, −49.3] (fdu 2.73 s, which stats
each batch as it reads). Local ext4, the same directory warm, 20 rounds: −6.3 % [−19.0, +1.7], no cost. The box
was loaded by other builds during these runs; the RPC counts do not depend on load.

## Memory behind a held-up frontier

**Synthetic** (fixture: 3,000,000 goes-like keys of about 100 bytes served as ListObjectsV2 XML by a keyfake
server, `slowfake`, whose requests starting in the first 5,000 keys wait 15 s and the rest 20 ms; 64 workers,
the default 128 MiB `--row-budget`, `--seed=false`; one run per arm on a shared 4-core, 7 GiB host). Every
range after the first lists, splits and waits ahead of the frontier. "Before" is the parent of the change
that copies retained bounds and adds `order.Count`:

| Run | Peak RSS | Peak live heap | Peak nodes | Spill |
|---|---:|---:|---:|---:|
| `--count-only`, before (through the order stage) | 797 MiB | – | 4,827 | 392 MB |
| `--count-only`, cursor copied, still ordered | 338 MiB | – | 4,799 | 392 MB |
| `--count-only`, after (`order.Count`) | **35 MiB** | 8 MiB | 16 | none |
| `-o keys.tsv` (ordered), before | 808 MiB | – | 4,829 | 392 MB |
| `-o keys.tsv` (ordered), after | **333 MiB** | 157 MiB | 4,815 | 392 MB |

The TSV is byte-identical before and after, and every count is 3,000,000. With the seed on (the default) the
count-only run peaked at 438 MiB before and 97 MiB after. The ordered run's remaining RSS is the budget's rows
(157 MiB live against the 128 MiB budget) and the garbage collector's headroom. The in-repository guard is
`TestRetainedBoundsDoNotPinPages` (synthetic: `scalefake`, 400,000 keys, [ordering](internals/ordering.md#what-a-waiting-range-retains)).

## Text sink, 2026-11-07

Local fake (keyfake, 50 ms a page), the first 2.5M keys of aws-public-blockchain, 64 workers, 4 vCPU,
`-o out.tsv.zst`; wall s, two quiet runs each (a third round on a shared, loaded box ran 1.5 to 2 times
slower across all arms and is not used). The fake's rows compress 35x (one ETag for every object), so
compression here is cheap and formatting dominates.

| Arm | Wall | CPU | RSS MiB | Spill MiB | Mean active | Sink wait s |
|---|---|---|---|---|---|---|
| Parquet `-o out` | 5.0, 5.3 | 9.3 | 410-430 | 210-230 | 41 | - |
| text before (spill under `/tmp`, ext4 here) | 5.9, 6.0 | 9.1 | 430-450 | 220-245 | 33-35 | - |
| text before, `TMPDIR` beside the output | 5.9, 6.0 | 9.1 | 440 | 217-235 | 34 | - |
| placement + 4 parallel zstd jobs | 5.6, 5.7 | 8.8 | 440 | 227-239 | 35-36 | 1.8 |
| + run-at-a-time escaper (this change) | 5.2, 5.2 | 8.3 | 450-460 | 230 | 38 | 1.2-1.3 |
| this change, `--encoders 1` (synchronous) | 5.7, 5.8 | 8.3 | 400 | 215-245 | 36 | 1.5 (0.5 output) |

Placement does not move the local wall (`/tmp` is the same ext4 here); in the study container `/tmp` was
the overlay filesystem. Before the change the order stage spent 1.68 s of a 6.1 s run in the sink, 1.56 s
of it formatting rows (`appendEscaped` alone 0.95 s, byte by byte). Every decompressed output equals the
uncompressed TSV byte for byte.

The compressor alone (klauspost v1.19.1, level default, a 400 MB TSV of the same keys with per-object ETags,
sizes and times: ratio 4.4, as the blockchain's 4.26 GB output suggests): one synchronous stream 249 MB/s;
the old default (concurrency GOMAXPROCS without concurrent blocks: one block overlapped) 283 MB/s; 4 jobs
at the 8 MiB window 459 MB/s and 449 MiB allocated; 4 jobs at a 1 MiB window 492 MB/s, 70 MiB, the same
ratio (4.42). `BenchmarkSink` (the order stage's cost, rows to a `.tsv.zst` file): 135 MB/s with one
synchronous encoder, 205 MB/s with 4 jobs.

On Batch (runs `tsinka` and `tsinkb`, 2026-11-07; `7b05605` before, `d63e04ca` after; the three largest
lists, 130-145M keys, n4-highcpu-16, 256 workers, `GOMAXPROCS=8`, fake at 50 ms): the change above took
`.tsv.zst` from 216/115/151 s to 167/105/140 s (Parquet on `d63e04ca` 129/74/99 s), with
`output.text.sink_wait_ns` still 64/51/63 s: one formatting stream. Parallel formatters (`--encoders`
goroutines, one ordered stream), on the local fake above, box shared and loaded, three interleaved rounds:

| Workers | Before (7b05605) | One formatter (f4653660) | 4 formatters | Sink wait s: one / four |
|---|---|---|---|---|
| 64 | 9.96, 7.93, 8.16 | 7.17, 7.95, 7.35 | 7.27, 6.88, 6.75 | 1.9-2.4 / 0.54-0.77 |
| 256 | 9.74, 9.43, 8.41 | 9.83, 9.39, 8.63 | 9.30, 7.23, 6.91 | 3.0-3.8 / 0.58-1.17 |

Mean active at 256 workers rose from 105-114 (before) to 132-137. The output equals the serial path's byte
for byte.

**On Batch** (run `tsinkc`, 2026-11-07; `836cb2e6`, parallel zstd and parallel formatters; the three largest
lists, 256 workers, 8 cores, fake at 50 ms, `-o out.tsv.zst`, sorted Parquet `-o out` on the same VM as the
control):

| List | Keys | `.tsv.zst`, `7b05605` | parallel zstd only | + parallel formatters | sorted Parquet, same binary | Sink wait s, last arm |
|---|---:|---:|---:|---:|---:|---:|
| noaa-goes19 | 144.8M | 216 s | 167 s | **111 s** | 126 s | 3 |
| chalmerscloudiceclimatology | 139.1M | 115 s | 105 s | **61 s** | 76 s | 1 |
| deafrica-sentinel-5p-cloud | 129.8M | 151 s | 140 s | **86 s** | 96 s | 2 |

A `.tsv.zst` file is now faster than sorted Parquet on the same engine (no Parquet encoding, no parts), mean
active 112–150 of 256. The study harness's zstd TSV row ([above](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms))
ran before this change and has not been rerun.

## Delta-coded spill frames

**Synthetic, frame layer** (`BenchmarkSpillFormats`: 50 frames of 4,096 S3 rows written to a spill file and
replayed, the keys 204,800 consecutive keys of a prod list, the payload what a real bucket lists: a random MD5
ETag per row, sizes over decades, times a few seconds apart, `STANDARD`, a checksum; page cache, 4-core host,
10 iterations). CPU is a write plus a replay per row:

| Keys | Arm | Bytes/row | CPU ns/row | Allocated B/row |
|---|---|---:|---:|---:|
| `aws-public-blockchain` (103 B/key) | plain | 181.7 | 322 | 331 |
| | plain+zstd | 40.0 | 1,069 | 456 |
| | delta | 54.4 | 313 | 284 |
| | **delta+zstd** (default) | **30.3** | 657 | 354 |
| `idc-open-data` (77 B/key) | plain | 155.5 | 277 | 300 |
| | plain+zstd | 55.2 | 1,302 | 436 |
| | delta | 65.5 | 254 | 249 |
| | **delta+zstd** | **53.2** | 733 | 306 |
| `nara-1950-census` (80 B/key) | plain | 158.1 | 324 | 303 |
| | plain+zstd | 36.3 | 911 | 401 |
| | delta | 32.7 | 253 | 257 |
| | **delta+zstd** | **25.2** | 542 | 325 |

Delta coding costs no CPU against plain frames (it writes and reads a third to a fifth of the bytes), and
delta+zstd takes 55-60 % of plain+zstd's CPU for fewer bytes: zstd's input is already a few times smaller. What zstd still finds after delta is in the key suffixes (the ETags' 16 raw
bytes are incompressible): 24 B/row on blockchain, 7 on nara, 12 on idc, whose suffixes are random UIDs.
Against plain frames, the production default before this change, delta+zstd saves 100 to 150 B/row of
disk traffic for 220 to 460 ns/row of CPU; delta alone saves 90 to 125 B/row for none. At scale and on one
core, below.

**Synthetic, end to end** (keyfake: the first 2,500,000 keys of `aws-public-blockchain`, 50 ms per request,
64 workers, sorted Parquet, `--row-budget 128KiB` so nearly every row spills; the fake's payload is constant
(one ETag, time, size), so its payload bytes are an underestimate for every arm; one or two runs per arm on
the shared host). "Before" is `go/engine-campaign` at 7b05605:

| Arm | Spilled rows | Spill bytes | B/row | Peak spill | CPU s | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| before, plain | 2.21-2.27 M | 325-334 MB | 147 | 217-226 MB | 9.5-10.0 | 111-125 MiB |
| before, `--spill-compress` | 2.20-2.26 M | 9.6-9.7 MB | 4.3 | 6.4-6.6 MB | 9.9-10.3 | 198-228 MiB |
| plain (`--spill-plain --spill-compress=false`) | 2.24-2.35 M | 330-347 MB | 147 | 218-221 MB | 8.9-9.4 | 108-119 MiB |
| plain+zstd (`--spill-plain`) | 2.21-2.26 M | 9.6-9.7 MB | 4.3 | 6.2-6.4 MB | 8.8-9.2 | 166-179 MiB |
| delta (`--spill-compress=false`) | 2.24-2.27 M | 58-59 MB | 26 | 38-40 MB | 8.7-9.0 | 110-117 MiB |
| **delta+zstd** (default) | 2.17-2.30 M | 6.5-6.6 MB | 2.9 | 4.5 MB | 8.9-9.2 | 128-142 MiB |

Wall time (12-24 s, against 203 s with `--no-spill`) is the fake's latency and the frontier, noise at this
size. The Parquet parts of every arm, with `--part-interval 0`, are byte-identical to the `--no-spill` run's.
The plain+zstd arm's RSS is its fresh decode buffer per frame; delta frames reuse theirs.

**On Batch** (runs `delta16b` and `delta1b`, 2026-11-07; `bf5e6d52`; the three largest lists, fake at 50 ms,
sorted Parquet, output identical in every arm; spill written in GB beside the wall). 16 cores is n4-highcpu-16 at
64 workers; one core is n4-highcpu-2 with `GOMAXPROCS=1`, beside the Java CLI pinned to one core as above. The
fake's payload is constant, so the zstd ratio flatters every arm; the delta step's 4–16x is the part real
payloads keep.

| List | Cores | Java | plain | delta | **delta+zstd** (default) |
|---|---|---:|---:|---:|---:|
| noaa-goes19 | 16 | – | 179 s · 22.2 | 167 s · 6.2 | 167 s · 0.59 |
| | 1 | 515 s | – | 347 s · 6.2 | **328 s** · 0.59 |
| chalmerscloudiceclimatology | 16 | – | 151 s · 16.0 | 147 s · 1.0 | 147 s · 0.08 |
| | 1 | 362 s | 329 s · 16.0 | 250 s · 1.0 | **253 s** · 0.08 |
| deafrica-sentinel-5p-cloud | 16 | – | 115 s · 20.3 | 113 s · 2.6 | 114 s · 0.06 |
| | 1 | 446 s | 351 s · 20.4 | 254 s · 2.6 | **246 s** · 0.06 |

At 16 cores neither step moves the wall. On one core delta coding takes 24–28 % off against plain frames on the
same VM and zstd on top costs nothing measurable, so delta+zstd stays the default; Go on one core is 36–45 %
ahead of Java pinned to one core. The plain arm of noaa-goes19 at one core did not run.

## Durable spill

**Fixtures.** One MinIO bucket and three production key lists. Every arm resumes through the real binary
(`cmd/swath` `TestDurableMinIO` and `TestDurableCorpus`; the schedule and the assertions are described in
[testing.md](testing.md#durable-spill-campaign-opt-in), with the commands that reproduce this table).

- **MinIO.** `RELEASE.2026-10-23T20-06-51Z`, the Chainguard image pinned in `bench/minio.sh`: a single drive
  on a tmpfs, 3 GiB, heap target 1 GiB. The bucket holds the first 250,000 keys of `noaa-nbm-grib2-pds`
  (date paths), filled by `bench/s3fill`. The runs are signed and use 32 workers. At 16 workers the captured
  ranges were still open at the 40 % kill, so that arm had no certificate; at 64, MinIO outgrew its container.
- **Corpus.** The first 2,000,000 keys of `real-changesets` (flat numeric names), `noaa-nbm-grib2-pds` (deep
  date paths) and `argoverse-6M` (long keys, about 130 B), each served by the private key-list fake at
  5 ms a page through `bench/durable_corpus.sh` (`MAX_KEYS=2000000`). The runs are anonymous and use the
  default workers.
- **Skew.** Pages below the key at 10 % of the list wait 200 ms more.
- **Flags.** `--row-budget 1MiB --spill-budget 256MiB --spill-reserve 64MiB`. Parts are sized to give about 16
  per dataset, 58 KiB to 1 MiB.
- **Build.** `go/p2-durable-spill` at `22fe7cda` for the kill arms. The suspect-composition arm's figures are
  from its rerun at `c0ffc6cf`, which checks replay provenance and logs every resume of the arm. 2026-11-08, 4 shared
  vCPU.
- **Oracle.** For every fixture, the uninterrupted reference lists exactly its key file.

**Result: 92 of 92 arms pass.** That is 11 kill schedules at 2 intervals, plus the suspect composition, on 4
fixtures. Every arm checks the following:

- With `--part-interval=0`, every resumed dataset is byte-identical to the reference. With a 1 s interval it
  is row-identical in every column.
- Rows reconcile in every resume.
- No certificate is rejected.
- Every resume is checked against the request log, and none of the logged data requests started in
  `[max(cs, K), ce)` of a certificate live in the ledger's prefix when that resume started. In the kill arms
  the resumes logged 52,222 data requests: 47,548 from the completing resumes and 4,674 from the killed first
  resumes of the mid-replay arm. The suspect-composition arm's resumes are counted in its own table below.
  Delimiter probes and continuations are not data requests and are not checked.
- Every reuse arm (the unlink, mid-replay, 40 % and 70 % kills) found certificates above K, and its resume
  accepted and replayed some.

Each cell below is the resume's requests over the cold run's, with the accepted certificates in brackets.
"No seal yet" marks a kill that, by design, falls before the first certificate is recorded. The cold run made
386 requests on MinIO, 2,112 on `real-changesets`, 2,223 on `noaa-nbm-grib2-pds` and 2,379 on `argoverse-6M`.

| Kill | MinIO, interval 0 | MinIO, 1 s | changesets, 0 | changesets, 1 s | nbm, 0 | nbm, 1 s | argoverse, 0 | argoverse, 1 s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| capture frames #1 | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet |
| seal #1 | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet |
| ledger append #1 | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet | no seal yet |
| ledger sync #1 | 0.99 (1) | 0.85 (1) | 1.00 (1) | 0.99 (2) | 1.00 (1) | 0.99 (1) | 1.00 (1) | 1.00 (1) |
| unlink #1 | 0.76 (27) | 0.81 (14) | 0.48 (254) | 0.55 (254) | 0.11 (255) | 0.14 (254) | 0.15 (254) | 0.14 (255) |
| after part record #2 | 0.80 (25) | 0.85 (6) | 0.46 (246) | 0.57 (248) | 0.10 (247) | 0.14 (256) | 0.60 (256) | 0.68 (256) |
| mid recovery replay (ledger sync #3, then replay frame #2) | 0.81 (25) | 0.75 (17) | 0.72 (255) | 0.72 (256) | 0.05 (235) | 0.10 (246) | 0.14 (254) | 0.14 (255) |
| lost unsealed tail (seal #2) | 0.99 (1) | 0.87 (1) | 0.99 (1) | 1.00 (1) | 1.00 (1) | 0.99 (1) | 1.00 (1) | 1.01 (1) |
| torn ledger append (sync #2) | 0.99 (1) | 0.98 (2) | 0.98 (55) | 0.97 (56) | 0.93 (14) | 0.93 (15) | 0.96 (25) | 0.95 (28) |
| 40 % of keys | 0.85 (23) | 0.82 (17) | 0.80 (188) | 0.78 (205) | 0.77 (42) | 0.76 (43) | 0.75 (158) | 0.76 (149) |
| 70 % of keys | 0.68 (7) | 0.70 (5) | 0.68 (234) | 0.68 (246) | 0.54 (83) | 0.56 (79) | 0.59 (256) | 0.69 (256) |

Savings follow the certificates that are live at the kill.

Request counts depend on scheduling and vary from run to run. In this run the MinIO resumes after the unlink
and mid-replay kills needed 0.75-0.81x the cold run's requests, against 0.07-0.30x in the first campaign at
16 workers. More workers seal more and smaller certificates, earlier, and K had advanced further by the first
unlink.

The kill percentages count `<Key>` elements returned to the binary before their delayed delivery. Overfetch
and probe keys are included, so a percentage approximates the keys served, not distinct keys processed.

**Suspect composition** (§6 "time cuts on", `--part-interval 3s`, 64 MiB parts; rerun at `c0ffc6cf`,
2026-11-08). On every fixture a part closed by time held rows from several certificates' intervals and rows
listed live between them. It was claimed, and the run was killed before its record. The next resume then:

- rejected no certificate;
- accepted every certificate live above K, the part's among them;
- replayed at least the part's certificate rows, so those rows are replay's;
- reproduced the claimed file at its ordinal byte for byte. This is the suspect-part invariant, not identity
  with a cold run's part, which only the interval-0 arms assert.

Every try and the final resume were checked against the request log, and no logged request started inside a
live certificate. With reuse off (`--spill-budget 1MiB` on the arm's resumes) the arm fails.

| Fixture | Killed at | Part | Certificates in it | Live rows between them | Bytes | Accepted | Rows replayed | Resumes (data requests logged) |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| MinIO | journal append #7 | 7 | 3 | 20,171 | 825,207 | 120 | 203,796 | 8 (825) |
| `real-changesets` | journal append #1 | 0 | 6 | 324,041 | 2,960,064 | 256 | 453,476 | 2 (2,216) |
| `noaa-nbm-grib2-pds` | journal append #1 | 0 | 6 | 362,875 | 4,103,476 | 256 | 1,759,471 | 2 (828) |
| `argoverse-6M` | journal append #2 | 1 | 6 | 172,709 | 9,459,146 | 254 | 911,631 | 3 (2,425) |

**Full scale (GCP Batch).** Runs `dur1` (build `79bafbc6`) and `dur2` (`7af71ece`) listed 64 production
lists of 20 to 150 million keys. Each list ran cold and was SIGKILLed at 40 % and at 70 %, then resumed.
In both runs all 64 resumed datasets were byte-identical. The resumes took a median of 0.66x the cold run's
requests after the 40 % kill (p90 0.99) and 0.22x after the 70 % kill (p90 0.43).

**A MinIO listing anomaly.** A first MinIO fixture held the first 250,000 keys of `nara-1950-census`. On it,
this MinIO answered every `start-after` inside `1950census/` with an empty page marked not truncated, even
when the `start-after` was a stored key. `1950census/` sorts after its siblings `1950census-descriptions/`
and `1950census-maps/`. A sequential continuation-token listing returned all 250,000 keys in order. swath
splits ranges by `start-after`, so it listed 107,651 keys and lost the 142,349 under `1950census/` without
an error, identically on every run. A three-key bucket of the same shape did not reproduce it.

The key-file oracle is what caught the loss. A review of swath's requests found no construction fault: a
`start-after` is sent only without a continuation token, data queries carry no delimiter, and values are
percent-encoded once, slashes included. `bench/minio_start_after.sh` reproduces the empty page with
curl-signed requests alone, no swath and no SDK (re-run 2026-11-08 against the pinned image). A prefix
query lists `1950census/43290879-Alabama/…-0001.jpg` first, yet `start-after=1950census/` and
`start-after=` that stored key both return `KeyCount=0 IsTruncated=false`.

swath trusts a page that is not truncated as the end of its range
([range-engine.md](internals/range-engine.md)), so the campaign's MinIO fixture
is a list without that shape.

## Drain tail: read-ahead and segments, 2026-11-07/08

Batch, 64 lists of 20M keys or more, 256 workers, sorted Parquet (`pq256`) and TSV, n4-highcpu-16 unless said;
ratios are wall against a binary run the same night (median · p90); every run's datasets byte-identical.

| Run | Against | pq256 wall | tsv | Notes |
|---|---|---|---|---|
| segments at 8 vs 15 workers (16 vCPU) | the serial build | 0.75× (8) vs 0.81× (15) | – | 8 workers chosen (`minSegmentWorkers`, one per two processors) |
| sentinel-cogs, 1.09B keys, 32 cores | 8 workers | 31 workers: −10 s of a 140 s tail | – | RSS 6.3 vs 3.4 GB; tail split 1,724 s writing, 714 s reading spill, 621 s decoding (≈68 % of 32 cores): CPU-bound |
| `seg3` (a9c1e57f: the reservation counting the tree) | `seg3b` (3b057e78) | tail 1.10× median, up to 3× | – | 46 of 64 lists waited (`budget_waits` up to 46,528; logan-pub 15.5 → 45.4 s) |
| `seg4` (eb826e5d: workers sized at plan time) | `seg4b` (3b057e78) | 1.01 · 1.09 (max 1.22) | 1.01 · 1.06 | `budget_waits` 0 on 64/64; the largest tails 10–25 % slower: encoders at start 5 → 3 (P2-E6) |
| `seg5` (f6fee172: zstd lower-memory states, 4 encoders at start) | `seg5b` (3b057e78) | 1.03 · 1.16 | 1.02 · 1.15 | tail median 1.05×; A/A of 3b057e78: 1.00 · 1.06; `--encoders 5` slower |
| `seg5` | `7b05605` (before phase 2) | 0.76 · 0.92 | – | |
| `seg6` (c57d8825, main merged) | `seg6b` (main 9ad554b1) | 0.89 · 1.04 (max 1.16) | 0.98 · 1.06 | RSS median 649 MB, max 1.1 GB |
| `seg6all` (251 lists, 64 workers, 50 ms) | the PR-1 gates | every gate holds | – | wall/ideal 1.17, p90 1.52; 251/251 identical, resume@40 too |

Read-ahead: on one core it took turns with the drainer (the three largest lists 5 to 11 % slower with it), so it is
off at GOMAXPROCS 1. Locally (2M keys, 4 cores) it read about 1,200 of 1,230 frames ahead without moving the
wall: the tail was encoder-bound, which the segments then addressed. It stops when segments begin, so it
serves the tail only while they cannot (one or two processors, one encoder, a single part, `listing_active`);
no run measures the wall with segments on and the read-ahead off.

Plan-time pricing (`segmentShare`, `budgetWorkers`): `seg3` had the reservation gate alone, and that gate
counted the tree; a95c1b12 priced the workers at plan time and took the tree out of the gate in one step, and
`seg4` then waited 0 times on 64 of 64 lists. The two changes are not separated on Batch. Locally the gate alone
(`TestSegmentsStayWithinBudget`, the share understated) holds the bound with waits (15 in one run) where the priced plan
(`TestSegmentsSizedToBudget`) has none; what that costs in wall is unmeasured.

Follow-up (open, not run): on the 64 lists, segments on with the read-ahead off; and the fixed gate with a
fixed worker count (pricing off) against `seg6`.

## Smoke runs only

These prove a harness end to end. They are single runs on a loaded host and **not evidence**; the measured
runs belong to a campaign on dedicated infrastructure.

**Scale tier** (synthetic: `internal/source/scalefake`, shape `32,32,32,8`, skew 1, seed 1; 30 ms + U[0, 20) ms
latency; 16 workers; shipped defaults plus `--seed`; `--memory` 256 MiB, the row budget's predecessor; one run
each; `bench/scale_run.sh OUTDIR KEYS WORKERS PORT`, `bench/scale_table.py OUTDIR`):

| Run | Rows | Wall s | Peak RSS MiB | Requests per 1k keys | Mean in flight | Output B per row |
|---|---:|---:|---:|---:|---:|---:|
| natural keys | 2,000,000 | 24.4 | 334 | 1.118 | 3.9 | 29.3 |
| 1 KiB keys (output on tmpfs) | 200,000 | 5.3 | 602 | 1.575 | 7.3 | 36.3 |

The natural-key run had a nearly full disk, so the spill cap was 1 byte and listing waited on the row budget
(mean 3.9 requests in flight of 16); the tmpfs run did not. CPU was 5.9 and 10.6 µs per row.

**Ordered emission against an external sort** (`bench/sortab.sh`, `bench/sortab_table.py`; synthetic trace
`slowfirst`, 3,000,000 keys in 300 ranges, 8 workers, 256 MiB, part 8 MiB, spill cap 64 MiB, tmpfs, one rep):

| Kill | Arm | Wall s | CPU µs/row | Requests | Relisted keys | Peak RSS MiB | Peak spill or runs MiB |
|---|---|---:|---:|---:|---:|---:|---:|
| none | A (the command's pipeline) | 41.3 | 2.01 | 3,050 | – | 295 | 64 |
| none | B (external sort) | 36.0 | 2.38 | 3,050 | – | 218 | 219 |
| at the slow range's half | A | 56.0 | 2.51 | 4,656 | 1,580,282 | 297 | 64 |
| at the slow range's half | B | 38.1 | 2.37 | 3,271 | 217,326 | 216 | 219 |

At the kill A had journaled no part (the slow first range had not filled part 0), so it relisted everything; B
had 1.77 M rows in checkpointed runs, which hold the whole listing and cannot be capped. The relisted keys
(1,580,282 for A against 217,326 for B) illustrate the mechanism (ordered emission relists the whole
speculative suffix above the durable frontier, [Known gaps](#known-gaps)); one synthetic trace and one rep, they
are not a representative ratio. The gate evaluates as designed; the deciding runs need sizes past RAM, 64–512
workers and real disk.

**Resume into a storm** (`bench/resume_storm.sh`, `bench/resume_storm_table.py`) and **defaults**
(`bench/defaults_replay.sh`, `bench/defaults_table.py`: the default argv against `--seed --workers 64` and Java
defaults, with smaller hosts emulated) have one smoke run each and no numbers to quote.

## Known gaps

- **Local filesystem.** No `--one-file-system`: the walk crosses mount points. Symlinks are not followed, but a
  bind-mount cycle is not detected. A directory is read whole and sorted on one worker, so a huge flat
  directory is serial. Local keys have no length cap (S3 keys are capped at 1,024 bytes). Timestamps are
  microseconds. A local run keeps no journal: an interrupted one starts over.
- **Resume rework: the whole speculative suffix.** S3 and GCS resume at part granularity: a resume lists the
  scope again from K, the last published part's max key (`keyrange` tiles `(K, ⊤]` afresh: the seed's listings above K, or one range), so everything
  the killed attempt had listed above K is listed again: the open part's rows, rows buffered in memory or in
  disposable spill for ranges ahead of a slow frontier, and parts closed but not yet recorded (a gap in the
  ordinals). That suffix is bounded by how far the listing ran ahead of the frontier (the row budget and spill
  cap, the workers and the time since the last published part), not by the part size: the part size is a target,
  not a hard maximum, and `--part-interval` bounds how long rows wait in an open part, neither the age of the
  durable frontier (a closed part still publishes in order, and a stalled frontier has no part to close) nor the
  work done past it.
  On a slow first range the suffix can be most of the listing (the slow-first smoke above), and on a large
  sorted run it is: a kill at 40 % relisted 90–100 % of the keys on every key list of 20M or more
  ([the sorted run's working set](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)). Java resumed each
  range from its own durable cursor. The measurement campaign's relisted-work gate (relisted keys at most one
  part plus 5 % of the listed keys, with the relisted bytes, requests and the durable frontier's age reported)
  is a gate to test on kills at peak speculation; it is not a property this build guarantees, and no claim
  about restart economics should precede it.
- **Split depth.** Each split waits for a page, so depth times the page latency bounds the wall from below. The
  merged port's pivot policy built a chain of dependent requests on large keyspaces: 398 deep on the live GCS
  run above where a balanced tree needs 13, about 16 of 64 workers in flight, and 3 of 232 production key lists
  within 4 log2(pages) + 8. With the anchor reflection, the gallop, structure probes and the seed's descent,
  185 of 201 lists are within that bound and 150 within 3 log2(pages) + 8
  ([engine campaign](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms), through a fake at
  50 ms); the 16 above it are open, and fourcastnetgfs sits at the bound in the local admission runs
  ([above](#split-admission-on-small-lists)). Live, candidate 3 listed noaa-nbm-grib2-pds at depth 16 and the
  64-wide ideal, cross-cloud from GCP us-east1 (one run); no live GCS run of the campaign's engine yet.
- **Throttling.** The limiter starts at `--workers` unless `--limiter-slow-start` (off, unmeasured). Under
  sustained throttling it holds the limit at 1–2 for the whole run ([above](#throttling-through-the-fault-proxy)).
  It has never met real 503s.
- **Not built:** versions mode, Azure. Java CLI features deliberately not ported, each with its reason, are in
  [cli-inventory.md](cli-inventory.md).
- **Restored Java CLI features, tested in process only.** Text output (TSV, JSONL, table; stdout and files;
  gzip and zstd), the seven filters (Go RE2, not Java regular expressions), the stderr progress display and
  statistics, `-v`/`-q`/`--color`, bearer-token authentication, the report's `cost`, `efficiency`, `args_hash`,
  `session_ms` and `stop_source`, and `--part-rotation-max-rows` are covered by unit and command tests against
  fakes. None has been run against a live bucket. At scale only text output has run: the study harness's
  zstd TSV row (143M keys, one file, through replay, [above](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)) and the
  plain TSV row on nbm; the filters, the progress display, bearer tokens and row rotation have not. No consumer
  of the report (the cloud importer included) has read the new fields.
- **Platforms.** Windows refuses journaled runs (no directory sync,
  [internals/durability.md](internals/durability.md)); Windows and macOS run the test suite in CI, but nothing is measured there. Power
  loss is untested.
- **Scale.** Runs of hours and listings past a billion keys are unmeasured. The largest runs are key lists of
  up to 593M keys through the fake and one live bucket of 1.09 billion objects at 256 and 1,024 workers
  ([a billion live objects](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)).
- **Local benchmarks.** Cold-cache numbers are guest-cold in a VM and single runs; all trees are synthetic.

## Skipped or not measured

- Live GCS beyond the one private in-region run above, which measured the port before the engine campaign; the
  campaign's in-region GCS gate (that bucket within 12 s) has not run. GCS's own latency profile.
- The policy switches on the campaign's engine (the four recordings themselves were rerun, `recordings1`).
- Throttling against real S3 or GCS; Java under the sustained-throttle fault.
- The resume ramp-up after a restart, and the defaults and resume-storm campaigns (smoke only, above).
- EIO beyond ENOSPC (no fault-injecting filesystem); power-loss ordering.
- Cold-cache medians of the local walker; `readlink` cost (80 symlinks in `realistic`).

## The seekparquet module's part writer, 2026-11-08

The part encoder moved from swath's writer to the seekparquet module's (`write.NewPart`, pages through
`PartWriter.WritePage`); the bytes are unchanged (`TestFooterOnlyChange`). Writer alone, 300,000 rows of
`parquetout`'s test schema, one part, best of four alternating runs on one shared Xeon Platinum 8581C host:

| | swath writer (main 5de12601) | module, `WriteRows` (6dba953c) | module, `WritePage` (6401109d) |
|---|---|---|---|
| one column goroutine | 1,303 k rows/s | 803 (−37 %) | 1,235 (−5 %) |
| four column goroutines | 2,166 k rows/s | 914 (−58 %) | 1,907 (−12 %) |

`WriteRows` checks and copies every row; `WritePage` borrows the values from swath's page of rows, and what
remains is its serial key-order check. Admission is back at swath's price (31.5 MiB an S3 encoder; the heap
gate held four local writers, inputs retained, at 219.5 of 244 MiB).

Shape suite, `nightly.yml` `go-shapes` dispatched on `main` and on the `WritePage` commit together (4-CPU
runners), nine `pq.*` arms and 79 listing-only arms:

| runs (main / branch) | `pq.*` main | `pq.*` branch | listing-only main | listing-only branch |
|---|---|---|---|---|
| 42529681487 / 42529679501 | 24.4 s | 31.1 s (+27.6 %) | 605.9 s | 649.3 s (+7.2 %) |
| 42530957423 / 42530955503 | 29.3 s | 26.9 s (−8.1 %) | 639.7 s | 626.7 s (−2.0 %) |

A single pair moves by tens of percent between runners (the same `main` took 24.4 s and 29.3 s), so these
two pairs do not resolve a difference. Interleaved on one host (the dev VM, 4 vCPUs, full scale, two rounds
of `pq.baseline`, `pq.highbytes`, `pq.punct_siblings`): `main` 15.32 s, the branch 15.12 s (−1.3 %). With
`WriteRows` (runs 42525290072 / 42525288025) the `pq.*` arms took 29.0 s against 36.0 (+24.3 %) and the
listing-only arms moved +0.9 %. One core hashes SHA-256 at 1,504 MiB/s and MD5 at 692.

## SeekParquet conformance

2026-11-08, the switch to SeekParquet ([testing.md](testing.md#seekparquet-conformance) says how it runs; CI's
`go-nonrace` job runs the same script on every Go change). seekparquet `go/v0.1.0` (commit `81e44548`), suite
revision `0.1.0-draft.4`; the same numbers on the development host and in the pull request's CI (#299, `go-nonrace`).

| Check | Result |
|---|---|
| Suite reader target, the module's Go adapter | 1,429 passed, 0 failed, 207 skipped (the validator targets, which the module does not claim); `reader` qualified |
| `s3-current`: 4 parts, 9,000 rows, `unique` true | reference adapter: `full` and `digest` complete; 234 requests answered alike by both adapters |
| `s3-versions`: 5 parts, 24,976 rows, `unique` false, a 3,000-row group | `full` and `digest` complete; 184 requests alike |
| `local-long-keys`: 3 parts, 3,000 keys of 1,109 bytes, half led by 1,100 `0xFF` bytes (a third with link targets as long, added after the review below) | `full` and `digest` complete; 204 requests alike |
| The same check with the versions dataset's manifest edited to `unique: true` | refused, `stamp_mismatch`, at open and at the `structural` level (the check fails as it should) |

`digest` checks nothing (swath writes no `sha256`). What this does not show: the suite's indexed-access
suite does not exist yet, so no reader claims it; the replay server's own routes over these datasets are
held by its store and route tests, not by the suite.

**Data pages unchanged.** `parquetout` `TestFooterOnlyChange` writes the same rows as the format 2 writer did
at `a1177c7f` (short unique keys over two parts, repeated keys, 1,100-byte binary keys) and compares SHA-256
digests recorded there: the column chunks' pages, the offset indexes, the value columns' indexes and the
FileMetaData without its key/value metadata, `created_by` and index locations are identical; the key index is
identical for keys within 1,024 bytes and exact for the long ones. The same digests held with
klauspost/compress v1.20.3, brotli v1.2.8 and lz4 v4.1.33, which the seekparquet module requires (from
v1.19.1, v1.1.1 and v4.1.21): the writer's zstd output did not change for these pages, the long keys' pages
over one zstd block included. That is evidence for these inputs, not a general identity: v1.20.3's encoder may
choose its entropy tables differently on other multi-block data. After the review (#299) the value columns'
bounds are exact too, which changes only values over 1,024 bytes; the pinned digests are unchanged. The digests are recorded on linux/amd64 and pinned there (60 runs at 1, 2 and 8 CPUs agree); on macOS/arm64
(CI's cross-OS job) the grouped scenario's compressed pages differed while its page indexes matched, so the test
compared the pages' bytes on linux/amd64 only. The cause (#305) is parquet-go's AVX2 kernel that ends a
bit-packed run of dictionary indexes, not zstd. It stops at a group of eight indexes whose halves are each
uniform, so the grouped rows' `kind` column is encoded differently: 2,264 against 2,262 bytes uncompressed.
The generic digests are the same on darwin/arm64 and linux/arm64 (CI), and on linux/amd64 with
`GODEBUG=cpu.avx2=off` or `-tags purego`. `-tags noasm`, which removes klauspost/compress's assembly, leaves the
AVX2 digests unchanged. The test now pins a golden per path
([internals/parquet-output.md](internals/parquet-output.md#byte-identity)).

## Downstream reader compatibility

The changes in [upgrade.md](upgrade.md) were applied to a scratch copy of the downstream reader. It accepts a
format-2 dataset by the manifest's coverage (whole bucket, fails closed), dispatches on the manifest's
`format` first, and reads every schema id and the `swath.profile` stamp. Probe on a 20,000-key synthetic fake
bucket (open, stream, list one prefix):

| Dataset | Result |
|---|---|
| format 2, `--sequential` | opened; 20,000 objects in strict order; `d05/` lists 5 prefixes |
| format 2, parallel, `--seed` | the same |
| format 1 (the pre-change binary) | the same: the legacy path still works |
| format 2, prefix `s3://eq/d03/` | refused: "dataset does not cover the whole bucket" |

The reader's own tests pass in the patched copy. `bench/diffds.py` finds the format-2 and format-1 datasets of
the same listing identical through its projection; format 2 is 2.2 % smaller. A local dataset is not a bucket
dataset; the reader dispatches on the schema id and reads every provider's datasets
([upgrade.md, "What a reader of the Java layout changes"](upgrade.md#what-a-reader-of-the-java-layout-changes)). The Java tool's reader does not read format 2, by design.

## CI: Go caches, race shards and cross-OS legs, 2026-11-09

Before (about 200 `ci.yml` runs, 2026-11-08/09, almost all Go-only changes): a warm run took 7–8 min and
`go-race (rest)` was its critical path at 5.4–6.0 min, 8.0–8.4 on a build-cache miss. The repository's
Actions cache held 16.3 GB against its 10 GB limit (the Go caches 9.15 GB in 16 per-job entries, each with
its own copy of the module cache), so entries were evicted continuously; a miss cost `go-objfile` 5.3–5.7 min
against 0.6, `go-reader` 2.5 against 0.5, `go-lint` 2.0 against 0.7. Every run over 13 min had waited 7–10
min for a macOS runner. 35 of 200 runs failed; the platform-only failures were 7 macOS-only (six test-timing
flakes fixed in the tests, #307 #317 #334 #336; one arm64 encoder-path difference, #308) and 6 Windows-only
(three of one clock-tick flake, #320; one filesystem difference, #310).

After #355 (shared module cache, per-job build caches, retention) and #356 (`rest` in three shards, Windows
and Linux arm64 on pull requests, macOS on `main`), job wall times in minutes, one run each:

| job | PR 42626611538 | `main` 42629790131 | `main` 42631148177 (warm) |
|---|---|---|---|
| `go-race (cmd-kill)` | 4.3 | 4.6 | 4.6 |
| `go-race (rest-replay)` | 4.1 | 3.4 | 3.2 |
| `go-race (rest-source)` | 3.4 | 4.8 | 2.7 |
| `go-race (listing-rest)` | 2.8 | 2.9 | 3.0 |
| `go-race (crash)`, `(durable)`, `(cmd)` | 2.4–2.5 | 2.5–2.9 | 2.4–2.5 |
| `go-race (rest)` | 2.2 | 1.7 | 1.6 |
| `go-xos (windows-latest)` | 5.0 | 5.8 | 5.4 |
| `go-xos (ubuntu-24.04-arm)` | 5.4 | 5.6 | 4.6 |
| `go-xos (macos-latest)` | not on pull requests | 7.8 | 7.5 |
| `go-objfile` | – | 5.5 (build-cache miss) | 0.8 |
| `go-lint`, `go-reader`, `go-nonrace` | – | 0.6, 0.9, 0.9 | 0.9, 0.9, 0.9 |

No `rest*` shard is above `cmd-kill`; the `go-xos` legs are the longest Go jobs on a pull request, and
`rest-source` moves 2.7–4.8 between runs (its packages' summed time was 716 s, `rest-replay`'s 510, the
remainder's 338 in the `-json` artifact of run 42614118007). The warm `main` run's `go-lint` log shows the
exact module-cache hit (`go-mod-Linux-X64-<deps>`) and the build-cache restore from the previous commit's
key (`go-build-Linux-X64-go-lint-<deps>-<sha>`).

Cache sizes as `gh cache list` reports them (zstd-compressed) after the warm run: the module cache 500–504
MiB on each of Linux amd64, Linux arm64, Windows and macOS (2.0 GB); the build caches `go-objfile` 628,
`go-build` 357, `go-lint` 206, `go-reader` 204, `go-race (cmd)` 161, `go-race (rest)` 157, `go-xos` 146–155
per OS, `go-race (cmd-kill)` 140, `rest-source` 122, `rest-replay` 111, `go-fixtures` 106, `go-nonrace` 87,
`crash`/`durable`/`listing-rest` 78 each (2.9 GB). One generation is about 4.9 GB; a second exists while
two `main` runs overlap, and `go-cache-prune` deleted 6, 2 and 3 superseded entries in the three runs. The
13 legacy combined entries (6.7 GB) were deleted by hand after #355 merged.

The first `main` run after #355 (42628492522) was red: `go mod download all` adds the hashes it fetches to
`go.sum`, so `go mod tidy -diff` failed in `go-lint`, `go-objfile` and `go-reader`; the module cache it
saved was complete. #360 runs the download on a copy of each module's `go.mod` and `go.sum`. Usage stayed
above 10 GB after the Go caches shrank because `docker-check` and `go-build` wrote BuildKit caches on pull
requests, scoped to each pull request's ref (0.4–0.8 GB each, 1.8 GB over three open pull requests), and
GitHub's CodeQL setup keeps a 672 MiB dependency cache per pull request head. #361 writes the BuildKit
caches from `main` only; its own pull-request run left no `buildkit-blob` entry under its ref.

### Parallel CLI crash tests and Java input filtering, 2026-11-10

PR #410 lets seven isolated CLI crash/recovery tests overlap after their private binary builds,
bounded by Go's normal `-parallel` limit. Their crash arms and assertions are unchanged. Java now runs
for its source, build, image and fixture inputs; ordinary prose, site layout and Go-only scripts no
longer trigger it ([testing.md](testing.md)).

One successful hosted run before and after, job wall time including setup:

| Job | [Before, 42821964692](https://github.com/varveio/swath/actions/runs/42821964692) | [After, 42825002980](https://github.com/varveio/swath/actions/runs/42825002980) |
|---|---:|---:|
| `go-race (cmd-kill)` | 4m34s | 3m20s |
| `go-xos (ubuntu-24.04-arm)` | 5m22s | 3m58s |
| `go-race (rest-source)` (unchanged) | 4m45s | 4m33s |
| Java `fast-tests` (unchanged) | 4m57s | 5m11s |
| CI run, creation through completion | 5m43s | 5m29s |

The revisions are `4c1dd429` and `67189d1b`, both with Go 1.26.10. These are observational runs, not
an interleaved benchmark: runner/cache variation and unrelated changes can affect the comparison.
The two targeted jobs were 27% and 26% shorter, but the full run only 4% shorter because Java became
the critical path. The workflow edit intentionally triggers Java; this run does not measure the
benefit of skipping it on ordinary documentation changes.

A local focused before/after run passed the same 15 test/subtest cases, with no failures. Its timing
is unsuitable for comparison: other workloads heavily loaded the shared host during the second run.
The focused command was `GOMAXPROCS=4 go test -count=1 -timeout 12m -json
-run '^(TestSIGKILL.*|TestForcedExit.*)$' ./cmd/swath` from `go/`.
