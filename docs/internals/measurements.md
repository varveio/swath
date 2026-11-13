# Measurements

The measurements the other docs and the pull request cite, each with its configuration and the harness that
reproduces it. Scripts are in [`../../bench/`](../../bench/); how to build the replay oracle and the fakes is in
[testing.md](testing.md).

**Host.** Unless a section says otherwise: 4 vCPU (Intel Xeon @ 2.10 GHz), 15 GiB RAM, ext4 on a virtio disk,
Linux 6.18 (Firecracker guest), Go 1.26. The host was shared and its speed drifted during sessions (steal time
seen in `vmstat`), so **compare absolute times only within one table**; each table's arms ran interleaved in
one session. Ratios held across sessions.

**Commands as run.** Commands and flags are quoted as the build of the day took them. Since report version 2
and the command-line cleanup ([../upgrade.md](../upgrade.md#command-line)): `swath list --count-only` is
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
| Java listing a Go dataset served by replay's sorted mode (1 part and 6 parts); historical: run on format-1 Go output, before format 2, which Java's reader does not read ([../upgrade.md](../upgrade.md)) | 2 |

The 21 live buckets are noaa-normals-pds, noaa-nws-fourcastnetgfs-pds, real-changesets and 18 open buckets
chosen for the Java policy features they engage (among them noaa-eri-pds and pansurg-curation-raw-open-data),
from 1,018 to 13.9 M objects.

**Size.** Go datasets are 0.4–6.9 % larger than Java's after writing `etag` as `DELTA_BYTE_ARRAY` (1M fixture
1.050 → 1.027 of Java's size; noaa-normals-pds 1.089 → 1.069). The rest is `last_modified` on buckets with few
distinct timestamps: parquet-go cannot switch from dictionary to delta mid-chunk as parquet-mr does
([parquet-output.md](parquet-output.md)).

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
the admission gates and the remainder rank ([range-engine.md](range-engine.md)).
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
[`../../build.sh`](../../build.sh).

### SDK-backed clients (2026-11-09)

Clients built with `-tags swath_sdkclient`, measured on branches `exp/measure-preview` (S3) and
`exp/gcs-sdkclient` (GCS). Synthetic inputs; the numbers describe those runs. Since then the SDK-backed clients
are the only clients ([clients.md](clients.md)) and the build tag, `SWATH_S3_DECODER` and the
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

### S3 scanner cost (2026-11-11)

A CPU profile of a full listing of `aws-public-blockchain` (143,008,674 objects, sorted Parquet, 256 workers on 8
vCPUs against the Go replay server; swath-notes `2026-11-11-swath-go-campaign/result-r2-profile.md`, a private run
summary) put the scanner at 252 of 714 CPU-seconds, 35 %, about 1.76 µs per object. Microbenchmarks on the build host
(4-vCPU GCP VM, Intel Xeon Platinum 8581C, Go 1.27.4; shared with other work, so the ratios are the result), the
commit before the change and the change alternated as two test binaries, eight runs each at `-benchtime=1500x`,
medians compared with benchstat (± is benchstat's confidence interval; every change p=0.000). `BenchmarkDecodeBody`
is the scanner alone, `BenchmarkDecode` one full `ListObjectsV2` or `ListObjectVersions` call through the SDK stack
and the Guard over an in-memory 1,000-entry page. Pages: `list` and `versions` are the older synthetic pages
(URL-encoded keys, owners, checksums); `blockchain` and `full` are in the replay server's bytes, keys shaped like
`aws-public-blockchain`'s (every key holds an escaped `=`), `full` with checksums, owners, multipart ETags and escaped
spaces and UTF-8:

| Benchmark | Page | Before ns/object | After ns/object | Change | B/object | allocs/object |
|---|---|---:|---:|---:|---:|---:|
| `DecodeBody` | list | 1,161 ± 2 % | 726 ± 2 % | −37 % | 271, unchanged | 0.003 (3 per page), unchanged |
| `DecodeBody` | versions | 1,181 ± 2 % | 764 ± 2 % | −35 % | 329, unchanged | 0.003, unchanged |
| `DecodeBody` | blockchain | 750 ± 2 % | 389 ± 2 % | **−48 %** | 279, unchanged | 0.003, unchanged |
| `DecodeBody` | full | 1,158 ± 2 % | 660 ± 1 % | −43 % | 279, unchanged | 0.003, unchanged |
| `Decode` | list | 1,333 ± 1 % | 897 ± 1 % | −33 % | 911, unchanged | 0.45 (451 per page), unchanged |
| `Decode` | versions | 1,366 ± 1 % | 950 ± 1 % | −30 % | 983, unchanged | 0.45, unchanged |
| `Decode` | blockchain | 890 ± 1 % | 537 ± 2 % | −40 % | 716, unchanged | 0.45, unchanged |
| `Decode` | full | 1,329 ± 2 % | 835 ± 2 % | −37 % | 922, unchanged | 0.45, unchanged |

What changed, in the order applied, each measured against the step before on the body-only benchmark (blockchain
page; the other pages moved the same way or less): the URL decoding of a key moves the runs between escapes as
blocks instead of byte by byte (−14 %); the `]]>` search on the scalar fast path runs only when the text holds a `]`
(−3 %); a `<` that is not followed by `!` or `?` goes straight to the tag reader instead of four prefix tests for
comments, CDATA, processing instructions and directives, measured together with the same `]` test in the general
text decoder (−18 %); the five predefined entities are a switch, not a map (−4 %); a tag name ends at
a byte-table lookup (−8 %); text with references that ends at its own end tag (every ETag's two `&quot;`) is decoded
on the fast path instead of the general loop (−11 %); a `<` already at the cursor skips the search for it (−3 %). Two
ideas did not win and are not in: one table-driven pass over a scalar's text in place of the byte searches (+4 %),
and a byte loop in place of `IndexByte` for the namespace colon (+2 %). The full-client cost above the scanner is
about 145 ns per object (145 µs per page) before and after: the SDK's middleware, its decoding of the minimal
envelope, and the Guard's one allocation and copy of the body, which is most of the 437 B/object the full call
allocates beyond the page. `TestScannerMatchesOld` and `FuzzScannerMatchesOld` hold the scanner to the one it
replaced (kept in `xml_old_test.go`): the same pages, the same errors.

Of that client cost, about 11 µs per request was `debug.ReadBuildInfo` parsing the build info for the User-Agent
token: the SDK runs API options on every call. The token is now read once per client. `BenchmarkDecode`, ten
alternated runs: blockchain 536.5 → 519.0 ns/object (−3.2 %), list 903.6 → 878.6 (−2.8 %), versions 957.8 → 930.6
(−2.8 %), full 843.2 → 822.6 (−2.4 %), all p ≤ 0.001; 451 → 364 allocations per page. The GCS client still reads
it per listing request (`gcs/client/list.go`); not measured.

### Pivot density step cost (2026-11-11)

The same blockchain profile put `keyrange.extrapolateWhy`, the density step `propose` runs after every continuing
page (138,485 split decisions considered in 149,783 pages), at 24 of 714 CPU-seconds, 3.4 %: 15 s in its own loop over
every byte of the page, 4 s in `isSafe`, 1.6 s in `growslice` (`append(page, c)` copied the 1,000-key page whenever
it had no spare capacity, and otherwise wrote the cursor into that capacity), about 3 s in `math/big` (three values of
about 80 digits per call, not one per key). `BenchmarkExtrapolate`, one call over a 1,000-key blockchain-shaped page
(stride 64), on the build host as above, old and new binaries alternated eight times at `-benchtime=3000x`:

| Case | Before | After | Change | B/op | allocs/op |
|---|---:|---:|---:|---|---|
| frontier | 82.9 µs ± 3 % | 25.7 µs ± 5 % | −69 % | 25.5 KiB → 1.5 KiB | 19 → 18 |
| bounded | 83.0 µs ± 3 % | 27.2 µs ± 6 % | −67 % | 25.8 KiB → 1.8 KiB | 24 → 23 |

The page and the cursor are scanned in place (no copy), every byte is marked in a 256-entry table eight bytes per
step, and the alphabet's safety is checked once over the table instead of per byte (−33 % from the first two, a
further −51 % from the unrolled marking). `TestExtrapolateMatchesOld` holds the result, pivot and reason, to the one
it replaced (kept in `extrapolate_test.go`).

### azblob v1.7.0 to v1.8.3 (2026-11-10)

`BenchmarkList` (`internal/source/azblob/client`), main `05e36feb` (azblob v1.7.0, azcore v1.21.1) against the
v1.8.3 change (azcore v1.24.0, explicit XML response format), both with Go 1.27.3, in one GitHub-hosted
`ubuntu-latest` job (4 CPUs), the two test binaries alternated ten times at `-benchtime=300x`, compared with benchstat:

| Page | v1.7.0 µs/object | v1.8.3 µs/object | Change | Allocations/page | Bytes/page |
|---|---:|---:|---:|---|---|
| flat | 22.54 ± 1 % | 22.73 ± 1 % | +0.8 % (p=0.000) | 865.2k, unchanged | 40.25 MiB, unchanged |
| hierarchy | 12.54 ± 0 % | 12.57 ± 3 % | ~ (p=0.10) | 485.3k, unchanged | 22.14 MiB, unchanged |

The XML path costs the same. v1.8's `container` package imports `apache/arrow-go` for its Arrow listing format with no
build tag to leave it out, so `cmd/swath` linked it before swath used Arrow (next section): stripped static binaries
40,194,208 to 45,015,200 bytes (+4.8 MB), 727 to 767 packages, 34 of the 40 from Arrow and its dependencies. Arrow's
`arrow_json_stdlib` tag would save 1.4 MB of that; it is not used. Package initialisation grows by about 0.3 ms.

### Arrow listing format (2026-11-10)

Live, anonymous, `swath list --anonymous az://azureopendatastorage/citydatacontainer/Crime/ -o FILE.tsv` (468,557
blobs, 64 workers, the default 5,000-entry pages) on a 4-vCPU GCP VM, `/usr/bin/time`. Azure never compresses List
Blobs; an Arrow page is about 2.2 times smaller than its XML. First measurement (issue #226, an experiment build
that switched the format by environment variable):

| Format | Wall | CPU | Peak RSS |
|---|---:|---:|---:|
| XML | 33–47 s | about 58 s | 1.5–1.9 GB |
| Arrow | 3–5 s | about 3.4 s | 0.2–0.5 GB |

Re-run with the change that made Arrow the default (one run each, the XML arm built from main before it, which
names the XML format):

| Format | Wall | User + sys CPU | Peak RSS | Attempts | Pages by format |
|---|---:|---:|---:|---:|---|
| XML (main `b546596a`) | 29.50 s | 55.72 + 2.50 s | 1,483,416 KB | 293 | (no counters) |
| Arrow | 3.36 s | 3.10 + 0.77 s | 468,212 KB | 289 | `list_format.arrow_pages` 287, `xml_pages` 0 |

The two TSV files were byte-identical (`cmp`), in both measurements. The Arrow arm's `list_format.arrow_items`
(1,271,822) exceeds the objects because it counts every listed item, the seed's and probes' and the adapter's
discarded overlap included. The CPU saved is the XML decoder's per-element work: the Arrow decoder reads typed
columns.

### Azure page size (2026-11-10)

`swath diag count --anonymous` against the public `azureopendatastorage/citydatacontainer`, Arrow listing, 64 workers on a
4-vCPU GCP VM, a binary built from main at `5803bf93` (the reports embed it), alternating runs. Wall and CPU are `/usr/bin/time`'s; peak RSS is its maximum resident set in MiB. "Decoded" is `list_format.arrow_items`; "overlap" is `order_overlap_rows`,
rows read past a span's end and discarded (rows handed to a sibling are not counted).

| Listing | Page | Decoded | Overlap | HTTP attempts | Wall s | CPU s | Peak RSS MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| `Crime/` (468,557 blobs) | 5,000 | 1,266,821 / 1,271,822 | 787,645 / 792,645 | 288 / 289 | 3.84 / 4.96 | 3.34 / 3.64 | 266 / 514 |
| `Crime/` | 1,000 | 672,192 | 180,761 | 709 | 2.81 | 2.49 | 177 |
| `Crime/` | 500 | 636,199 | 138,395 | 1,337 | 3.98 | 2.61 | 127 |
| whole container (1,840,967 blobs) | 5,000 | 4,285,291 / 4,150,688 | 1,376,343 / 1,272,315 | 915 / 888 | 14.34 / 11.51 | 12.63 / 12.26 | 331 / 365 |
| whole container | 1,000 | 2,759,935 / 2,737,864 | 448,737 / 422,093 | 2,818 / 2,794 | 6.66 / 6.19 | 9.50 / 9.20 | 153 / 165 |

The seed cuts these listings for parallelism, not to page boundaries (on `Crime/`, 256 cuts gave 258 ranges; the seed does not count their populations), and a span reads until its first non-member, so
most of each span's last page was discarded. At 1,000 the whole container listed in about half the wall time with a quarter
less CPU and half the memory, for three times the requests (List Blobs is billed per request). 500 decoded about 5% fewer rows and used less memory
(127 MB against 177) but nearly doubled the requests and took longer. The default became 1,000, which matches S3's page cap and the GCS page size swath uses. `EndBefore` (#226) would end each span at the
service and make large pages cheap again; revisit the default if it lands.

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
`TestRetainedBoundsDoNotPinPages` (synthetic: `scalefake`, 400,000 keys, [ordering](ordering.md#what-a-waiting-range-retains)).

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
([range-engine.md](range-engine.md)), so the campaign's MinIO fixture
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

## Uploads to a bucket, on the fake (2026-11-11)

The upload path of a dataset written to GCS (`objfile/gcs`'s writer, 8 MiB resumable chunks), measured on a
4-vCPU GCE VM (Xeon Platinum 8581C) against the in-process fake, so with no network: what it costs the client,
not what a bucket sustains. `BenchmarkUpload` (`go test ./gcs -run X -bench BenchmarkUpload -benchtime 10x` in
`go/objfile`) writes a 64 MiB object 4 KiB at a time, as the part writer does. Measured at the writer of
`8c256076` (the benchmark itself added in `5415cf54`); the raw benchmark output was not kept, so rerun the
command to reproduce:

| Server | Throughput | Bytes allocated per object | Allocations per object |
|---|---|---|---|
| the fake, keeping the bytes and checking the CRC32C | 388 MB/s | 523 MB (the fake's own appends) | 2,117 |
| middle chunks read and dropped (the client's cost) | 1,557 MB/s | 8.9 MB (one 8 MiB chunk buffer) | 1,763 |

So an upload of one 64 MiB part takes about 43 ms of wall time on this host with a server that drops the bytes (client and server together), one chunk buffer per open
upload, against the seconds of CPU a part's encoding takes. `TestMeasureRemoteOverhead`
(`SWATH_MEASURE_REMOTE=1`, `internal/listing`) runs one listing of 400,000 keys (gofakes3) with 4 encoders and
256 KiB parts (28 parts, 7.8 MB) to a local directory and to the fake, three times each: wall time 0.97 to
1.15 s either way, process CPU (the fakes' included) 3.1 to 3.4 s local and 3.2 to 3.7 s remote after the first
run, and the remote runs' `upload_wait_ns` 0.48 to 0.60 s summed over the encoders (each part one request to
the in-process fake), at `5415cf54` on a host shared with other builds; its raw log was not kept (provenance:
the test's own `t.Log` lines, rerun with the variable set). These are single-host smoke numbers; a real
bucket's latency and bandwidth are not measured here.

## Spill read handles on local trees, 2026-11-11

**Local host, screening only.** A 4-vCPU, 8 GiB GCP VM, ext4 on a persistent disk, shared with other agents' builds
and tests: the 1-minute load was 11 to 50 during every arm and the host swapped (5 GiB of swap in use), so trees
listed warm went cold between runs. Wall times swing tenfold between repetitions of one arm in either binary and
are not evidence; the counters (frames, ranges, opens per frame and per range, handle hits) are. Trees from
`bench/localtree_gen.py`, runs by `bench/localtree.sh` (arms shuffled within each repetition, `--encoders 4`,
default 16 workers, output and spill on ext4), tables by `bench/localtree_table.py`. Base is 0c856fc3, new is
the read-handle cache (`order/spillhandles.go`) at the same base.

**Baseline: the macOS pathology's shape reproduces on Linux, at a smaller sort buffer.** At 128 MiB and 1 GiB no
tree of 1.2M files spilled with 16 workers (`smalldirs`, `bigdirs`); a probe at 128 MiB with 64 workers (the owner's
Mac runs 64) spilled 19,406 rows in 5,656 frames (3.4 rows a frame, 3.0 opens a frame) with a 2.7 s tail. At 32 MiB:

| tree (1.2M files, ext4) | wall | spilled rows | frames | ranges | rows/frame | opens | opens/frame | tail |
|---|---|---|---|---|---|---|---|---|
| `smalldirs` (200k directories of 3-9 files) | 255 s | 1,163,642 | 332,315 | 166,143 | 3.50 | 996,868 | 3.00 | 135 s |
| `skewed` (70k directories, long names) | 70 s | 1,112,656 | 112,870 | 64,824 | 9.86 | 367,016 | 3.25 | 3.9 s |
| owner's Mac, 128 MiB (for comparison) | 31 s | 1,019,908 | 194,277 | 116,859 | 5.25 | 622,180 | 3.20 | 13 s |

Opens are two per frame plus two per range in the base (996,916 predicted, 996,868 counted on `smalldirs`): each
frame is written on its own open and the trailer on another, each frame read at an offset on its own, and each
trailer on a reader opened for it. On
`smalldirs` the read-ahead's reads took 383 s summed over its 3 readers (2.3 ms a frame), so the tail is I/O
bound on this host, not decode bound (0.6 s of decoding on the Mac). How much a run spills depends on
how fast the walk outruns the drainer: on a cold or contended tree the same `smalldirs` arm spilled nothing.

**After: read opens per range from 3 to about 1.5.** Medians of 5 interleaved repetitions (`smalldirs` 300k files,
50k directories; `bigdirs` 1.2M files in 24 directories):

| tree, buffer | bin | rows/frame | opens/frame | opens/range | hits | misses | user CPU | tail |
|---|---|---|---|---|---|---|---|---|
| `smalldirs` tmpfs, 8 MiB | base | 3.50 | 2.97 | 5.94 | – | – | 5.5 s | 2.9 s |
| | new | 3.50 | 2.27 | 4.53 | 56,014 | 57,186 | 3.4 s | 3.0 s |
| `smalldirs` tmpfs, 32 MiB | base | 3.48 | 2.99 | 6.04 | – | – | 1.6 s | 0.8 s |
| | new | 3.51 | 2.22 | 4.45 | 18,684 | 17,442 | 1.8 s | 1.8 s |
| `smalldirs` ext4, 8 MiB | base | 3.50 | 2.98 | 5.97 | – | – | 4.7 s | 0.8 s |
| | new | 3.50 | 2.20 | 4.39 | 68,201 | 58,694 | 3.5 s | 0.7 s |
| `smalldirs` ext4, 32 MiB | base | 3.49 | 3.00 | 6.00 | – | – | 2.0 s | 0.5 s |
| | new | 3.50 | 2.03 | 4.05 | 21,403 | 19,257 | 2.1 s | 0.6 s |
| `bigdirs` ext4, 8 MiB | base | 1,020 | 3.02 | 5.90 | – | – | 3.2 s | 0.5 s |
| | new | 1,020 | 2.04 | 3.98 | 1,028 | 556 | 3.4 s | 0.3 s |
| `bigdirs` ext4, 32 MiB | base | 1,020 | 3.02 | 5.90 | – | – | 3.1 s | 0.5 s |
| | new | 1,019 | 2.39 | 4.66 | 356 | 490 | 3.0 s | 0.2 s |

`skewed` (1.2M files, ext4, 32 MiB) has single runs only (its repetitions did not finish on the loaded host):
base 5.66 and 5.95 opens a range (367,016 and 400,892 opens), new 4.17 (272,791 opens over 65,416 ranges;
98,098 hits, 93,354 misses). At 128 MiB and 1 GiB nothing spilled in either binary (the cache is idle; walls within the noise). The writes'
opens are unchanged (one per frame and one per trailer), so the floor with one read open per file is one per
frame plus two per range: about 4.0 per range on both trees (two frames a range). `bigdirs` reaches it; on
`smalldirs` the cache reaches 4.0 to 4.5, with 1.4 to 1.6 misses per range, because the read-ahead touches up to 64 planned frames (about 30 files) ahead of the drainer while at
most 16 descriptors stay open, so a file's handle is often evicted (`evictions.capacity`) before the drainer
reads its trailer. Evicting the most recently used instead (a probe, not kept) gave the same 4.3 to 4.4. User CPU
fell by a quarter to a third at 8 MiB on `smalldirs` (fewer opens, and no 64 KiB reader buffer per trailer); no
wall or tail change is measurable on this host. On the Mac, where the open and close were the expensive part
(162 s of system CPU), the same counters predict about a third fewer opens; Batch and the Mac decide the wall.

## S3 uploads, on the fake (2026-11-11)

The same measurements for a dataset written to S3 (`objfile/s3`'s writer: 8 MiB parts, a multipart upload per
part larger than one, CRC32 checksums), on the same kind of 4-vCPU VM, shared with other jobs during the run
(load average 10 to 18): `BenchmarkUpload` (`go test ./s3 -run X -bench BenchmarkUpload -benchtime 10x` in
`go/objfile`) writes a 64 MiB object 4 KiB at a time to `s3/s3fake`, and the GCS benchmark ran in the same
invocation for comparison. Measured at the S3 writer as first written on `go/s3-output` (the commit
"objfile/s3: create-only writes as multipart uploads, with a fake"), before the later changes to its failure
paths (aborts, redaction, unresolved completions), which add no request to a successful upload; the raw
benchmark output was not kept, so rerun the command to reproduce:

| Writer, server | Throughput | Bytes allocated per object | Allocations per object |
|---|---|---|---|
| S3, the fake keeping the bytes (and each part's MD5, its ETag) | 144 MB/s | 431 MB (the fake's own copies) | 5,439 |
| S3, parts read and dropped (the client's cost) | 857 MB/s | 3.2 MB (the part buffer is pooled across uploads) | 5,473 |
| GCS, the fake keeping the bytes | 310 MB/s | 514 MB | 2,112 |
| GCS, middle chunks read and dropped | 1,332 MB/s | 8.8 MB | 1,758 |

So the S3 client spends about 78 ms per 64 MiB object (8 `UploadPart` requests, the creation and the
completion), against GCS's 50 ms on the same run: it sends each part from `Write` (nothing overlaps the
buffering of the next part) and computes each part's CRC32 before sending it. Both are small beside a part's
encoding. `TestMeasureRemoteOverhead` now runs per provider (the same tree and day; its log was not kept either): for S3, with the listing's 256 KiB parts (one
`PutObject` each), wall time 1.12 to 1.42 s remote and 1.13 to 1.37 s local, process CPU 3.2 to 3.9 s remote and
3.2 s local (after each provider's first run, whose CPU figure includes the setup), and `upload_wait_ns` 0.26 to
0.32 s summed over the 4 encoders (GCS in the same run: 0.46 to 1.04 s). These are fake, single-host smoke
numbers; a real bucket's latency, bandwidth and request rates are not measured here.

## Azure uploads, on the fake (2026-11-11)

The same measurements for a dataset written to Azure Blob Storage (`objfile/azblob`'s writer: 8 MiB blocks,
staged and committed by a block list for a part larger than one, each body's CRC64 computed over the buffer),
on the same kind of 4-vCPU VM, shared with other jobs (load average 3 to 4): `BenchmarkUpload`
(`go test ./azblob ./s3 ./gcs -run X -bench BenchmarkUpload -benchtime 10x` in `go/objfile`) writes a 64 MiB
object 4 KiB at a time to `azblob/azfake`, with the S3 and GCS benchmarks in the same invocation. Measured on
`go/azblob-output` once the CLI took `-o az://` (the commit "cmd/swath: -o az://ACCOUNT/CONTAINER/PREFIX, with
--output-azure-sas-token"); the raw output is not kept in the repository, so rerun the command to reproduce:

| Writer, server | Throughput | Bytes allocated per object | Allocations per object |
|---|---|---|---|
| Azure, the fake keeping the bytes (and checking each CRC64) | 336 MB/s | 446 MB (the fake's own copies) | 2,007 |
| Azure, blocks read and dropped (the client's cost) | 1,070 MB/s | 1.3 MB (the block buffer is pooled across uploads) | 1,687 |
| S3, the fake keeping the bytes | 184 MB/s | 442 MB | 5,464 |
| S3, parts read and dropped | 950 MB/s | 3.3 MB | 5,984 |
| GCS, the fake keeping the bytes | 391 MB/s | 529 MB | 2,235 |
| GCS, middle chunks read and dropped | 1,555 MB/s | 8.9 MB | 1,880 |

So the Azure client spends about 63 ms per 64 MiB object (8 `Put Block` requests and the `Put Block List`),
between GCS's 43 ms and S3's 71 ms on the same run: like S3's, it sends each block from `Write`, and computes
each block's CRC64 before sending it. The SDK's own CRC64 option copies the body before hashing it (137 MB
allocated per object in a first version of the writer); the writer hashes the buffer itself and passes the sum.
`TestMeasureRemoteOverhead` (`SWATH_MEASURE_REMOTE=1`, 400,000 keys, 4 encoders, 256 KiB parts: 28 parts,
each one `Put Blob`), three runs a provider in one invocation: for Azure, wall time 0.89 to 1.03 s remote and
0.85 to 1.04 s local, process CPU 3.1 to 3.7 s remote and 3.1 s local (after the first run, whose CPU figure
includes the setup), and `upload_wait_ns` 0.24 to 0.35 s summed over the encoders (S3 in the same run: 0.21 to
0.29 s; GCS: 0.28 to 0.78 s). These are fake, single-host smoke numbers; a real account's latency, bandwidth,
request rates and throttling are not measured here.

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
- **Resume rework: the whole speculative suffix.** *Superseded 2026-11-13.* This entry predates durable spill
  (#282). A resume now replays the certified spill files sealed above K instead of listing them again: on 64
  production lists of 20 to 150 million keys the resumes took a median 0.66x the cold run's requests after a
  kill at 40 % (p90 0.99) and 0.22x after a kill at 70 % (p90 0.43) ([Durable spill](#durable-spill)). What
  stays true, for S3, GCS and Azure Blob: a resume lists again the ranges unfinished at the kill (a durable
  file seals only when its range completes), the ranges that got no certificate slot, and the rows held in
  memory or in disposable spill; and no kill and resume with durable spill is measured above 150 million keys.
  The relisted-work gate below is a target: the engine does not enforce it and no test asserts it
  (`bench/sortab_table.py` evaluates it on the sort A/B bench's killed cells only). The entry as first
  written:
  S3 and GCS resume at part granularity: a resume lists the
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
- **Not built:** versions mode, Azure. *Superseded 2026-11-13: both are built (`--all-versions` for S3, GCS and
  Azure Blob, [running.md](../running.md#listing-every-version); Azure Blob in `internal/source/azblob`).*
  Java CLI features deliberately not ported, each with its reason, are in [cli-inventory.md](cli-inventory.md).
- **Restored Java CLI features, tested in process only.** Text output (TSV, JSONL, table; stdout and files;
  gzip and zstd), the seven filters (Go RE2, not Java regular expressions), the stderr progress display and
  statistics, `-v`/`-q`/`--color`, bearer-token authentication, the report's `cost`, `efficiency`, `args_hash`,
  `session_ms` and `stop_source`, and `--part-rotation-max-rows` are covered by unit and command tests against
  fakes. None has been run against a live bucket. At scale only text output has run: the study harness's
  zstd TSV row (143M keys, one file, through replay, [above](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)) and the
  plain TSV row on nbm; the filters, the progress display, bearer tokens and row rotation have not. No consumer
  of the report (the cloud importer included) has read the new fields.
- **Platforms.** Windows refuses journaled runs (no directory sync,
  [durability.md](durability.md)); Windows and macOS run the test suite in CI, but nothing is measured there. Power
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

## Reader Scan and Stream baseline, 2026-11-10

The reader before the batch-scan work: `go/reader` at `5220c636` with only the benchmarks added (`86ce3fe6`
and its review fixes), `BenchmarkReader*` ([testing.md](testing.md#reader-benchmarks)) as documented there,
non-race, `-benchtime 3x`, `-p 1`, through `flock /tmp/swath-gates.lock`. Host: a shared 4-vCPU Xeon Platinum
8581C (2.30 GHz), 7 GiB RAM, GOMAXPROCS 4, Go 1.26.10, other builds running; load average 2.3 at the start
and at the end of the run. Compare times only with runs on the same host at a similar load.

**Fixture** (`writeFixture`, 4,000,000 rows, seed 1, generated in 7.6 s): 2 parts, 20 row groups of 157,952
to 202,752 rows, 42.0 bytes a row on disk, keys 93.5 bytes on average (51 to 117, by DuckDB).

| Arm | ns/row | allocs/op | allocs/row | B/op | B/row |
|---|---:|---:|---:|---:|---:|
| Scan workers=1 | 1,205 | 52,233,223 | 13.06 | 5,643,673,661 | 1,411 |
| Scan workers=4 | 579 | 52,225,755 | 13.06 | 5,636,877,146 | 1,409 |
| Scan workers=8 | 564 | 52,220,381 | 13.06 | 5,634,453,962 | 1,409 |
| Stream | 1,791 | 52,701,427 | 13.18 | 10,032,517,557 | 2,508 |

Per row is per op divided by `rows/op` (4,000,000). In this run Scan made about 13 allocations and allocated
about 1.4 KB an object at every worker count; Stream made as many allocations and allocated about 2.5 KB. On
the same host `TestScanAllocations` counted 13.07 allocations an object for Scan over its 20,000-row fixture,
with and without `-race`, under its budget of 20.

## Reader column batches, 2026-11-10

The R1 SeekParquet bump also changes existing users of `RangePlan.Scan`: it is now an owning adapter over
`ScanBatches`. Completed batches may be delivered before a later page/read error, and cancellation is checked
between batches. Reader `Scan` still checks cancellation before every owning-batch flush; `Totals` publishes
no totals on failure. Projected data-page scratch is retained at each scan request's high-water capacity
across row groups. Concurrent shards each own this scratch, so the transient memory cost multiplies across
shards; scratch is not shared between them. These are dependency-level behavior and memory changes, not
only a new reader entry point.

R1's `ScanBatches` on the R0 baseline (merged as `5c2fa6e5`), with SeekParquet
`af57842d09a5` in both Go modules. The same 4,000,000-row, seed-1 fixture described above:
2 parts, 20 row groups, 42.0 B/row on disk; manifest SHA-256
`e4a5e2397fed214f2e9c33f2be037be2c0de29bf1fb244a3bfd2c42d341e2084`.

Go 1.26.10, linux/amd64, GOMAXPROCS 4, default GOGC/GOMEMLIMIT, shared 4-vCPU Xeon Platinum
8581C (2.30 GHz), about 7.7 GiB visible RAM. `BenchmarkReaderScan`, `-run '^$' -benchmem
-benchtime=3x -count=1`, under `flock /tmp/swath-gates.lock`; fixture creation and warm-up excluded,
planning and splitting included. Load average rose from 8.73 to 16.57 during this run: timing is heavily
contended and does not establish worker scaling or the 18-core commoncrawl acceptance target.

| Workers | Path | ns/row | allocs/row | B/row |
|---:|---|---:|---:|---:|
| 1 | Scan | 2,615.5 | 12.03064 | 972.5 |
| 1 | Batch-noop | 1,061.4 | 0.03500 | 375.3 |
| 1 | Batch-touch | 1,189.4 | 0.03504 | 375.3 |
| 1 | Batch-all | 504.6 | 0.04404 | 448.9 |
| 1 | Batch-size | 285.9 | 0.02204 | 373.8 |
| 4 | Scan | 2,082.0 | 12.02953 | 972.1 |
| 4 | Batch-noop | 847.4 | 0.03514 | 375.5 |
| 4 | Batch-touch | 943.4 | 0.03518 | 375.5 |
| 4 | Batch-all | 1,112.8 | 0.04310 | 448.8 |
| 4 | Batch-size | 688.1 | 0.02184 | 373.9 |
| 8 | Scan | 2,286.4 | 12.02902 | 972.8 |
| 8 | Batch-noop | 1,317.1 | 0.03506 | 375.8 |
| 8 | Batch-touch | 957.7 | 0.03506 | 375.8 |
| 8 | Batch-all | 907.6 | 0.04295 | 449.6 |
| 8 | Batch-size | 536.7 | 0.02179 | 374.1 |

`Batch-noop` selects last_modified and storage class and only counts rows. `Batch-touch` reads every key
byte, size, non-null time and class byte into a checksum. `Batch-all` selects all Object fields and
`Batch-size` only key+size. `Scan` is the complete owning API on the new dependency. Each number is the
mean of three scans; per-row costs divide the native benchmark counters by 4,000,000. Interference varied:
worker=1 all-fields was faster than the narrower projection, so these times cannot rank projection costs.

The 300K allocation guard observed 0.02269, 0.03615 and 0.04439 allocations/row for key+size, overview and
all fields, with a ceiling of 0.06. Validation and typed accessor loops allocate zero in isolation.
Allocation counts are much lower than owning Scan, but the design's proposed <0.01 allocations/row target
is not met; the roughly 375 B/row overview cost remains. A separate 300K-row allocation-space profile,
filtered to ScanBatches, attributes about 99% of sampled scan bytes to key-page decoding, including delta
output growth and the non-reusing key buffers. Page headers, projected-page wrappers, chunk setup and
codec state also allocate. This is allocated space, not peak or retained heap. No Stream implementation
change is included.

### Reader review fixes and decoder padding, 2026-11-10

R1's review fixes on swath main `b546596a`, with both modules pinned to SeekParquet `e7aa75951776`
(#48, decoder padding). The same benchmark cases, command, toolchain, GOMAXPROCS, host and 4M-row fixture
as above; the manifest SHA-256 is unchanged. Three timed scans per case, after warm-up, with planning
and splitting included. The shared-host load average fell from 11.31 to 7.12 during the run; timing is
still contended and cannot establish the commoncrawl acceptance target or a portable scaling result.

| Workers | Path | ns/row | allocs/row | B/row |
|---:|---|---:|---:|---:|
| 1 | Scan | 1,287.9 | 12.02852 | 709.4 |
| 1 | Batch-noop | 421.4 | 0.03383 | 184.0 |
| 1 | Batch-touch | 408.1 | 0.03383 | 184.0 |
| 1 | Batch-all | 440.7 | 0.04095 | 185.2 |
| 1 | Batch-size | 452.2 | 0.02041 | 182.4 |
| 4 | Scan | 797.0 | 12.02724 | 708.8 |
| 4 | Batch-noop | 217.3 | 0.03373 | 184.0 |
| 4 | Batch-touch | 288.9 | 0.03376 | 184.0 |
| 4 | Batch-all | 299.4 | 0.04068 | 185.5 |
| 4 | Batch-size | 198.2 | 0.02045 | 182.4 |
| 8 | Scan | 684.7 | 12.02701 | 709.9 |
| 8 | Batch-noop | 250.8 | 0.03377 | 184.4 |
| 8 | Batch-touch | 213.7 | 0.03378 | 184.3 |
| 8 | Batch-all | 227.6 | 0.04074 | 186.7 |
| 8 | Batch-size | 277.3 | 0.02049 | 182.6 |

Overview batches now allocate about 184 B/row, versus 375–376 B/row at `af57842`; all-field batches
allocate 185–187 B/row versus 449–450 B/row. SeekParquet #48 supplies the extra capacity its delta decoder
requires, avoiding repeated output-buffer growth. The reader also shares its size/time checks between
owning and borrowed paths in this run. Overview allocation counts remain about 0.034/row, above the
proposed <0.01 target; this is cumulative allocated space, not a peak/live-memory measurement.

### Reader Stream in one pass, 2026-11-10

`Stream` reading in one `seekparquet.Dataset.RangeBatches` pass (varve #124, R1b) against the paged `Stream`
it replaced (`Range-pages`: `Range` requests of 1,024 rows, each re-seeking after the last; kept in
`stream_test.go` as the oracle), in the same binary: swath main `4121c6fd` plus this change, both modules
pinned to SeekParquet `b1c848a4d2d6` (#50 OffsetIndex cache, #54 RangeBatches). `BenchmarkReaderStream`,
`-run '^$' -benchmem -benchtime 3x -count 3 -p 1`, under `flock /tmp/swath-gates.lock`, Go 1.27.4,
linux/amd64, GOMAXPROCS 4, the shared 4-vCPU Xeon Platinum 8581C; load average 1.1 at the start, 1.9 at the
end. The same 4,000,000-row, seed-1 fixture (2 parts, 20 row groups, 42.0 B/row on disk); warm Dataset;
counting-only callback; Workers do not apply. Indicative only.

| Path | ns/row (3 runs) | allocs/row | B/row |
|---|---|---:|---:|
| Stream (one pass) | 700, 703, 726 | 9.03 | 537 |
| Range-pages (before) | 1,235, 1,350, 1,713 | 12.14 | 1,696 |

About 1.8× faster and a third of the bytes on this fixture: the pass opens each projected chunk and its
OffsetIndex once per row group instead of once per 1,024-row request, positions once instead of re-seeking
each request's cursor, and copies
each value once (the row decoder reads a borrowed view of the batch; only kept values are copied) instead of
cloning rows. The remaining allocations are the owning Object's (key, size, time, ETag and class with their
pointers) and two the shared row decoder makes per row (the Object and the decoded row move to the heap),
which Scan and List share. An unsplit `PlanRanges` + `RangePlan.ScanBatches` pass with the same decoding,
measured as a probe in the same run (not committed), took 677–702 ns/row with the same allocations: within
this host's noise of the plan-free pass. `TestStreamAllocations` measured 9.04 allocations an object over its
20,000-row fixture (9.05 under `-race`), budget 10.5.

### Reader shard queue, 2026-11-10

`Scan` and `ScanBatches` split the plan into up to `shardsPerWorker` shards a worker and `Workers` goroutines
take them in order, in place of exactly `Workers` shards with one goroutine each (varve#124: on an 18-core
arm64 host and 57.9M commoncrawl objects, varve's overview took 3.9 s at one shard a core and 2.4 s asking for
eight a core). Swath main `1357c628` with the change, against the same tree with `scan.go` and `batch.go` as
on main, both with the same benchmark files; Go 1.27.4 (the module's toolchain), GOMAXPROCS 4, the shared
4-vCPU host above, the 4M-row seed-1 fixture (20 row groups, so a scan has at most 20 shards), under `flock
/tmp/swath-gates.lock`, other agents' test runs on the host throughout. `BenchmarkReaderScanSkewed` does 32
times the work an object in the first eighth of the keys ([testing.md](testing.md#reader-benchmarks)).

Shards per worker, five interleaved rounds (each round every binary once, the order rotated; `-benchtime
2x`), load average 0.8 to 4.4. ns/row, median of five (minimum in brackets); `old` is main. At 4 workers, 1,
2, 4 and 8 shards a worker are 4, 8, 16 and 20 shards; at 8 workers, 8, 16, 20 and 20.

| Case | old | 1 | 2 | 4 | 8 |
|---|---:|---:|---:|---:|---:|
| Skewed, 4 workers | 490.9 (488.7) | 485.2 (478.9) | 370.3 (364.9) | 261.7 (253.9) | 252.7 (248.0) |
| Skewed, 8 workers | 415.2 (407.9) | 397.8 (393.0) | 268.9 (265.6) | 357.8 (264.3) | 270.1 (266.4) |
| Batch-noop, 4 workers | 114.8 (112.1) | 122.6 (113.8) | 141.2 (122.4) | 136.0 (127.3) | 133.3 (118.2) |
| Batch-noop, 8 workers | 114.3 (112.8) | 114.9 (114.6) | 122.9 (119.2) | 116.8 (113.5) | 154.9 (111.3) |

With the skewed callback the queue at eight shards a worker scans in about half old's time at 4 workers
(253 against 491 ns/row) and two thirds at 8 (270 against 415); one shard a worker is old's schedule and
time. Four and eight a worker differ little here because the fixture's 20 row groups cap both near 20
shards. The even-cost `Batch-noop` shows no gain on four vCPUs, and these contended measurements cannot resolve
an even-cost regression: its medians went from 114.8 to 133.3 ns/row at 4 workers and from 114.3 to 154.9 at
8, while the minimums overlap (111 to 127 ns/row in every arm), which does not show the two equal. Eight a
worker allocates 185.0–185.5 B/row against old's 183.7–183.9 (each shard grows its own SeekParquet page
buffers) and 0.0003–0.0006 more allocations a row. The queue bounds the tail by the slowest shard still
running, not by an eighth of a worker's time: Split cuts only at row groups, and the callback's cost per
shard is unequal.

SeekParquet is pinned at `b1c848a4d2d6` here. Its open #55 (key-page buffers reused within a `ScanBatches`
call) keeps that scratch per call, so per shard under this queue: at most `Workers` sets in use, but their
growth repeats for every shard. The allocation and RSS of the two changes together are not measured.

Old against eight a worker, the `Scan`, `Batch-noop` and `Batch-touch` arms of `BenchmarkReaderScan` and the
skewed benchmark (`Batch-all` and `Batch-size` not run), three interleaved rounds at the default
benchtime, load average 3.7 to 9.1 (the medians carry that contention; the B/row and allocations do not):

| Workers | Path | old ns/row | new ns/row | old B/row | new B/row | old allocs/row | new allocs/row |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | Scan | 1,032.0 | 1,027.0 | 708.4 | 708.4 | 12.02779 | 12.02784 |
| 1 | Batch-noop | 310.0 | 304.2 | 183.5 | 183.5 | 0.03350 | 0.03354 |
| 1 | Batch-touch | 366.6 | 347.2 | 183.5 | 183.5 | 0.03353 | 0.03353 |
| 4 | Scan | 672.9 | 649.0 | 708.1 | 712.5 | 12.02682 | 12.02774 |
| 4 | Batch-noop | 206.9 | 172.6 | 183.5 | 185.1 | 0.03348 | 0.03413 |
| 4 | Batch-touch | 210.3 | 207.7 | 183.5 | 185.1 | 0.03351 | 0.03406 |
| 8 | Scan | 674.6 | 844.1 | 709.2 | 712.1 | 12.02656 | 12.02679 |
| 8 | Batch-noop | 188.1 | 175.5 | 183.8 | 184.9 | 0.03345 | 0.03377 |
| 8 | Batch-touch | 197.8 | 191.9 | 183.8 | 184.9 | 0.03350 | 0.03377 |
| 4 | Skewed | 564.0 | 391.6 | 183.9 | 185.5 | 0.03378 | 0.03417 |
| 8 | Skewed | 602.0 | 454.3 | 183.9 | 185.0 | 0.03366 | 0.03390 |

Workers 1 is unchanged by design (one shard). The 8-worker `Scan` medians (new 717–1,106, old 650–1,132
ns/row) overlap under this load. Scan time on many cores, where the imbalance varve measured lives, is not
measured here.

### Reader Totals in shards, 2026-11-10

`Totals` scanning in parallel shards over borrowed batches (varve #124, plain refresh) against the serial
`Totals` it replaced (`serial`: one `RangePlan.Scan` of owning rows plus a count plan per path; kept in
`totals_test.go` as `serialTotals`, the oracle), in the same binary: swath main `c97fda1b` (the shard queue,
eight shards a worker) plus this change, SeekParquet `586dcb7` (#55, key page buffers reused) in both modules.
`BenchmarkReaderTotals`, `-run '^$' -benchtime 3x -count 3 -p 1`, under `flock /tmp/swath-gates.lock`, Go 1.27.4,
linux/amd64, GOMAXPROCS 4, the shared 4-vCPU Xeon Platinum 8581C; load average 2.6 at the start, 3.4 at the
end. The same 4,000,000-row, seed-1 fixture (2 parts, 20 row groups); warm Dataset; 600 paths, root included,
nested up to eight levels (`benchTotalsPaths`). Each arm equals the oracle. Indicative only.

| Arm | ns/row (3 runs) | allocs/row | B/row |
|---|---|---:|---:|
| serial (before) | 765, 728, 747 | 6.03 | 427 |
| workers=1 | 195, 196, 199 | 0.021 | 4.7 |
| workers=4 | 100, 102, 104 | 0.021 | 9.9 |
| workers=8 | 102, 102, 103 | 0.021 | 9.9 |

Probes earlier the same day, on SeekParquet `b1c848a4d2d6`, not committed: the serial `Totals` spent about
260 ms planning the 600 ranges (planning positions at both ends of every range, decoding a key page at each)
and about 280 ms on 600 count plans, against 0.4 ms to plan the one range not inside another. `Totals` plans
only the outermost ranges (the same rows) and takes each count from the rows its scan adds to the path. On that
pin, before the shard queue, Totals measured 187 B/row at every worker count against 183 for a no-op
`ScanBatches`; SeekParquet #55's key page reuse is most of the drop to 5–10 B/row.

The key order check across shards (`shardOrder`: two key copies per shard, the last key copied once per
batch, a comparison per shard boundary after the join) against the same binary with it stubbed out,
interleaved three times on the same fixture (load average 0.6 to 4.4 during the runs): `BenchmarkReaderTotals`
4.680 against 4.678 B/row at one worker and 9.919 against 9.908 at four (about 0.01 B/row); ns/row of Totals,
`Scan` and `Batch-noop` at 1, 4 and 8 workers overlapped between the arms in every case (Totals at one worker
190–259 against 191–196, `Batch-noop` 220–227 against 221–226), within this host's noise.

### Reader on the redesigned SeekParquet API, 2026-11-12

`go/reader` and the replay store moved to SeekParquet's redesigned read API (`Plan`/`Scan`/`Rows`,
`ColumnView` accessors in place of `[]parquet.Value`; candidate `22318bd745a0`, merged as `b26862f2b4c8` with
identical content) against swath main `3021bfd6` (SeekParquet `586dcb7fa54c`), each built from its own tree
with the same uncommitted peak-memory harness added. Go 1.27.4, linux/amd64, GOMAXPROCS 4, a shared 4-vCPU
Xeon @ 2.30 GHz loaded by other agents' runs throughout (load average 2 to 8): six interleaved rounds, each
round main's binary then this branch's. Reader: `SWATH_READER_BENCH_ROWS=1000000` (the seed-1 generated
fixture, [testing.md](testing.md#reader-benchmarks)), `reader.test -test.run '^$' -test.bench
'^(BenchmarkReaderScan|BenchmarkReaderScanSkewed|BenchmarkReaderStream|BenchmarkReaderTotals|BenchmarkScan)$'
-test.benchmem -test.benchtime 3x`; `BenchmarkScan` is its own 300,000-row fixture. Replay: `store.test
-test.run '^$' -test.bench '^BenchmarkWindowFill$' -test.benchmem` (10,000-row fills of a 200,000-key
dataset). `benchstat`, n=6; "~" is no significant difference (p ≥ 0.05). Indicative only.

| Benchmark | sec/op | B/op | allocs/op |
|---|---|---|---|
| ReaderScan Scan, 1 / 4 / 8 workers | −19% / −22% / −28% | −32% | −25% |
| ReaderScan Batch-noop, -touch, -all, -size (1, 4, 8 workers) | ~ (one −6%) | ~ | −0.03% to −0.5% |
| ReaderScanSkewed, 4 / 8 workers | ~ | ~ | −0.2% |
| ReaderStream Stream | ~ (1.083 → 1.016 s) | ~ | ~ |
| ReaderTotals serial / 1, 4, 8 workers | −4.7% / ~ | −2.4% / ~ | ~ / −0.6% |
| Scan, 1 / 4 workers | −19% / −19% | −33% | −24% |
| WindowFill owned, from "" / "d05/" | −12% / ~ | −4% | ~ |
| WindowFill batches, from "" / "d05/" | ~ / −10.5% | ~ | ~ |

`Scan`'s gain is its decoding: each object is decoded from the borrowed batch (the row decoder over a borrowed
row, the key cloned) instead of from an owning copy of every row, which the module's former row-callback
`RangePlan.Scan` made. The batch paths (`ScanBatches`, `Totals`, the window fill) read the same decoded
values through `ColumnView`'s typed accessors, which inline at swath's call sites (`-gcflags=-m=2`) with no
bounds check left in the per-value loops (`-d=ssa/check_bce`).

Keeping a callback's error apart from the module's (a callback's `seekparquet.Stop` is swath's error, not a
stop) in `Scan`'s and `ScanBatches`' own callbacks, against `ec1e5756`'s wrapper that added an indirect call
per batch: the same six interleaved rounds of `BenchmarkReaderScan` and `BenchmarkScan`, every sec/op, B/op and
allocs/op ~ (geomean +0.05% time).

Peak heap: the highest `/memory/classes/heap/objects:bytes` sampled every 50 µs above the level after a forced
GC, at GOGC=10, mean MiB of six rounds (one operation each). The wide projection decodes every column of
`swath/azblob-objects/v1` from a 400,000-row part, in pages of 16,384 rows, that lacks the 16 columns added
later; the absent-column fill packs a 200,000-row part lacking `etag`, `owner_id` and `checksum_algorithm`
(SeekParquet now holds a batch projecting an absent column to 4,096 rows).

| Workload | main | this change |
|---|---:|---:|
| Scan, 4 workers | 7.46 | 7.82 (~) |
| ScanBatches, all fields, 4 workers | 3.90 | 3.88 (~) |
| Stream | 4.07 | 3.98 (~) |
| Totals of "" | 2.69 | 2.77 (~) |
| Wide projection, 16 columns absent | 16.17 | 16.41 (~) |
| Window fill, 10,000 rows | 2.98 | 2.99 (~) |
| Window fill, 100,000 rows | 18.77 | 18.83 (~) |
| Window fill, 3 columns absent | 22.44 | 21.69 (−3.4%) |

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

## Listing index on a wide prefix, 2026-11-10

`swath-replay serve` with `--list-index` on and off (the [listing index](swath-replay.md)), one server at a time,
on the dev VM: 4 vCPU (Xeon Platinum 8581C @ 2.30 GHz), about 7.7 GiB RAM, Linux 7.0, shared. Seekparquet `go`
at `6acba1a0`, default flags otherwise. Fixture: a SeekParquet conversion of an `aws-public-blockchain`
recording, 143 million keys, 3.0 GB, 371 row groups (a private working copy; not in the repository). All
requests are S3 `list-type=2` over loopback with no injected latency.

**The wide listing.** Build `8eb53c04`, load average about 4. The four pages of
`delimiter=/&prefix=v1.1/stellar/parquet/pubnet/v1/` (1,000, 1,000, 1,000 and 984 common prefixes, following
continuation tokens), one sample each, first after startup, Python client wall clock:

| Page | on | off |
|---|---|---|
| 1 | 43.3 ms | 118.8 ms |
| 2, 3, 4 | 1.8, 1.8, 1.8 ms | 81.4, 80.8, 80.9 ms |

Byte-identical on and off (`cmp`), as were seven other delimiter and plain listings in the same run, which are
measured under control below. Over its eight rollups `delimiter.skipscan.decoded_key_rows` averaged 2,560 key
values per rollup with the index (max 5,120) and 512,112 without (max 1,026,048); `page_reseeks` 0.6 against
479; `delimiter.index.skips` counted 3,984 and `delimiter.index.declines{dense_page}` 11 (no other declines).

**Controlled repetition.** The single-sample run above also showed slower plain and root listings with the index
(14.2 and 9.3 ms against 2.9 and 5.4 ms; 7.5 against 1.7 ms). An independent review re-measured those shapes:
build `52632f60` (the same code), load average 3.5 to 3.9, no other build or test jobs. Each server process warmed
the four shapes five times, then measured each 30 times, interleaved (plain page 1, plain page 2 from its
continuation token, root, wide). Default-cache processes ran on, off, off, on, so each default-cache cell is 60
samples; window-cache-off controls (`--prefetch-window-rows=0`) ran off, on, 30 samples each. Times are the
client's, the full body read over one persistent HTTP/1.1 connection; p90 is nearest rank. Every response hash
matched across on, off and both cache modes. Plain and wide use `prefix=v1.1/stellar/parquet/pubnet/v1/` and
`max-keys=1000`; root and wide use `delimiter=/`.

| Request | on, median / p90 ms | off | on, window cache off | off, window cache off |
|---|---:|---:|---:|---:|
| Plain page 1, 1,000 keys | 2.407 / 3.878 | 2.465 / 4.028 | 2.474 / 4.350 | 2.516 / 2.627 |
| Plain page 2, 1,000 keys | 0.571 / 0.646 | 0.571 / 0.713 | 2.460 / 2.624 | 2.461 / 3.384 |
| Root delimiter, 2 prefixes and 3 keys | 1.488 / 1.618 | 1.513 / 1.962 | 1.408 / 1.497 | 1.468 / 2.820 |
| Wide delimiter page 1, 1,000 prefixes | 1.366 / 1.751 | 81.061 / 83.225 | 1.278 / 1.381 | 81.413 / 82.783 |

The plain and root slowdown does not reproduce: the two default-cache process pairs agree (plain page 1 on
2.405 and 2.410 ms against off 2.487 and 2.436; root 1.485 and 1.493 against 1.521 and 1.501). Per-request read
work, from the store's reader with the window cache bypassed (5 warm-ups, 30 reads per shape and arm, counts
constant across samples, the 1,001-entry lookahead included):

| Request | key / value pages, on : off | keys decoded | groups opened | reseeks | source reads | index skips / dense declines, on |
|---|---|---|---|---|---|---|
| Plain page 1 | 2/12 : 2/12 | 2,048 : 2,048 | 1 : 1 | 0 : 0 | 23 : 23 | 0 / 0 |
| Plain page 2 | 2/12 : 2/12 | 2,048 : 2,048 | 1 : 1 | 0 : 0 | 23 : 23 | 0 / 0 |
| Root delimiter | 2/12 : 2/12 | 2,048 : 2,048 | 1 : 1 | 1 : 1 | 24 : 24 | 0 / 2 |
| Wide delimiter | 1/0 : 1,001/0 | 1,024 : 1,025,024 | 1 : 45 | 0 : 956 | 1 : 1,001 | 1,000 / 0 |

Source bytes were equal for the plain and root shapes (74,394, 73,249 and 106,452); the wide page read 1,853
bytes with the index and 603,547 without. The root listing's two index probes decline at dense pages before
moving the scan, so it does the same page work as without the index. The server's meters agree: per request,
plain page 1 is one cold window fill of 1,001 rows and plain page 2 a window hit in both arms; root is one rollup
of 2,048 decoded keys, one group open and one reseek in both arms, plus two `delimiter.index.declines{dense_page}`
with the index; wide is 1,000 `delimiter.index.skips` and 1,024 decoded keys against 1,025,024 keys and 956
reseeks.

**The build.** Build `8eb53c04`:

```
swath_replay list_index state=built max_bytes=67108864 workers=4 groups_indexed=371 groups_skipped=0 groups_failed=0 parts_failed=0 pages=139818 entries=8288 bytes=1173440 budget_exhausted=false has_index=true seconds=9.391
```

It adds 9.4 s before the startup line (every key page decoded once; 12.3 and 12.6 s in two more starts minutes
later, and 8.9, 8.9 and 8.7 s in the review's three starts, with the same stats) and retains 1.1 MiB. Peak RSS
over the run (`/usr/bin/time -v`) was 147 MB with the index and 97 MB without. `index.load.latency`, which
excludes the build, read 879 ms with the index and 157 ms without in the first pair, and 201, 179 and 194 ms
(on, off, on) in three later starts: the 879 ms was a one-off.

## Go and Java replay servers on converted fixtures (#352), 2026-11-11

The functional comparison of the Go and Java replay servers for #352: the Java server on each Java-era fixture,
the Go server on its footer-only SeekParquet conversion with the list index on (its default). One GCP Batch run,
`20261111-0013`, of [`bench/replay/java-compare.sh`](https://github.com/varveio/swath/blob/java-final/go/bench/replay/java-compare.sh) (at tag `java-final`), 00:14:30 to 00:53:00 UTC,
verdict PASS, task status 0; receipts in
`gs://s3-listing-study-results-29c02004/swath-352/20261111-0013/receipts/` (`summary.md`, `summary.json`, one
directory per step). It is functional evidence and descriptive startup and resource figures, not a performance
comparison: capacity is the s3-listing-study replay qualification's (varveio/s3-listing-study#37), not measured
here.

**Configuration.** swath `ff20e3df` (both servers, the lister, `replaydiff`, `diffds`); the converter
`bench/replay/adoptfixture` at `cd453c59` (`archive/replay-adopt-fixture`, built in a separate checkout, not
merged). One n4-standard-16 (16 vCPU, Xeon Platinum 8581C, 64 GB), Spot, nothing else running; container
`eclipse-temurin:25-jdk-noble` (Temurin 25.0.4+7, the base `Dockerfile.replay` pins), Go 1.27.4, Python 3.12.3,
DuckDB 1.4.1. The Java server at the launcher's defaults (no `SWATH_REPLAY_OPTS`, `JAVA_OPTS` or
`JAVA_TOOL_OPTIONS`; maximum heap 16.9 GB), both servers `--serving-mode sorted` with their other flags at their
defaults. The binaries' SHA-256 and `go version -m` are in `build/`.

**Fixtures and conversion.** The Java-written fixtures of [`make-fixtures.sh`](https://github.com/varveio/swath/blob/java-final/go/bench/replay/make-fixtures.sh) (at tag
`java-final`, since replaced by the Go `bench/replay/fixtures`): synthetic-10k, synthetic-1m (sorted, and rolled into 6 parts
and 110 row groups) and evidence-gcs, written by the Java `sort-fixture` in the run; the study's NARA
(`nara-1950-census`, 13,540,310 rows, 4 parts), FourCast (`noaa-nws-fourcastnetgfs-pds`, 4,081,170, 1 part)
and blockchain (`aws-public-blockchain`, 143,008,674, 4 parts) Java-era fixtures, their pinned study digests
checked. Every part of every conversion: input and output SHA-256 equal to the manifest's, the bytes before
each footer (located from each file's own Parquet trailer) of equal extent and byte-identical, and
`seekparquet validate --level full` complete. The three study conversions' parts are byte-identical to the
study's own staged conversions. evidence-azure is not compared: the Java `sort-fixture` refuses its names, so
there is no Java-era fixture; its coverage stays the Azure evidence replay's.

**replaydiff** (the Java server with `-fixture` on the Java-era fixture, the Go server with `-go-fixture` on
its conversion; 1M fixtures: walks `1,7,1000` (Azure `1,7,5000`) at `-walk-limit 400`, then a `-full` run of
the largest page size to the end with `-random 0`): 18 runs, 29,150 requests compared (336,698 HTTP requests),
0 failed, 0 skipped.

| Fixture | S3 compared (divergences, residuals) | GCS compared (by token effect) | Azure compared (by marker effect) |
|---|---|---|---|
| synthetic-10k | 1,948 (129, 2) | 2,082 (39,057) | 1,807 (35,062) |
| synthetic-1m sorted | 1,948 (129, 2); full 1,392 (109, 2) | 2,082 (5,746); full 1,568 (4,096) | 1,807 (5,059); full 1,293 (1,548) |
| synthetic-1m rolled | 1,948 (129, 2); full 1,392 (109, 2) | 2,082 (5,746); full 1,568 (4,096) | 1,807 (5,059); full 1,293 (1,548) |
| evidence-gcs | 1,103 (192, 2) | 1,068 (4,401) | 962 (3,642) |

Every difference reported is one of [replay-java.md](replay-java.md)'s accepted deviations: the documented S3
divergences (Go answering as live S3; each listed with both answers), the request-framing residuals (two of the
five sent per S3 run, reported and not judged), the Azure route's error documents of the S3 route (3 a run),
and token and marker bytes, which bind each fixture's identity (GCS and Azure answers that agree once each token
or marker is read for its effect; the six S3 runs also list 3,504 requests whose answers differ only in token bytes, and each Azure run's S3 check one more).
No Azure answer needed the segment improvement (`-accept-segment-improvement` counted 0 in every Azure run).
The three GCS foreign-token cases per run (each server sent the other's token) were refused by both servers.

**Full listings and startup.** `swath list s3://BUCKET --workers 64` through each server, then
`bench/diffds.py` (its streaming method on NARA and blockchain, the SQL method on the others): both listings of every fixture had the fixture's row count,
and diffds found every pair identical (rows, ordered sequence, multisets, key order and bounds), blockchain's
143,008,674 rows included. Startup is exec to the first 200 from `/healthz`, median of 3 starts (the ranges were
within 5%), and VmRSS then; the starts are fresh processes without dropping the OS page cache, so
cache-uncontrolled and generally warm (the fixtures had just been written, converted, hashed and read), and for
each fixture the Java server started first. Ready ms and RSS at ready are those startup medians; the listing columns are
the listing's wall time and the server's peak RSS over the listing's own start and the listing (VmHWM).

| Fixture | Server | Ready ms | RSS at ready MiB | Listing wall s | Listing VmHWM MiB |
|---|---|---:|---:|---:|---:|
| synthetic-10k | Java | 1,242 | 204 | 0.5 | 615 |
| | Go | 25 | 16 | 0.2 | 71 |
| synthetic-1m sorted | Java | 1,249 | 202 | 1.7 | 1,245 |
| | Go | 78 | 22 | 0.9 | 452 |
| synthetic-1m rolled | Java | 1,866 | 248 | 1.9 | 1,427 |
| | Go | 92 | 28 | 0.9 | 467 |
| evidence-gcs | Java | 1,234 | 210 | 0.3 | 256 |
| | Go | 26 | 14 | 0.1 | 28 |
| NARA | Java | 1,746 | 252 | 13.5 | 7,795 |
| | Go | 506 | 34 | 9.2 | 1,182 |
| FourCast | Java | 1,315 | 203 | 8.0 | 6,932 |
| | Go | 146 | 37 | 5.8 | 1,404 |
| blockchain | Java | 2,845 | 607 | 84.5 | 7,985 |
| | Go | 3,591 | 95 | 81.6 | 1,487 |

The Go server builds the list index before it serves: 3.38 s of blockchain's start (371 row groups indexed,
139,818 key pages, 1.2 MB retained), 0.46 s of NARA's, 0.11 s of FourCast's, 0.04 s of synthetic-1m rolled's
(its `list_index` startup line, in `logs/`). The listings are single runs, one server at a time, for the row
comparison; their wall times are recorded, not compared.

**Runs before this one** (receipts beside it, under `swath-352/`): `20261110-2248-probe` (synthetic-10k only,
swath `2f9805bd`, before the review fixes to the script) passed. `20261110-2303` (swath `32050133`) failed on
blockchain only: every replaydiff run passed and six of seven fixture pairs were diffds-identical, both
blockchain listings had 143,008,674 rows, but `diffds.py`'s SQL `EXCEPT ALL` ran out of memory (50.2 GiB of
50.2 GiB pinned); diffds gained its bounded streaming method for that. `20261110-2354` (swath `3e46d3d2`, the
first streaming method, since revised after review) was cancelled and superseded by this run. After this run
`969dfd9a` changed only how diffds's streaming method compares float columns, which the listing schema does not
have.

## Parquet pages at zstd SpeedFastest (2026-11-11)

Parquet pages are compressed at klauspost `SpeedFastest` (`parquetout.pageLevel`, writer recipe
`swath/parquet-r4`) where `swath/parquet-r3` used `SpeedDefault`; the other codec options are unchanged, and
the spill codec was already at `SpeedFastest`.

**The codec, on the build host** (a shared 4-vCPU VM, Xeon Platinum 8581C, not a benchmark machine).
`BenchmarkPageCodec` (`SWATH_CODEC_BENCH_PART=PART go test ./internal/parquetout -run X -bench
BenchmarkPageCodec -benchtime 3x`) decompresses every page of a part and compresses each again on one state at
both levels, the codec's other options kept. The parts were written by the `swath/parquet-r3` build from
aws-public-blockchain's keys (the study's replay fixture `current-eba31d9`, its first part): two prefixes,
`v1.1/sonarx/` (1,060,892 keys of mixed shapes) and `v1.1/stellar/ledgers/pubnet/FE` (4,108,000 keys of the
shape that holds most of the bucket), served by `swath-replay` from a 5,168,892-row subset with no injected
latency and listed by `swath list` into a local sorted dataset (4 encoders). Medians of three runs:

| Pages | Level | ns a page | Bytes a page | Ratio |
|---|---|---|---|---|
| sonarx, 9,353 pages, 68.6 MB raw | SpeedDefault | 19,798 | 3,150 | 2.330 |
| | SpeedFastest | 16,286 (-17.7 %) | 3,186 (+1.1 %) | 2.304 |
| stellar FE, 36,168 pages, 239.6 MB raw | SpeedDefault | 21,169 | 2,718 | 2.437 |
| | SpeedFastest | 18,032 (-14.8 %) | 2,723 (+0.2 %) | 2.433 |

The ETag column (hex, about half the compressed bytes) and the size column compress the same at both levels;
the difference is in the key column (sonarx +5.2 %, stellar -0.9 %) and `last_modified` (+5.6 % and +4.9 %).
The same listings end to end (three runs a build, alternating, the replay server on the same host; process CPU
from `/usr/bin/time`, medians): sonarx 30,404,915 to 30,736,985 bytes of output (+1.09 %), CPU 4.42 to 4.29 s
(-2.9 %); stellar FE 101,705,572 to 101,868,044 bytes (+0.16 %), CPU 15.16 to 14.89 s (-1.8 %). Those runs
are listing-bound, so wall time and peak RSS show no difference beyond their noise. The outputs read back the
same: the swath reader's row digests and DuckDB's `EXCEPT ALL` both ways match, and `seekparquet validate
--level digest` completes on both builds' datasets.

**A diagnostic replay A/B on Batch**, not a calibrated benchmark: the whole aws-public-blockchain fixture
(143,008,674 objects) replayed to a sorted dataset written to GCS, on 4 and 8 vCPU with 4 GiB of memory and a
30 GB disk, three blocks of the `SpeedFastest` build against `main` (`swath/parquet-r3`). Ratios
`SpeedFastest`/`main`, with the interval the A/B reports:

| vCPU | Wall | CPU a object | Tail |
|---|---|---|---|
| 8 | 0.97 [0.89, 1.04] | 0.965 | 0.92 |
| 4 | 0.96 [0.91, 1.09] | 0.95 | 0.94 |

The dataset grew 0.3 %, from 3,317 to 3,327 MB. So the level saves 3.5 to 5 % of the CPU an object costs and
6 to 8 % of the tail, for 0.3 % more stored and uploaded bytes on this bucket (up to 1.1 % on the mixed-shape
prefix above); wall time moves within the A/B's interval.

## Parquet fill: repeated values and nulls built once (2026-11-11, microbench)

A microbenchmark on the build host (a shared 4-vCPU VM, Xeon Platinum 8581C), not a run. The fill turns a
page's rows into each column's parquet values (`parquetout.fill`, `fillKey`). In a profile of the tail of a
sorted aws-public-blockchain run (143M objects to GCS, 8 vCPU, 256 workers, at `855001c8`) it took 40.8 of
194 CPU-seconds. The fill allocates nothing; nearly all of its time was building each value: parquet-go's
`Value.Level` is not inlined (cost 93 over the budget of 80), and each call copies the 24-byte value in and
out, then into the page's slice. The accessors cost about 5 % of it.

The fill now builds a column's null value once, and reuses a string value equal to the previous row's (a
storage class, a checksum algorithm or type repeat down a page) instead of building it again. The values
are the same, kind, bytes, levels and column, so the parts are byte-identical. A reused value points at the
earlier row's equal bytes. `BenchmarkFill` (`go test ./internal/parquetout -run X -bench BenchmarkFill`)
fills every column of a 1,024-row S3 page shaped like aws-public-blockchain's: ledger keys, hex ETags,
STANDARD, a CRC64NVME full-object checksum on every row (as in the fixture), no owner, and each row holding
its own strings. `distinct=true` alternates every string column row to row between two values of different
lengths: a synthetic case where no value is reused and each comparison ends at the length (long equal-length
near-matches, which compare more bytes, were not benchmarked).
Five runs each, 0 allocations a row before and after:

| Page | Before (ns a row) | After (ns a row) |
|---|---|---|
| blockchain-shaped | 211.0 to 212.2 | 122.4 to 122.9 (-42 %) |
| synthetic: every string differing from the row before | 211.0 to 211.6 | 174.9 to 175.3 (-17 %, the nulls alone) |

`TestFillMatchesReference` compares every value with the earlier fill's, one by one, on these pages and on
a page of random repeats, nulls and empty strings; `TestFillReuseOutlivesNoPage` writes two pages whose
repeated strings own their bytes, poisoned once each page's flush returns, serially and column-parallel, and
requires the file of the same rows in ordinary strings. The run-level saving was not measured; at the profile's
share, a 42 % cheaper fill would be about 17 CPU-seconds of that run's 194-second tail.

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
([parquet-output.md](parquet-output.md#byte-identity)).

## Downstream reader compatibility

The changes in [../upgrade.md](../upgrade.md) were applied to a scratch copy of the downstream reader. It accepts a
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
([upgrade.md, "What a reader of the Java layout changes"](../upgrade.md#what-a-reader-of-the-java-layout-changes)). The Java tool's reader does not read format 2, by design.

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


### PR tier selection: costs used in the policy, 2026-11-10

The warm [PR run 42826108596](https://github.com/varveio/swath/actions/runs/42826108596),
`ac1ca731` with Go 1.26.10, spent **12 seconds** in `go-build`'s static cross-build step
(two commands for four targets plus the Windows journal test compile), **86 seconds** in the first
multiarchitecture image build, and **4 seconds** in the offline image smoke. The entire job took
2m47s, including Go/Buildx setup and cleanup. Cross-compilation is retained on affected-code PRs;
conditional packaging saves runner capacity but was not this run's critical path.

The [main run 42828142577](https://github.com/varveio/swath/actions/runs/42828142577), `c3b93d33`,
spent 197.289s in `cmd/swath` and 188.071s in `internal/listing` inside the 4m05s non-race arm64 job.
Those package times overlap; they must not be added. Both packages also ran in arm64 race shards.
Ordinary PRs now omit that second execution and derive the remaining arm64 packages from the shard
table. Full platform runs retain it. No test cases or race shards were removed.

These are individual hosted observations, not a cold-cache benchmark or a measured speedup for the
new scope policy. Skipping unaffected nested modules likewise saves work and queue capacity; their
short jobs were not usually the critical path. The provider shard's variability can still dominate.
