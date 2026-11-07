# Measurements

The measurements the other docs and the pull request cite, each with its configuration and the harness that
reproduces it. Scripts are in [`../bench/`](../bench/); how to build the replay oracle and the fakes is in
[testing.md](testing.md).

**Host.** Unless a section says otherwise: 4 vCPU (Intel Xeon @ 2.10 GHz), 15 GiB RAM, ext4 on a virtio disk,
Linux 6.18 (Firecracker guest), Go 1.26. The host was shared and its speed drifted during sessions (steal time
seen in `vmstat`), so **compare absolute times only within one table**; each table's arms ran interleaved in
one session. Ratios held across sessions.

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

**Size.** Go datasets are 0.4–5.3 % larger than Java's after writing `etag` as `DELTA_BYTE_ARRAY` (1M fixture
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
Same configuration, 3 interleaved repetitions, median (min–max):

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

**Builds after candidate 1.** Each row of the tables below names the build it measured. The head of this change
(`7b05605`) was not re-swept on the iteration and held-out sets: it adds to candidate 4 a consistent total order
for the remainder rank (a decision change: three ranges could each outrank the next) and cancellation of
`order.Count` between children; the runs that name `7b05605` are the scale, one-core and study runs further down.

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

## Split admission on small lists

The keyfake S3 fake (`keyfake-static`, 50 ms a page, ListObjectsV2, path-style, anonymous) on 4 shared cores,
64 workers, `--count-only`, 3 interleaved repetitions, median; before is `go/engine` at `2cb204c2`, after adds
the admission gates and the remainder rank ([providers](internals/providers.md#range-engine-internalsourcekeyrange)).
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
http://127.0.0.1:19600 --path-style --anonymous --region us-east-1 --count-only --report R.json`.

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

**Verification read rate** (resume reads every retained part and checks its MD5): a 16 GiB, 4-vCPU VM, virtio
disk, 1 GiB in 16 files of 64 MiB: 247, 306 and 376 MiB/s with the page cache dropped before each of three
repetitions, 511 MiB/s warm, where MD5 on one core is the limit. 10,000 retained 64 MiB parts take roughly
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

## Local filesystem walker

Trees are synthetic (`bench/gen_tree.py`, seed 20261105): `realistic` is 971,927 non-directory entries and
99,892 directories (project trees, Hive partitions, home directories with spaces and non-ASCII names; sparse
files); `wide` is 1,000,000 empty files in 1,000 directories. Warm cache, 3 repetitions interleaved, median,
`/usr/bin/time`.

These were measured on the walker before the pipeline was restructured (the same `getdents64` and
dirfd-relative `fstatat` path; its `--engine`, `--fields` and `--order` flags no longer exist, and
`bench/run.sh` and `bench/compare.sh` still drive them). The current pipeline on the same tree, in one
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
row per entry. Not established: cold-cache behaviour of these tools, hard-link de-duplication, network or
overlay filesystems.

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
  not a hard maximum, and `--part-interval` bounds the age of the durable frontier, not the work done past it.
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
  [internals/durability.md](internals/durability.md)); Windows and macOS builds are compiled, not run. Power
  loss is untested.
- **Scale.** Billions of keys and runs of hours are unmeasured. The largest runs are the campaign's key lists
  through the fake (up to 150M keys) and the live nbm bucket (69M objects); 256 and 512 workers were measured
  through the fake only ([engine campaign](#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)).
- **Local benchmarks.** Cold-cache numbers are guest-cold in a VM and single runs; all trees are synthetic.

## Skipped or not measured

- Live GCS beyond the one private in-region run above, which measured the port before the engine campaign; the
  campaign's in-region GCS gate (that bucket within 12 s) has not run. GCS's own latency profile.
- The policy switches on the campaign's engine (the four recordings themselves were rerun, `recordings1`).
- Throttling against real S3 or GCS; Java under the sustained-throttle fault.
- The resume ramp-up after a restart, and the defaults and resume-storm campaigns (smoke only, above).
- EIO beyond ENOSPC (no fault-injecting filesystem); power-loss ordering.
- Cold-cache medians of the local walker; `readlink` cost (80 symlinks in `realistic`).

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
([upgrade.md, "The downstream reader: dispatch on schema id"](upgrade.md#the-downstream-reader-dispatch-on-schema-id)). The Java tool's reader does not read format 2, by design.
