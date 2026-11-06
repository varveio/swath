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
| Java listing a Go dataset served by replay's sorted mode (1 part and 6 parts) | 2 |

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

Replay at each recording's worker-page p50 latency, 64 workers, `--seed`, sorted Parquet output, 3 interleaved
repetitions (Java defaults interleaved with the Go arm), median (min–max). Requests per 1k keys are computed
from the medians.

| Recording (objects, latency) | Arm | Wall s | Requests | Requests per 1k keys | Splits | Empty children | Overfetched keys | Mean active |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| noaa-eri-pds (845,693, 99 ms) | Java | 43.4 (43.0–43.5) | 1,353 | 1.60 | 94 | 100 | 148,904 | 7.7 |
| | Go | **3.7** (3.7–3.8) | 1,228 | 1.45 | 466 | 150 | 378,597 | 35.6 |
| pansurg-curation-raw-open-data (501,262, 164 ms) | Java | 66.4 (66.1–68.8) | 1,174 | 2.34 | 77 | 99 | 310,056 | 6.8 |
| | Go | **6.0** (5.1–6.2) | 803 | 1.60 | 395 | 168 | 298,042 | 22.9 |
| noaa-nws-fourcastnetgfs-pds (4,081,170, 99 ms) | Java | 83.2 (80.8–92.4) | 4,913 | 1.20 | 296 | 55 | 171,414 | 8.3 |
| | Go | **14.8** (14.6–15.6) | 5,803 | 1.42 | 2,337 | 1,106 | 1,720,578 | 43.6 |
| nara-1950-census (13,540,310, 103 ms) | Java | 109.4 (108.8–111.3) | 14,942 | 1.10 | 994 | 170 | 715,051 | 19.6 |
| | Go | **57.2** (55.9–58.1) | 20,928 | 1.55 | 9,847 | 4,504 | 7,383,588 | 40.4 |

Java's splits are its owner splits plus steals; its empty children are `CHILD_MASS/empty`; its overfetched
keys are `swath.page.raw_keys` minus objects. Go is 12x and 11x faster on the shallow recordings and 5.6x and
1.9x on the deep ones, at about 40 of 64 ranges active where Java runs 8–20. On the deep recordings it sends
more requests (+18 % and +40 % here) and overfetches far more keys; the next section explains why. Replay
inflates Java's serial tail; the live runs below are smaller wins.

**Reproduce:** with the replay oracle built and the recording under `$BENCH_DATA/recordings/BUCKET/`,

```bash
printf 'java-default\tjava\t\ngo-base\tgo\t/path/to/swath\n' > arms.tsv
REPLAY_ENV=/path/to/env.sh bench/policy_replay.sh arms.tsv noaa-eri-pds 99 19300 3 out/eri
python3 bench/policy_table.py out/eri
```

The harness runs Go arms as `swath diag list` with 64 workers and `--seed`; latencies are 99, 164, 99 and
103 ms for the four recordings.

## Splits, overfetch and requests on the deep recordings

A second interleaved session on the deep recordings (same configuration, 3 repetitions, median (min–max)):

| Recording | Arm | Wall s | Requests | Splits | Empty children | Overfetched keys | Mean active |
|---|---|---:|---:|---:|---:|---:|---:|
| fourcastnetgfs | Java | 81.8 (80.2–84.3) | 4,916 | 287 | 54 | 177,414 | 8.6 |
| | Go | 16.4 (16.0–17.0) | 6,078 | 2,464 | 1,093 | 1,751,569 | 40.4 |
| nara-1950-census | Java | 111.4 (109.9–114.0) | 14,961 | 1,015 | 170 | 709,051 | 19.3 |
| | Go | 55.0 (49.6–57.2) | 19,128 | 7,131 | 2,908 | 5,360,246 | 39.1 |

Per split (medians): overfetched keys 711 (Go) against 618 (Java) on fourcastnetgfs and 752 against 699 on
nara; empty children 0.44 against 0.19 and 0.41 against 0.17.

- Go splits 8.6x and 7.0x as often as Java. That buys about 40 active ranges against Java's 9–19 and 5.0x and
  2.0x less wall. Each split adds a trimmed last page and a child's first page, so requests (+24 % and +28 %)
  and overfetched keys follow.
- Per split, Go overfetches within 15 % of Java. The rest of the excess is empty upper children, which Java
  avoids by probing the pivot from an idle thief (see the 1-key probe below).
- S3 has no end bound, so a range's last page reads past it; GCS ranges send `endOffset` and overfetch nothing
  ([GCS route parity](#gcs-route-parity)).

**Reproduce:** as above, on `noaa-nws-fourcastnetgfs-pds` (99 ms) and `nara-1950-census` (103 ms).

## Policy switches

Same session and configuration as [the four recordings](#replayed-recordings-go-against-java-defaults), Go
arms only; wall s / requests, median of 3. `--floors` and `--structure` are `swath diag list` switches; the
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

**1-key pivot probe** (deleted), deep recordings, the session of the previous section: with the probe, empty
children fell 1,093 → 201 and 2,908 → 443, but it sent 2.8 and 2.3 probes per split, 31 % and 27 % more
requests, and took 2.3x and 2.2x the wall (fourcastnetgfs 38.0 s, nara 119.9 s), because the owner waits for
the probe. An asynchronous probe would save at most one request per empty child (0.41–0.44 per split) and
spend at least one per probe.

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
host; an earlier build on the same server took 11.67–11.77 s on GCS against 11.15–12.06 s on S3. No live GCS
bucket was listed.

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
  scope again from K, the last published part's max key (`keyrange` restarts one range `(K, ⊤]`), so everything
  the killed attempt had listed above K is listed again: the open part's rows, rows buffered in memory or in
  disposable spill for ranges ahead of a slow frontier, and parts closed but not yet recorded (a gap in the
  ordinals). That suffix is bounded by how far the listing ran ahead of the frontier (the row budget and spill
  cap, the workers and the time since the last published part), not by the part size: the part size is a target,
  not a hard maximum, and `--part-interval` bounds the age of the durable frontier, not the work done past it.
  On a slow first range the suffix can be most of the listing (the slow-first smoke above). Java resumed each
  range from its own durable cursor. The measurement campaign's relisted-work gate (relisted keys at most one
  part plus 5 % of the listed keys, with the relisted bytes, requests and the durable frontier's age reported)
  is a gate to test on kills at peak speculation; it is not a property this build guarantees, and no claim
  about restart economics should precede it.
- **Throttling.** The limiter starts at `--workers` unless `--limiter-slow-start` (off, unmeasured). Under
  sustained throttling it holds the limit at 1–2 for the whole run ([above](#throttling-through-the-fault-proxy)).
  It has never met real 503s.
- **Not built:** versions mode, Azure. Java CLI features deliberately not ported, each with its reason, are in
  [cli-inventory.md](cli-inventory.md).
- **Restored Java CLI features, tested in process only.** Text output (TSV, JSONL, table; stdout and files;
  gzip and zstd), the seven filters (Go RE2, not Java regular expressions), the stderr progress display and
  statistics, `-v`/`-q`/`--color`, bearer-token authentication, the report's `cost`, `efficiency`, `args_hash`,
  `session_ms` and `stop_source`, and `--part-rotation-max-rows` are covered by unit and command tests against
  fakes; none has been run against a live bucket or at scale, and no consumer of the report (the cloud
  importer included) has read the new fields.
- **Platforms.** Windows refuses journaled runs (no directory sync,
  [internals/durability.md](internals/durability.md)); Windows and macOS builds are compiled, not run. Power
  loss is untested.
- **Scale.** Billions of keys, runs of hours and 256 or more workers are unmeasured; the largest runs are the
  replay recordings and live buckets above (13.9 M objects).
- **Local benchmarks.** Cold-cache numbers are guest-cold in a VM and single runs; all trees are synthetic.

## Skipped or not measured

- Live GCS listing; GCS's own latency profile.
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
