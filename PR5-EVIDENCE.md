# PR 5 ZSTD1 segment-codec evidence

## Protocol

This is the WP0.8 live-listing experiment at
`c5560c9f67a762f0b583cdefb1426ed75f32d495` on 2026-08-31. Each arm used a fresh JVM and
listed the full public `s3://sorel-20m` bucket anonymously from a GCP VM in `us-east1-b` to
S3 in `us-west-2`. Runs were interleaved LZ4, ZSTD1, LZ4, ZSTD1, LZ4, ZSTD1. Three samples
per arm were taken because every run was much shorter than 15 minutes. The public bucket is
mutable and the measurements are network-dependent; interleaving is the noise control. No
run had visibly anomalous listing throughput, failed, retried materially, or required a redo.
Every run listed exactly 9,919,142 rows.

The client was an x86_64 GCP VM with 4 physical / 8 logical Intel Xeon Platinum 8581C CPUs,
15 GiB RAM, no swap, Linux 6.17, and a 193 GiB ext4 root disk with 30 GiB free before the
experiment. The runtime was Temurin 25.0.4+7. The CLI was built with
`./gradlew :swath-cli:installDist`; the launcher reported `swath 0.2.5-SNAPSHOT`, commit
`c5560c9f67a7`.

The common invocation was:

```bash
JAVA_HOME=/home/sagi_varve_io/.jdks/jdk-25.0.4+7 \
JAVA_OPTS='-Xms2g -Xmx8g -Dswath.sort.segment-codec=CODEC <common JFR option>' \
/usr/bin/time -v swath-cli/build/install/swath/bin/swath list s3://sorel-20m \
  --no-sign-request --region us-west-2 --concurrency 128 \
  --format parquet --sort -o RUN-out --report RUN-report.json \
  --progress --progress-interval 30s
```

`CODEC` was `LZ4` or `ZSTD1`. Every JVM used the same JFR CPU-time sampling configuration
from `docs/performance.md`. Fetch-worker CPU is the sum of 10 ms CPU-time samples attributed
to virtual threads; pack CPU is the inclusive `PageBlock.pack` sample sum, plus
`ZstdCompressCtx.compressByteArray0` samples for ZSTD1 because the native boundary separates
those samples from the Java caller stack. JFR lost 465–520 samples per run, so these two CPU
columns are comparative sampled CPU, not exact CPU accounting. Report `cpu_seconds` is the
authoritative process CPU measurement. `swath.sort.backpressure.wait` is reported directly
from `meters[]`; the rounded sort-summary engagement field was zero in every run. There were
no additional pack/lane wait timers. Handoff-queue and off-thread-buffer peaks were 1 in every
run. Each run's managed dataset, including temporary staging and final output, was deleted
after its report and profile values were captured.

## Per-run results

| Run | Codec | Rows | Staging bytes | Z/L staging ratio | `backpressure.wait` count / total / max ms | Fetch-worker sampled CPU s | Pack sampled CPU s | Process CPU s | Peak RSS bytes | Decode + merge ms | Listing ms | Run wall ms | Fresh-process wall s |
| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| L1 | LZ4 | 9,919,142 | 893,284,081 | — | 3 / 0.026383 / 0.018045 | 46.49 | 8.33 | 96.51 | 2,454,863,872 | 6,918 | 24,887 | 31,816 | 37.41 |
| Z1 | ZSTD1 | 9,919,142 | 633,565,914 | 0.709255× | 3 / 0.028438 / 0.020594 | 45.09 | 8.95 | 95.85 | 2,422,067,200 | 6,956 | 26,096 | 33,063 | 38.64 |
| L2 | LZ4 | 9,919,142 | 893,288,410 | — | 3 / 0.027339 / 0.017553 | 46.16 | 8.33 | 95.26 | 2,517,291,008 | 6,798 | 26,086 | 32,894 | 39.10 |
| Z2 | ZSTD1 | 9,919,142 | 633,567,851 | 0.709253× | 3 / 0.038363 / 0.029938 | 45.44 | 9.14 | 96.22 | 2,425,688,064 | 7,096 | 23,826 | 30,933 | 36.51 |
| L3 | LZ4 | 9,919,142 | 893,285,391 | — | 3 / 0.028212 / 0.019681 | 44.80 | 7.74 | 97.09 | 2,506,391,552 | 7,042 | 25,650 | 32,703 | 38.30 |
| Z3 | ZSTD1 | 9,919,142 | 633,565,562 | 0.709253× | 3 / 0.023829 / 0.015994 | 46.04 | 9.48 | 95.38 | 2,417,561,600 | 6,871 | 25,591 | 32,472 | 38.10 |

`Run wall` is the report's `duration_ms`; `Fresh-process wall` is GNU time around the launcher.
The paired staging ratios were 0.709255×, 0.709253×, and 0.709253×.

## Arm medians

| Metric | LZ4 median | ZSTD1 median | ZSTD1 / LZ4 |
| --- | ---: | ---: | ---: |
| Rows | 9,919,142 | 9,919,142 | 1.000000× |
| Staging bytes | 893,285,391 | 633,565,914 | 0.709254× |
| `backpressure.wait` total ms | 0.027339 | 0.028438 | 1.04020× |
| `backpressure.wait` max ms | 0.018045 | 0.020594 | 1.14126× |
| Fetch-worker sampled CPU s | 46.16 | 45.44 | 0.984402× |
| Pack sampled CPU s | 8.33 | 9.14 | 1.09724× |
| Process CPU s | 96.51 | 95.85 | 0.993161× |
| Peak RSS bytes | 2,506,391,552 | 2,422,067,200 | 0.966356× |
| Decode + merge ms | 6,918 | 6,956 | 1.00549× |
| Listing ms | 25,650 | 25,591 | 0.997700× |
| Report run wall ms | 32,703 | 32,472 | 0.992936× |
| Fresh-process wall s | 38.30 | 38.10 | 0.994778× |

## Gate verdict

**PASS; flip the default to ZSTD1.** ZSTD1 reduced median staging bytes by 29.07% while
every run retained the same row count and recorded zero actual sort-backpressure engagements.
The timer's three acquisition observations per run totaled only 0.024–0.038 ms, so its small
relative median increase is not sustained backpressure. ZSTD1's sampled pack CPU was 9.72%
higher, as expected, but fetch-worker sampled CPU, process CPU, and RSS did not increase. The
local decode-and-merge median was 38 ms (0.55%) higher, inside the observed run-to-run spread,
while median report wall was 0.71% lower and median fresh-process wall was 0.52% lower. The
required no-sustained-backpressure and no-final-wall-regression gates therefore both pass.
