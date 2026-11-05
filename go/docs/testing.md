# Testing

```bash
cd go
go vet ./...
go test -race ./...
```

CI runs the same in the `go` job of `.github/workflows/ci.yml`, plus `CGO_ENABLED=0` builds for
linux/amd64, linux/arm64, darwin/arm64 and windows/amd64. A commit touching `go/` needs that job green.

## Invariant tests

| Invariant ([internals/contracts.md](internals/contracts.md)) | Test |
|---|---|
| no gap, no overlap; boundary to the left | `source/s3` T1 (random splits cover every key once, traced intervals tile the keyspace) |
| safe pivots | `source/s3` T2 (pivot properties over adversarial bounds) |
| guards | `source/s3` T3 (every clause) |
| paging, trimming, truncation errors | `source/s3` T4, `TestOutOfOrderListingFails` |
| split balance (synthetic) | `source/s3` T6 (`p/%07d`, 200k keys, 20 ms pages, 16 workers) |
| order and memory bound | `order` `TestOrderedOutput`, `TestSinkFailureStopsPromptly`, `TestCancellation` |
| deterministic output | `parquet` `TestRouterIsDeterministicAcrossEncoders`, `TestColumnParallelEncodingIsByteIdentical` |
| publication order and gate | `dataset` `TestCheck`, `TestPublishFailureLeavesNoSuccess`; `listing` cancellation and encoder-failure tests |
| overwrite safety | `dataset` `TestPrepareNeverDeletesTheTree`, `TestReplaceRemovesOnlyOwnEntries` |

## Oracles

- **Filesystem:** a sorted `filepath.WalkDir` (`TestListingEqualsSortedOracle`), exact sequence, both
  engines, every tier, with and without DIR rows, reorder budgets from 1 to unbounded, tricky names.
- **S3 parallel vs sequential:** the parallel core's dataset is byte-identical to the sequential
  continuation-token listing (`listing` `TestS3ParallelEqualsSequential`, on gofakes3).
- **Replay** (step 4): placeholder. Recorded listings served by `swath-replay`; row-identical to Java swath.

## The fake bucket

`bench/s3fake` serves a gofakes3 bucket loaded from a key list:

```bash
go run ./bench/s3fake -keys keys.tsv -bucket bench -addr 127.0.0.1:9000 &   # TSV: key, mtime (µs)
./swath list s3://bench -endpoint http://127.0.0.1:9000 -path-style -out /tmp/ds
```

`bench/s3_parallel.sh` runs the sequential and parallel wall table on it. Fake-bucket numbers are synthetic:
they measure client CPU and coordination, not S3 latency.

## verify.py

`python3 verify.py OUT` checks a dataset with DuckDB, independently of the Go code: `_SUCCESS`, `data/` holds
exactly the manifest's parts, row counts, strictly ascending raw key bytes within and across parts, manifest
`minKey`/`maxKey`, footer stamps, and for S3 datasets every canonical column against contracts §4.

## Fuzzing

`go test -fuzz FuzzParseDirents ./internal/source/fs` (the getdents record parser).

## Measurements

Benchmarks and their results are recorded in [../REPORT.md](../REPORT.md); scripts are in `../bench/`.
