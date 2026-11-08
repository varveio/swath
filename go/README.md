# swath (Go port)

This directory is the Go port of swath: module `github.com/varveio/swath/go`, command `go/cmd/swath`, CLI name
`swath`. It lists a local directory tree, an S3 bucket or a GCS bucket in parallel and writes a globally key-sorted Parquet
dataset in swath's sorted-dataset layout (`data/part-NNNNN.parquet` and `manifest.json`, the commit; a [SeekParquet](https://github.com/varveio/seekparquet) dataset, [docs/upgrade.md](docs/upgrade.md)), as a
pure-Go (`CGO_ENABLED=0`) static binary. The Java distribution keeps its own `swath` launcher (`swath-cli`);
the two are separate tools. Documentation: [docs/README.md](docs/README.md) says which page to read first, for
an operator ([docs/operators.md](docs/operators.md)), someone coming from the Java CLI
([docs/from-java.md](docs/from-java.md)), a contributor ([docs/architecture.md](docs/architecture.md)) or a
reviewer ([docs/measurements.md](docs/measurements.md)). The replay server `cmd/swath-replay`, which serves a recorded
listing through the S3 API for tests and benchmarks: [docs/replay.md](docs/replay.md).

```bash
./build.sh -o swath                         # CGO_ENABLED=0, the revision stamped (docs/operating.md)
./swath list /some/root -o /tmp/ds          # list, order, encode, publish
./swath count /some/root                    # list and count only
./swath list s3://bucket -o /tmp/ds         # an S3 bucket or prefix
./swath list gs://bucket -o /tmp/ds         # a GCS bucket or prefix
./swath resume /tmp/ds                      # continue an interrupted S3 or GCS run
./swath show /tmp/ds                        # what a dataset or an interrupted run holds
python3 verify.py /tmp/ds                   # independent DuckDB check
go vet ./... && go test -race ./...
```
