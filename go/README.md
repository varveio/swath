# swath (Go port)

This directory is the Go port of swath: module `github.com/varveio/swath/go`, command `go/cmd/swath`, CLI name
`swath`. It lists a local directory tree or an S3 bucket in parallel and writes a globally key-sorted Parquet
dataset in Swath's sorted-dataset layout (`data/part-NNNNN.parquet` and `manifest.json`, the commit; format 2, [docs/upgrade.md](docs/upgrade.md)), as a
pure-Go (`CGO_ENABLED=0`) static binary. The Java distribution keeps its own `swath` launcher (`swath-cli`);
the two are separate tools. Documentation:
[docs/architecture.md](docs/architecture.md) (start here), [docs/cli.md](docs/cli.md),
[docs/testing.md](docs/testing.md). Measurements and findings: [REPORT.md](REPORT.md).

```bash
CGO_ENABLED=0 go build -ldflags='-s -w' -o swath ./cmd/swath
./swath list /some/root -o /tmp/ds          # list, order, encode, publish
./swath list /some/root --count-only        # list only
./swath list s3://bucket -o /tmp/ds         # an S3 bucket or prefix
./swath resume /tmp/ds                      # continue an interrupted S3 run
./swath show /tmp/ds                        # what a dataset or an interrupted run holds
python3 verify.py /tmp/ds                   # independent DuckDB check
go vet ./... && go test -race ./...
```
