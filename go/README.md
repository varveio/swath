# swath

Parallel listing and inventory for large object stores and filesystems.

swath lists a local directory tree, an S3 bucket, a GCS bucket or an Azure Blob container (flat namespace) in
parallel. It prints the rows as text, totals them per prefix (`swath du`), or writes a globally key-sorted
Parquet dataset: sorted parts and a `manifest.json` that commits them ([docs/dataset.md](docs/dataset.md)). It is
one static binary built with `CGO_ENABLED=0`.

> **Not released yet.** Build from source ([docs/install.md](docs/install.md)). The latest published release,
> [v0.3.2](https://github.com/varveio/swath/releases/tag/v0.3.2), is the earlier S3-only command line;
> [docs/upgrade.md](docs/upgrade.md) lists what changed.

```bash
./build.sh -o swath                         # CGO_ENABLED=0, the revision stamped (docs/install.md)
./swath du /some/root -d 2                  # totals per directory two levels down, as du
./swath list s3://bucket/logs/ | head       # text rows, in key order
./swath list /some/root -o /tmp/ds          # a sorted Parquet dataset
./swath list gs://bucket -o /tmp/ds         # a GCS bucket or prefix
./swath list az://acct/ctr -o /tmp/ds       # an Azure Blob container or prefix
./swath resume /tmp/ds                      # continue an interrupted object-store run
./swath show /tmp/ds                        # what a dataset or an interrupted run holds
```

## Documentation

- Using swath: [docs/README.md](docs/README.md), starting with [getting-started](docs/getting-started.md) and
  [concepts](docs/concepts.md).
- Reading a dataset from Go: the [`reader`](reader/README.md) module. Pinned ranged reads and create-only
  uploads in S3, GCS and Azure Blob: the [`objfile`](objfile/README.md) module.
- Contributing: [docs/internals/README.md](docs/internals/README.md) (architecture, contracts, testing,
  [measurements](docs/internals/measurements.md)).
- The replay server, `cmd/swath-replay`, serves a recorded listing through the S3, GCS and Azure APIs for tests
  and benchmarks: [docs/internals/swath-replay.md](docs/internals/swath-replay.md).

## Layout

| Path | What it is |
|---|---|
| `cmd/swath` | the command |
| `cmd/swath-replay` | the replay server |
| `internal/` | the lister's packages ([architecture](docs/internals/architecture.md#packages)) |
| `reader/`, `objfile/` | nested modules, importable on their own |
| `docs/` | the documentation; `go test ./docs` checks its links |
| `bench/`, `verify.py` | benchmarks and the independent DuckDB verifier |

The gate is `go vet ./... && go test -race ./...` ([testing](docs/internals/testing.md)).
