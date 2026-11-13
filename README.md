[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**Parallel listing and inventory for object stores and filesystems.**

See what's taking up space in a bucket or a directory, and save a Parquet inventory you can query without
listing again. swath reads listing metadata only; it never reads, changes or deletes what it lists.

- `swath du` totals files and bytes per directory or prefix.
- `swath list` prints every entry as a table, TSV or JSON lines, or writes a sorted Parquet inventory with a
  manifest that records how complete it is.
- `swath du --from-dataset` totals a saved inventory again without listing.

Sources: Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage, and local or mounted
filesystems ([what each supports](https://swath.varve.io/docs/sources/)).

## Install

swath 0.4.0 is in release candidates: download one from the
[releases page](https://github.com/varveio/swath/releases) and verify it as
[Install](https://swath.varve.io/docs/install.html) describes. The documentation describes `main`, which has
more than the newest candidate, such as `swath du --from-dataset`. swath is pre-1.0, so flags and schemas can
still change. To build `main` with Go 1.26 or later:

```bash
git clone https://github.com/varveio/swath.git && cd swath && ./build.sh -o swath
```

The build leaves the binary at `./swath`; put it on your `PATH` (for example `install swath ~/.local/bin/`) to
run the examples as written.

## Try it

On a directory of yours, with no account or credentials:

```bash
swath du -d 1 ~/Downloads                          # totals per directory, one level down
swath list ~/Downloads -o downloads-inventory      # a sorted Parquet inventory
swath du --from-dataset downloads-inventory -d 1   # the same totals, from the inventory (main; not in rc.2)
```

On a public S3 prefix, without credentials:

```bash
swath du -d 1 s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1
```

Listing a bucket makes billable API requests; credentials otherwise come from the provider's usual chain.

## When it fits

`du` and `find` answer most questions about a small directory, and a scheduled provider inventory costs less
when a daily or weekly report is fresh enough. swath divides the key space into ranges and lists them in
parallel, which matters once a bucket holds tens of millions of keys. A billion-object S3 bucket lists in about
three minutes on one 32-vCPU VM
([how this was measured](docs/internals/measurements.md#a-billion-live-objects-sentinel-cogs-2026-11-07)).
[Start here](https://swath.varve.io/docs/start-here.html#when-swath-fits) compares the choices.

## Limits worth knowing

- Azure Blob Storage: flat namespaces only. An account with a hierarchical namespace (Data Lake Storage Gen2)
  is refused ([Sources](https://swath.varve.io/docs/sources/azure.html)).
- Only object-store runs that write a dataset resume; a directory listing, text output and `swath du` start
  over ([Stopping and resuming](https://swath.varve.io/docs/running.html#stopping-and-resuming)).
- Windows refuses dataset runs of S3, GCS and Azure Blob and `swath resume`, for lack of directory sync;
  listing a directory and text output work ([Platforms](https://swath.varve.io/docs/install.html#platforms)).
- macOS binaries are not notarized yet, so Gatekeeper blocks a browser download until you clear its
  quarantine attribute ([Install](https://swath.varve.io/docs/install.html#verify-and-install)).

## Documentation

The documentation is at [swath.varve.io](https://swath.varve.io/):
[Start here](https://swath.varve.io/docs/start-here.html),
[Install](https://swath.varve.io/docs/install.html),
[Your first inventory](https://swath.varve.io/docs/first-inventory.html),
[Sources](https://swath.varve.io/docs/sources/),
[Commands](https://swath.varve.io/docs/commands/swath.html),
[Dataset](https://swath.varve.io/docs/dataset.html) and
[Troubleshooting](https://swath.varve.io/docs/troubleshooting.html).

To work on swath, read the [contributor documentation](docs/internals/README.md) and
[CONTRIBUTING.md](CONTRIBUTING.md).

## License

[Apache-2.0](LICENSE). swath is by [Varve](https://varve.io).
