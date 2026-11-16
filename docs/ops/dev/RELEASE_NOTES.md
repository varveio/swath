# swath 0.4.0

<!-- Links are absolute and pinned to the tag: the release body does not resolve repository-relative links. -->

## User-facing changes

swath lists object stores and filesystems in parallel: it shows where the space went, and saves a sorted
Parquet inventory you can query without listing again. It reads listing metadata only and never reads, changes
or deletes what it lists. 0.4.0 is a new tool under the same name: one static binary, a new command line and
a new dataset format. [Start here](https://github.com/varveio/swath/blob/v0.4.0/docs/start-here.md); the
documentation is also at [swath.varve.io](https://swath.varve.io/).

- **Sources.** Amazon S3 general-purpose buckets and S3-compatible stores (`s3://`), Google Cloud Storage
  (`gs://`), Azure Blob Storage flat namespaces (`az://`), and local or mounted filesystems (a path).
  `--all-versions` lists S3 versions and delete markers, GCS generations, and Azure versions, snapshots and
  soft-deleted blobs ([Sources](https://github.com/varveio/swath/blob/v0.4.0/docs/sources/README.md)).
- **Where the space went.** `swath du SOURCE` on a terminal draws the largest prefixes and objects at any
  depth, each with its size, count, share of the total and newest date. `-d N`, a pipe or `--format` print
  totals per prefix as a table, TSV or JSONL
  ([Total a source](https://github.com/varveio/swath/blob/v0.4.0/docs/totals.md)).
- **An inventory you can query.** `swath list SOURCE -o DIR` writes a key-sorted Parquet dataset whose
  `manifest.json` commits its parts and records how complete the listing was. Query it with DuckDB, pandas,
  Spark or any Parquet reader; `swath show DIR` summarizes it, and `swath du --from-dataset DIR` totals it
  without listing again. `-o` a bucket prefix with `--work-dir DIR` writes the dataset to S3, GCS or Azure
  Blob. `swath list` also prints rows as a table, TSV or JSONL, or writes them to a `.tsv` or `.jsonl` file
  (optionally `.gz` or `.zst`)
  ([Query an inventory](https://github.com/varveio/swath/blob/v0.4.0/docs/query.md),
  [Output](https://github.com/varveio/swath/blob/v0.4.0/docs/output.md)).
- **Resume and refresh.** An object-store dataset run that is stopped or interrupted keeps its published
  parts and its journal, and `swath resume DIR` continues it. `--overwrite` keeps the previous dataset
  readable until the new one commits, and `--seed-from PREVIOUS` divides a relisting along the previous
  dataset's keys
  ([Stop, resume and refresh](https://github.com/varveio/swath/blob/v0.4.0/docs/resume-and-refresh.md)).
- **The run report.** A run accounts for itself in one JSON document (`--report FILE`, and `run.json` beside
  a dataset): the build, the command, the outcome and exit code, the timings, the requests made and their
  estimated cost. `progress.json` holds a running dataset's live counts, and `--log-format json` writes
  stderr as events ([Run report](https://github.com/varveio/swath/blob/v0.4.0/docs/reference/run-report.md)).
- **Install.** One archive per target (`linux-amd64`, `linux-arm64`, `darwin-amd64`, `darwin-arm64`,
  `windows-amd64`) holding static `swath` and `swath-replay` binaries, with `SHA256SUMS`, keyless cosign
  signatures, an SPDX SBOM and build provenance; images `ghcr.io/varveio/swath:0.4.0` and
  `ghcr.io/varveio/swath-replay:0.4.0` for linux/amd64 and linux/arm64. `go install` is not supported in
  0.4.0: build from a checkout ([Install](https://github.com/varveio/swath/blob/v0.4.0/docs/install.md)).

### Moving from 0.3.2

- **The command.** The commands are `swath list`, `swath du`, `swath show` and `swath resume`, and options
  are renamed or removed, without aliases. An option only one provider has carries its prefix
  (`--s3-region`, `--s3-profile`); an old option names what replaced it when you give it
  (`--force-path-style` is `--s3-path-style`)
  ([Unknown flag](https://github.com/varveio/swath/blob/v0.4.0/docs/troubleshooting.md#unknown-flag)).
- **Filters.** `--include` and `--exclude` take globs; regular expressions go to `--include-regex` and
  `--exclude-regex`. Common regular-expression spellings given as a glob (`'\.parquet$'`) are refused with
  exit 2, naming the regex option; others are valid globs that match something else (`--include parquet`
  matches only the key `parquet`), so move every regular expression to `--include-regex` or `--exclude-regex`
  ([Filters](https://github.com/varveio/swath/blob/v0.4.0/docs/filters.md#globs)). In sizes `KB`, `MB`, `GB`
  and `TB` are decimal; `K`, `KiB` and the like are binary.
- **Durations** are a number with a unit (`90s`, `1h30m`, `2d`). An ISO-8601 duration (`PT30S`), `none`,
  `off` and upper-case units are refused with exit 2; `0` turns off `--max-duration`, `--stall-timeout` and
  `--part-interval`.
- **Parts.** A sorted dataset's parts were about 1 GiB in 0.3. They are 64 MiB by default now, and a part
  open for 30 seconds with rows closes although not full, so part boundaries depend on timing. For parts
  about as large as before, give `--part-size 1GiB --part-interval 0`.
- **`--report FILE`** no longer creates its directory: a name in a directory that does not exist is refused
  with exit 2, before anything is listed.
- **The dataset.** A dataset is a SeekParquet dataset with the swath profile: `manifest.json`, written last,
  commits it (there is no `_SUCCESS`), `run.json` is its run report, and its columns are named per source
  under a schema id such as `swath/s3-objects/v1`
  ([The swath dataset format](https://github.com/varveio/swath/blob/v0.4.0/docs/dataset.md)). 0.4.0 neither
  reads a 0.3 dataset nor resumes a 0.3 run; `--overwrite` replaces the directory an interrupted 0.3 run
  left. List again with 0.4.0, and update queries to the new columns.
- **The image** `ghcr.io/varveio/swath` is distroless, without a shell: its entrypoint is `/opt/swath/swath`,
  it runs as `65532:65532`, and it cannot run `--s3-bearer-token-command` or a profile's
  `credential_process` ([Container images](https://github.com/varveio/swath/blob/v0.4.0/docs/install.md#container-images)).

The 0.3 documentation stays at the [v0.3.2 tag](https://github.com/varveio/swath/tree/v0.3.2/docs).

## Evidence

The release pipeline publishes a tag only after these gates pass
([Releasing swath](https://github.com/varveio/swath/blob/v0.4.0/RELEASING.md#what-gates-a-release)); the
results are in the tag's `ci.yml` and `release.yml` runs.

- `ci.yml` on `main` at that exact commit (`go-full-gate`): `go vet` and the race tests of the three Go
  modules, the default tier on macOS, Windows and Linux ARM64, cross-builds of the five targets, SeekParquet
  conformance, the replay fixture tier, and both images built for two architectures and smoked offline
  ([testing](https://github.com/varveio/swath/blob/v0.4.0/docs/internals/testing.md)).
- `release.yml` on the tag: `go vet` and the whole race suite of the main module unsharded, the five targets
  built once with each linux/amd64 binary's `--version` checked, and a smoke of the pushed image digests
  before they are tagged: an anonymous listing of a public S3 prefix into a dataset, read back with DuckDB
  from the manifest's part list, resumed as a no-op, and served by the replay image. The release stays a
  draft until its own signatures and attestations verify against the published assets.
- The license gate that generates `THIRD_PARTY_NOTICES` fails the build on a linked module whose license is
  not recognized or not allowed.
- Measured runs, each with its build, date, machine and command, are in
  [measurements.md](https://github.com/varveio/swath/blob/v0.4.0/docs/internals/measurements.md). In one
  live listing swath counted 1.09 billion S3 objects in 177 seconds, and in another wrote them as a sorted
  inventory in 846 seconds, on one 32-vCPU VM; 35 production listings of public buckets, 50.6 billion objects
  in all, completed. None was taken on this tag.

## Limits and known issues

- **Not listed:** Azure hierarchical namespaces (Data Lake Storage Gen2), S3 directory buckets, access points
  and Outposts, and GCS soft-deleted objects.
- **Resume:** only object-store runs that write a dataset resume. A filesystem listing, text output and
  `swath du` are run again.
- **Windows:** dataset runs and `swath resume` work as on Linux and macOS, on an output whose filesystem
  syncs directories (checked when the run starts). Power loss is safe on local NTFS; on ReFS or an SMB share
  a crash or kill is recovered, but a power loss may lose the latest part names. An `--overwrite` blocked by
  a file another program holds open fails naming that cause and leaves the previous dataset readable
  ([Known limits](https://github.com/varveio/swath/blob/v0.4.0/docs/troubleshooting.md#known-limits)).
- **S3-compatible stores:** swath trusts a store's empty last page; a store that returns one too early makes
  a listing miss keys ([#265](https://github.com/varveio/swath/issues/265)). Check a first run against a new
  store with an independent count
  ([S3-compatible stores](https://github.com/varveio/swath/blob/v0.4.0/docs/sources/s3.md#s3-compatible-stores)).
- **macOS Gatekeeper.** The binaries are not signed with an Apple Developer ID or notarized
  ([#420](https://github.com/varveio/swath/issues/420)). A browser download is refused until you clear its
  quarantine attribute, after verifying and extracting the archive (`darwin-amd64` on Intel):

  ```bash
  xattr -d com.apple.quarantine swath-0.4.0-darwin-arm64/swath swath-0.4.0-darwin-arm64/swath-replay
  ```

- Archives are built for the five targets above only; images are linux only.
- swath publishes the complete result of the live listing it performed. It is not a point-in-time snapshot
  of a bucket that changes during the run.
- The rest, and what has not been measured, is in
  [Known limits](https://github.com/varveio/swath/blob/v0.4.0/docs/troubleshooting.md#known-limits).
