# swath 0.5.0-rc.1

<!-- Links are absolute and pinned to the tag: the release body does not resolve repository-relative links. -->

## User-facing changes

swath lists object stores and filesystems in parallel: it shows where the space went, and saves a sorted
Parquet inventory you can query without listing again. 0.5.0-rc.1 continues 0.4.0: the same commands and the
same dataset format, a 0.4.0 dataset reads as before, and the changes below. It also makes swath a Go library.
[Start here](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/start-here.md); the documentation is also
at [swath.varve.io](https://swath.varve.io/).

This is a release candidate, a rehearsal of the publish path. Its binaries report `0.5.0-rc.1`; it is published
as a GitHub pre-release, and its images carry only the `0.5.0-rc.1` tags, with no `latest` and no `0.5`. It is
not promoted: 0.5.0 will be a fresh build
([Release candidates](https://github.com/varveio/swath/blob/v0.5.0-rc.1/RELEASING.md#release-candidates)).

- **`swath du` on a local tree leads with the space on disk**, as `du`, ncdu, gdu and dust do: the table's and
  the view's figure is `on disk`, `--sort size` orders by it, and the closing line gives it. A directory's own
  blocks count in it, as `du` counts them, so without a size or time filter a tree's total is below
  `du -sB1`'s by the source directory's own blocks (How sizes are counted lists the other differences).
  `-A` (`--apparent-size`) shows the lengths, as 0.4.0 did. A listing that reads no blocks (`--metadata names`), an
  object store and a saved dataset without allocated bytes lead with the lengths. `--allocated` now only
  requires the space on disk, and its heading and closing line say `on disk`
  ([How sizes are counted](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/totals.md#how-sizes-are-counted)).
- **A listing no longer misses keys silently on an S3-compatible store that ends it early.** MinIO answers a
  start key inside a folder with an empty or short final page while keys follow; 0.4.0 took that page as the
  end of the range and exited 0 (107,651 of 250,000 keys in testing). swath now checks each such page against
  the keys and folders the run has listed past it, or, where there are none, by one round trip of small
  requests these stores answer correctly, and asks again for a page proved wrong. A store that keeps omitting
  keys stops the run with exit 1, cause `omitted_keys`, and the run can be resumed. On the AWS listings
  measured, of 2,180 to 62,557 keys at 1 and 64 workers, the check added 1 to 6 requests a run
  ([2026-11-25](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#the-end-check-of-s3-listings-2026-11-25)).
  Versions listings (`--all-versions`) are not checked yet
  ([S3-compatible stores](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/sources/s3.md#s3-compatible-stores)).
- **Windows local trees.** The default stat tier fills `allocated_bytes` (the space on disk) and `inode` (the
  file ID) from the directory listing swath already reads, with no call per file, so `swath du` leads with the
  space on disk there too and takes `--allocated`. `--metadata links` opens each entry through a handle for its
  link count and gives `dev` the volume's serial number, so `swath du` counts a hard-linked file once and says
  so (`links_once`); it costs an open per entry, which the run report counts in `entry_handles`.
  `--one-file-system` is accepted, and a junction or a volume mount point is listed as kind `symlink` with its
  target in `link_target`, and never walked
  ([Local and mounted filesystems](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/sources/local.md#options-for-this-source),
  [Hard links](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/totals.md#hard-links)).
- **Local walks on Linux and macOS.** Every directory below the root is opened relative to its parent's open
  descriptor (`openat`, `O_NOFOLLOW`), never by its path, so a symbolic link swapped in for a directory while
  the walk runs is not followed and the walk does not leave its root through one; it counts in `unsafe_dirs`,
  and the report's diagnostics gain `dir_opens`. On macOS, `--one-file-system` also checks each opened
  directory's volume, so a scan rooted at `/` stops at the APFS Data volume even where its device number
  matches System's
  ([Local and mounted filesystems](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/sources/local.md#options-for-this-source)).
- **Azure Blob `--include` narrows the listing.** An `--include` with a literal leading prefix lists only that
  prefix, as on S3 and GCS. A prefix the source URL cannot carry beside its SAS is not narrowed, and the report
  says so (`invocation.narrowing` `provider_declined_prefix`)
  ([What narrows the listing](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/filters.md#what-narrows-the-listing)).
- **`swath show` and the manifest.** A dataset's manifest records the build that wrote it
  (`extensions.swath.written_by`: tool, version and revision); the member is optional, so older readers ignore
  it and older manifests open without it, and `swath show -v` prints it. `swath show` names a dataset's
  storage by its Parquet parts and schema, and `-v` adds the SeekParquet format id. On an interrupted run it
  prints when the run started, its command and the parts saved, and `-v` adds the journal version and the
  identity a resume must match
  ([swath show](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/commands/swath-show.md),
  [The swath extension](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/dataset.md#the-swath-extension)).
- **Live listing from Go.** Package `github.com/varveio/swath/source` opens an S3 bucket or an S3-compatible
  store, a GCS bucket, an Azure Blob container (a flat namespace) or a local directory (Linux and macOS) as a
  `Source`, and asks it during the program's own run for a page of a prefix's children, a lookup of one key, or
  a walk of everything under a prefix, with no swath process and no output files. `source.Open` takes an
  address (`s3://BUCKET`, `gs://BUCKET`, `az://ACCOUNT/CONTAINER`) through the providers the program imports;
  each provider also has its own typed `Open`. Every answer carries its `Evidence`, and an error matches one of
  `reader/listing`'s classes with `errors.Is`, but for the context's own error and, from a walk, an error the
  program's callback returned or a panic the walk recovered. An `objfile` `Observer` on the context also
  receives a live source's requests
  ([The Go library](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/library.md)).
- **Inventories, totals and the command from Go.** `reader/view` answers the same pages, lookups and walks from
  a saved dataset (`view.Open`, `view.OpenDir`), and `Summarize` gives the totals under several prefixes,
  optionally split by storage class and by size band
  ([The inventory view](https://github.com/varveio/swath/blob/v0.5.0-rc.1/reader/README.md#the-inventory-view)).
  `reader/du` sums a walk's entries per key prefix by du's rules, and `reader/du/tree` draws the view
  `swath du` shows on a terminal, with columns of the program's own; `swath du` now draws its view with it, and
  what it prints is unchanged
  ([The du tally](https://github.com/varveio/swath/blob/v0.5.0-rc.1/reader/README.md#the-du-tally)).
  `command.Main` runs the whole command line in a child process the program dedicates to it, and `command.List`
  runs one `swath list` in the calling process from typed `ListOptions`. It returns a `ListResult` and, for a run
  that did not complete, an error: an output another run holds, a source not found or denied, refused options
  and a `ctx` that ended match `ErrOutputInUse`, `ErrSourceNotFound`, `ErrSourceDenied`, `ErrUsage` and the
  context's error with `errors.Is`, and any other failure, of the output for one, matches none of them
  ([Run a listing from Go](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/library.md#run-a-listing-from-go)).
  Every package named here is experimental.
- **Smaller changes.** The `swath du` table heads the newest time under a prefix `newest (ZONE)`, as the
  terminal view does, instead of `last_modified (ZONE)`. A dataset of a local tree has no `cuts.txt`, since
  `--seed-from` does not take a local source. GCS reads on Windows retry a connection the server reset
  ([#689](https://github.com/varveio/swath/pull/689)). A GCS redirect's error message leaves out the target's
  query, and the Azure SDK's log never sees an error answer's body, so a SAS echoed in one is not logged.
- **Install.** One archive per target (`linux-amd64`, `linux-arm64`, `darwin-amd64`, `darwin-arm64`,
  `windows-amd64`) holding static `swath` and `swath-replay` binaries, with `SHA256SUMS`, keyless cosign
  signatures, an SPDX SBOM and build provenance; images `ghcr.io/varveio/swath:0.5.0-rc.1` and
  `ghcr.io/varveio/swath-replay:0.5.0-rc.1` for linux/amd64 and linux/arm64. The Homebrew tap
  (`brew install varveio/tap/swath`) is updated for final releases only and stays on 0.4.0 for this candidate.
  The root module's `go.mod` requires `reader` and `objfile` by version and carries no `replace`, so a Go
  program can require `github.com/varveio/swath`; the two nested modules keep their own versions
  ([Install](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/install.md)).

### Moving from 0.4.0

- **`swath du` on a local tree** leads with the space on disk: its figures, the order of `--sort size` and the
  closing line change. Give `-A` for 0.4.0's figures. TSV and JSONL rows keep both figures under the same
  names, but their `primary`, the order with `--sort size`, `allocated_bytes` (now with directories' own
  blocks) and `allocated_bytes_missing` (now counting directories without blocks) change, and with
  `--metadata statx` an entry the filesystem answers without blocks counts as missing. The run report's
  `totals` gains `primary_reason`, and the `read` event of `swath du --from-dataset -v` gains `primary` and
  `primary_reason`
  ([du rows](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/output.md#du-rows)).
- **The `swath du` table** heads its time column `newest (ZONE)`, not `last_modified (ZONE)`. TSV and JSONL keep
  the field `max_last_modified`.
- **`swath show`** prints less without `-v`: a dataset's `storage` line no longer names the SeekParquet format,
  and an interrupted run's output no longer gives the journal version or the identity a resume must match. `-v`
  prints them, and the build that wrote the dataset, from the manifest's new optional
  `extensions.swath.written_by`
  ([swath show](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/commands/swath-show.md)).
- **The run report's diagnostics** gain `dir_opens` on Linux and macOS local walks, `entry_handles` on Windows
  with `--metadata links`, and the `end_check` counters on S3; like every diagnostic, they may change in any
  release ([Diagnostics](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/reference/run-report.md#diagnostics)).
- **S3-compatible stores.** A listing that 0.4.0 completed with exit 0 and keys missing now fails with exit 1,
  cause `omitted_keys`; a supervisor that resumes on exit 1 when `error.json` says so resumes it, and the resume
  checks again ([Exit codes](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/reference/exit-codes.md)).
- **Windows local listings.** A junction or a volume mount point is kind `symlink` with its target in
  `link_target`, where 0.4.0 listed it as `unknown`. Rows at the default tier now carry `allocated_bytes` and
  `inode`, and `swath du` leads with the space on disk. `--one-file-system`, which 0.4.0 refused there (exit 1),
  is accepted and recorded among a dataset's filters, though it compares no device or volume (below).
- **macOS `--one-file-system`** stops at a different volume even where its device number matches, so a scan of
  `/` no longer enters the Data volume through `/Users` or `/System/Volumes/Data`. Select a directory on Data as
  the root to list it.
- **Local walks.** A symbolic link swapped in for a directory during a walk counts in `unsafe_dirs`; the
  getdents engine counted it in `open_errors`.
- **Local datasets** have no `cuts.txt`, and `--seed-from` does not take a local source. `--overwrite` removes
  the `cuts.txt` an earlier version wrote.
- **Azure Blob with an `--include` that has a literal leading prefix.** The run lists only that prefix: the
  report's `invocation.narrowing` says `narrowed` where 0.4.0 said `provider_lists_root`, `invocation.source`
  and the manifest's source and `scope.prefix` name the prefix, and `output.rows_filtered` counts only the rows
  under it. A 0.4.0 dataset of such a run no longer seeds it (`--seed-from` refuses it, exit 2), and an
  interrupted 0.4.0 run must be finished with 0.4.0: `swath resume` refuses it (exit 2)
  ([What narrows the listing](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/filters.md#what-narrows-the-listing)).
- **Programs that read datasets.** `reader.Dataset.CheckSchema`, and `view.OpenDir`, which runs it, refuse a
  column whose annotation SeekParquet does not recognize, which 0.4.0 took for an unannotated one. No swath
  writer writes such a column. The reader module now requires `golang.org/x/text`.
- **Programs that use SeekParquet through `reader.Dataset.SeekParquet()`.** The reader module, and the root
  module, use SeekParquet `go/v0.3.0`, so such a program follows its breaking changes: `Dataset.Page` and the
  named reductions (`Dataset.Aggregate`, `Dataset.AggregateRows`) are removed (a page is a `List` of one entry
  more, a count `PlanRows` and `Plan.Count`, a reduction `FoldRows`), a zero `FoldOptions.ValueBudgetBytes` is
  unlimited and a negative one refused, `Dataset.Columns` takes read statistics, and the repetition and
  time-unit constants name their type
  ([SeekParquet's changelog](https://github.com/varveio/seekparquet/blob/go/v0.3.0/go/CHANGELOG.md)). What
  swath's commands and its reader packages answer is otherwise unchanged.

## Evidence

The release pipeline publishes a tag only after these gates pass
([Releasing swath](https://github.com/varveio/swath/blob/v0.5.0-rc.1/RELEASING.md#what-gates-a-release)); the
results are in the `ci.yml` run of the push to `main` at the tagged commit and in the tag's `release.yml` run.

- `ci.yml` on `main` at that exact commit (`go-full-gate`): `go vet` and the race tests of the three Go
  modules, the main module's in shards; the tests that must not run under the race detector, and SeekParquet
  conformance; the default tier on Linux ARM64 and in the macOS and Windows legs; cross-builds of the five
  targets; the replay fixture tier; both images built for two architectures and smoked offline; and the
  acceptance suite on the five targets
  ([testing](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/testing.md#go-jobs-per-push)).
- `release.yml` on the tag, from a cold cache: first, that the tag names a commit on `main` whose `go.mod`
  carries no `replace` and requires nested-module tags that hold that commit's `reader/` and `objfile/`. Then
  the race tests of the main module, one leg per shard, every leg on linux/amd64, and one leg that builds the
  main module as an outside project does (`GOWORK=off`: `go build ./...`, `go mod tidy -diff`), runs `go vet` of
  the main and reader modules, checks that every test runs in exactly one shard, and runs the reader module's
  race tests. The five targets are built once, each linux/amd64 binary's `--version` checked. Before any image
  tag exists, the `--version` of both pushed image digests is checked, the lister image is smoked (an anonymous
  listing of a small public S3 prefix over HTTPS into a dataset, read back with DuckDB from the manifest's part
  list, then resumed as a no-op), and the replay image serves that dataset. The release stays a draft until its
  own signatures and attestations verify against the published assets.
- No release leg runs all the tests of `internal/inventory`, or all those of `command`, in one process under
  the race detector. Nightly's `go-race-nightly` does, and the release checklist requires it to have succeeded
  on the released commit, or on one with the same Go sources, before the tag is pushed.
- The license gate that generates `THIRD_PARTY_NOTICES` fails the build on a linked module whose license is
  not recognized or not allowed, or when the targets link different modules
  ([license gate](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/testing.md#license-gate)).
- New in this release, the black-box acceptance suite
  ([`acceptance/`](https://github.com/varveio/swath/blob/v0.5.0-rc.1/acceptance/README.md)): a Go module of its
  own, outside the workspace, that runs the built `swath` and `swath-replay` as a user does, with no network and
  no credentials, and checks the datasets with a pinned DuckDB CLI, not with swath. Its scenarios cover the
  command line and its errors, local trees with hostile names, listings served over the S3, GCS and Azure Blob
  APIs in exact byte order, filters, formats, totals and datasets, runs stopped by a deadline, killed or
  signaled and then resumed to the same parts as a run never stopped, a resume that reads back
  durable spill, a reader that closes the pipe early, and a 0.4.0 dataset read by `swath show`,
  `swath du --from-dataset` and `--seed-from` and replaced by `--overwrite`. `go-full-gate` requires it on
  linux-amd64, linux-arm64, darwin-arm64, darwin-amd64 and windows-amd64, once against the 0.4.0
  `swath-replay` and once against the commit's own; where behavior changed on purpose, each scenario keeps
  0.4.0's expectation beside the current one, and the published 0.4.0 archives are run against it whenever the
  suite changes
  ([acceptance suite](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/testing.md#acceptance-suite)).
- `internal/readbudget` pins the exact HTTP and metadata-request counts of the reader's operations over the S3,
  GCS and Azure Blob fakes. Its tables now also run `view.Inventory`'s `Page` and `Summarize` with their
  production settings, and every count was the same before and after the move to SeekParquet `go/v0.3.0`
  ([#696](https://github.com/varveio/swath/pull/696),
  [testing](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/testing.md#reader-benchmarks)).
- Measured runs, each with its build, date, machine and command, are in
  [measurements.md](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md). None was
  taken on this tag. On the 0.4.0 release, one live listing of 1.09 billion S3 objects wrote a sorted inventory
  in 289 seconds at 1,024 workers on one 32-vCPU VM
  ([2026-11-17](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#a-billion-live-objects-on-the-040-release-against-032-sentinel-cogs-2026-11-17)),
  and another in 238 seconds at 2,048 workers on one 64-vCPU VM
  ([2026-11-17](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#the-same-bucket-on-64-vcpus-at-2048-workers-sentinel-cogs-2026-11-17)),
  one run each. Since 0.4.0, each on a build of its change's branch before the change merged: the space on disk
  that the Windows directory listing reports, against a call per file, on a GitHub Windows runner
  ([2026-11-25](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#space-on-disk-in-a-windows-directory-listing-2026-11-25));
  the macOS volume checks on a GitHub macOS runner, whose small synthetic run resolves no added cost
  ([2026-11-23](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#macos-volume-boundary-checks-2026-11-23));
  and descriptor-relative opens on synthetic trees on a shared host, which cost no more than opens by path
  ([2026-11-17](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#descriptor-relative-opens-2026-11-17)).
- **What has run against the real services**, recorded in measurements.md and summarized per source in
  [What has run against the real service](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/sources/README.md#what-has-run-against-the-real-service):
  anonymous listings of public S3 buckets, the billion-object runs included; 35 production listings of public
  S3 buckets with 0.4.0-rc.2, 50.6 billion objects in all, each writing its dataset to a GCS bucket with default
  credentials
  ([2026-11-13](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#spill-at-billion-scale-and-a-spill-cap-too-small-2026-11-13));
  anonymous listings of a public Azure Blob container of 1.84 million blobs; a private GCS bucket of 5.1
  million objects, listed by an earlier build; and, of S3-compatible stores, MinIO. Versioned listings of seven
  small public S3 buckets and one GCS bucket were checked against an independent listing, as
  [Known limits](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/troubleshooting.md#known-limits)
  records; measurements.md has no record of those runs. New in this release, the end check of S3 listings ran
  against MinIO, where it stops the listing 0.4.0 completed with keys missing, and on AWS
  ([2026-11-25](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#the-end-check-of-s3-listings-2026-11-25)).
  The rest of what is new, the library's live sources included, has no run against the real service recorded
  in measurements.md; it is tested against fakes, emulators, replays and CI's Linux, macOS and Windows
  runners. Still not run against the real service: credential renewal during a run, on any service (S3's
  credential refresh is [#577](https://github.com/varveio/swath/issues/577)); Azure Blob with a SAS, Shared Key
  or Entra ID ([#346](https://github.com/varveio/swath/issues/346)); a service throttling a run
  ([not measured](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/internals/measurements.md#skipped-or-not-measured));
  and datasets written to S3 and Azure Blob buckets.

## Limits and known issues

- **The lister image's base packages.** `ghcr.io/varveio/swath` is built on
  `gcr.io/distroless/static-debian12:nonroot`, pinned by digest in the
  [`Dockerfile`](https://github.com/varveio/swath/blob/v0.5.0-rc.1/Dockerfile), for its CA certificates and
  zone data. Its `tzdata` (2026b-0+deb12u1) and `ca-certificates` (20250419~deb12u1) packages have Debian LTS
  advisories open against them: DLA-4792-1, DLA-4854-1 and DLA-4867-1 (new time zone database) and DLA-4845-1
  (a `ca-certificates` security update). The nightly `osv-scan` job has failed on them since 2026-11-20; the
  release workflow does not run that scan. In the image, swath verifies HTTPS to S3, GCS and Azure against
  those CA certificates, and reads a zone that `TZ` names from that zone data. The archives' binaries, which
  use the host's own, and the `swath-replay` image, which is `scratch` and in which the scan finds nothing,
  are not affected.
- **Experimental packages.** Every library package new in this release (`source` and its providers, `command`,
  `reader/listing`, `reader/view`, `reader/du` and `reader/du/tree`) is experimental: its API may change in any
  release. `view.OpenDir` opens a dataset with SeekParquet's default reader limits, which a dataset with very
  long keys can exceed ([#500](https://github.com/varveio/swath/issues/500)). `view.Open` checks the values
  it reads, not each part's schema: on a dataset a program opened without `Dataset.CheckSchema`, which
  `view.OpenDir` runs, `Summarize` can refuse a defect with another error class than the check's, or sum a
  column whose logical annotation alone is wrong
  ([The inventory view](https://github.com/varveio/swath/blob/v0.5.0-rc.1/reader/README.md#the-inventory-view)).
  `go install github.com/varveio/swath/cmd/swath@v0.5.0-rc.1`, and an outside program that imports the library
  by this version, have not been tried yet ([#567](https://github.com/varveio/swath/issues/567)); the release
  builds the command as an outside project does, from its own checkout.
- **Not listed:** Azure hierarchical namespaces (Data Lake Storage Gen2), S3 directory buckets, access points
  and Outposts, and GCS soft-deleted objects.
- **Resume:** only object-store runs that write a dataset resume. A filesystem listing, text output and
  `swath du` are run again.
- **S3-compatible stores:** versions listings (`--all-versions`) are not checked for a final page that ends
  early ([#265](https://github.com/varveio/swath/issues/265)). Before relying on a new store, compare one
  prefix's object count with an independent listing
  ([S3-compatible stores](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/sources/s3.md#s3-compatible-stores)).
- **Windows:** dataset runs and `swath resume` work as on Linux and macOS, on an output whose filesystem
  syncs directories (checked when the run starts). Power loss is safe on local NTFS; on ReFS or an SMB share
  a crash or kill is recovered, but a power loss may lose the latest part names. An `--overwrite` blocked by
  a file another program holds open fails naming that cause and is rolled back: the previous dataset stays
  readable and `swath resume DIR` installs the replacement later. If the rollback is blocked too, the
  directory holds no dataset until `swath resume DIR` finishes the replacement
  ([The output directory is refused](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/troubleshooting.md#the-output-directory-is-refused)).
  On a local tree the stat tier's space on disk can lag under a hard link's other names and for a file still
  being written; `uid` and `gid` are null at every tier; and `--one-file-system` compares no device or volume,
  so a DFS link or a share the server maps to another volume is walked.
- **macOS `swath du /`** counts the Data volume twice: the firmlinked directories such as `/Users` are the same
  directories as those under `/System/Volumes/Data`, and the walk enters them along both paths
  ([#676](https://github.com/varveio/swath/issues/676)). With `--one-file-system` a scan of `/` stops at the
  Data volume; list `/System/Volumes/Data` as its own root.
- **macOS Gatekeeper.** The binaries are not signed with an Apple Developer ID or notarized
  ([#420](https://github.com/varveio/swath/issues/420)). A browser download is refused until you clear its
  quarantine attribute, after verifying and extracting the archive (`darwin-amd64` on Intel):

  ```bash
  xattr -d com.apple.quarantine swath-0.5.0-rc.1-darwin-arm64/swath swath-0.5.0-rc.1-darwin-arm64/swath-replay
  ```

- **The rehearsal.** A candidate publishes no rolling image tags, so the step that moves `0.5` and `latest` to
  the verified digests, new since 0.4.0, first runs at 0.5.0.
- Archives are built for the five targets above only; images are linux only.
- swath publishes the complete result of the live listing it performed. It is not a point-in-time snapshot
  of a bucket that changes during the run.
- The rest, and what has not been measured, is in
  [Known limits](https://github.com/varveio/swath/blob/v0.5.0-rc.1/docs/troubleshooting.md#known-limits).
