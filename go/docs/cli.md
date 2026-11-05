# CLI

```text
swath list <root> [flags]
```

`<root>` is a local directory or `s3://bucket[/prefix]`. The command lists it and writes a dataset to `-out`
([internals/parquet-output.md](internals/parquet-output.md)), or with `-count-only` only lists. It prints a
human summary, or the run summary as JSON with `-json`. The Java distribution keeps its own `swath`
launcher; the two are separate tools until the Java code is retired.

## Flags

Generated from the flag set (`TestCLIDocFlags` fails when this block is stale; `UPDATE=1 go test ./cmd/swath`
rewrites it):

<!-- flags:begin -->
```text
usage: swath list <root> [flags]
  <root> is a local directory or s3://bucket[/prefix]

  -anonymous
    	S3: send unsigned requests, ignoring any credentials
  -count-only
    	walk and count without writing a dataset
  -dualstack
    	S3: use the dual-stack (IPv6) endpoint
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS)
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fields tier
    	metadata tier per entry: names (no stat), stat (the default), statx (adds birth_time; Linux)
  -fips
    	S3: use the FIPS endpoint (s3-fips.REGION.amazonaws.com)
  -json
    	print the summary as JSON
  -max-keys int
    	S3: with -sequential, stop after this many objects (experiments; the dataset is marked incomplete)
  -memory bytes
    	bytes of rows held between listing and encoding; sets the reorder bound (default 268435456)
  -no-dirs
    	omit kind=DIR rows
  -no-journal
    	S3: publish parts only at the end, without the journal that -resume needs
  -no-link-target
    	do not read symlink targets
  -no-probe
    	S3: split without the 1-key probe that checks the upper child is not empty
  -no-spill
    	wait on the reorder budget instead of spilling rows ahead of the frontier to disk
  -out directory
    	output dataset directory (required unless -count-only)
  -overwrite
    	replace a previous swath-local dataset in -out
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing
  -profile profile
    	S3: shared config profile (default: the AWS SDK chain: environment, AWS_PROFILE or default, SSO, web identity, IMDS)
  -region string
    	S3: region (default: AWS_REGION, AWS_DEFAULT_REGION, the profile's, us-east-1)
  -request-timeout duration
    	S3: per-request timeout; a request that exceeds it is retried (default 30s)
  -resume
    	S3: continue the interrupted run in -out (same source and output settings)
  -seed
    	S3: tile the keyspace from one delimiter page before splitting
  -sequential
    	S3: list with one continuation-token chain (no ranges; the reference)
  -spill-compress
    	compress spill files (zstd level 1)
  -spill-max bytes
    	cap on spill bytes on disk (0: no cap); past it, listing waits instead of spilling
  -spill-reserve bytes
    	free bytes spill leaves on its filesystem, for the parts still to be written (default 1073741824)
  -workers int
    	listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))

swath inspect <out> prints an S3 run's journal (.swath/journal) as JSON lines.

Profiling: SWATH_LOCAL_CPUPROFILE=FILE and SWATH_LOCAL_MEMPROFILE=FILE.
```
<!-- flags:end -->

## Semantics worth stating

- **Sizes** accept `KiB`, `MiB`, `GiB` and `KB`, `MB`, `GB` suffixes.
- **`-memory`** is one budget: half of it bounds the bytes of rows in flight (reorder storage and encoder
  queues); see
  [internals/ordering.md](internals/ordering.md).
- **`-out`** is refused when non-empty unless `-overwrite`, which replaces only a previous dataset of this
  tool and never anything inside the listed root.
- **S3 modes.** The default is the parallel core; `-seed` tiles the scope from one delimiter page first;
  `-sequential` is the one-chain oracle and the only mode where `-max-keys` stops early (the dataset is then
  marked incomplete). `-endpoint` and `-path-style` select S3-compatible stores.
- **S3 credentials:** `-anonymous` sends unsigned requests whatever the environment holds. Otherwise the AWS
  SDK's default credential chain applies: environment, shared files and profiles (`-profile`), SSO, web
  identity, credential processes, AssumeRole, container and instance roles. With nothing found, requests go
  unsigned. Secrets are never taken from flags (they would show in `ps`). Details:
  [internals/providers.md](internals/providers.md).
- **Spill:** when the reorder budget is full, rows of ranges ahead of the frontier go to disposable files
  under `-out/.swath/spill/` instead of stalling the workers; `-no-spill` turns it off, `-spill-compress`
  compresses them. Spill stops (and listing waits instead) at `-spill-max` bytes on disk or when the
  filesystem would drop below `-spill-reserve` free bytes (default 1 GiB)
  ([internals/ordering.md](internals/ordering.md)).
- **Resume:** an S3 run with `-out` keeps a journal in `-out/.swath/`; after a crash or interruption,
  rerun the same command with `-resume` ([internals/durability.md](internals/durability.md)).
  `-no-journal` turns it off (parts are then published only at the end). `swath inspect DIR` prints the
  journal.
- **Exit status** is non-zero on any error; a failed or interrupted run (SIGINT, SIGTERM) leaves no `manifest.json`.
  An interrupted run gets 15 s to stop cleanly, and is then exited with status 130; an S3 run can be resumed.
- **Profiling:** `SWATH_LOCAL_CPUPROFILE` and `SWATH_LOCAL_MEMPROFILE` name pprof output files.
