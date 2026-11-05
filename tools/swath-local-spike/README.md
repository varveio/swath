# swath-local (feasibility spike)

`swath-local` lists a local directory tree in parallel and writes a globally key-sorted Parquet dataset in
Swath's sorted-dataset layout (`data/part-NNNNN.parquet`, `manifest.json`, `_SUCCESS`). It is a pure-Go
(`CGO_ENABLED=0`) prototype whose core is meant to be reused for other listing providers. Design:
[DESIGN.md](DESIGN.md). Measurements and findings: [REPORT.md](REPORT.md).

```bash
CGO_ENABLED=0 go build -ldflags='-s -w' -o swath-local ./cmd/swath-local
./swath-local list /some/root -out /tmp/ds        # list, order, encode, publish
./swath-local list /some/root -count-only         # list only
python3 verify.py /tmp/ds                         # independent DuckDB check
go vet ./... && go test -race ./...
```

## S3

`swath-local list s3://bucket[/prefix] -out DIR` lists a bucket with ListObjectsV2 and writes the same dataset
layout with Swath's canonical 13-column schema (contracts §4), so a Swath dataset reader (Varve) opens it.
Credentials come from the default AWS chain; with none, requests are anonymous. `-endpoint` and
`-path-style` point it at S3-compatible stores. The keyspace is listed as `(lo, hi]` ranges in parallel: a
range splits itself at a page boundary when workers are idle (DESIGN.md, "S3"); `-seed` first tiles it
from one delimiter page; `-sequential` lists with one continuation-token chain instead (the reference, and
the only mode where `-max-keys` stops early for experiments, marking the dataset incomplete). The JSON
summary reports pages, requests, retries, splits by outcome, empty split children and peak concurrency. The manifest records
`sourceBucket`, and `_swath_summary.json` records the listed target (`s3://bucket` or `s3://bucket/prefix`)
so readers can tell a whole-bucket dataset from a prefix one. `bench/s3fake` serves a gofakes3 bucket loaded
from a key list, for tests and benchmarks without credentials.

## Flags

Generated from the flag definitions (`TestReadmeFlags` fails when this block is stale; `UPDATE=1 go test
./cmd/swath-local` rewrites it):

<!-- flags:begin -->
```text
usage: swath-local list <root> [flags]
  <root> is a local directory or s3://bucket[/prefix]

  -count-only
    	walk and count without writing a dataset
  -encoders int
    	Parquet encoder goroutines (default GOMAXPROCS)
  -endpoint URL
    	S3: endpoint URL of an S3-compatible store
  -fetch-owner
    	S3: fill owner_id and owner_display_name
  -fields string
    	metadata per entry: names (no stat), stat, statx (adds birth_time; Linux) (default "stat")
  -json
    	print the summary as JSON
  -max-keys int
    	S3: with -sequential, stop after this many objects (experiments; the dataset is marked incomplete)
  -memory bytes
    	bytes of rows held between listing and encoding; sets the reorder bound (default 268435456)
  -no-dirs
    	omit kind=DIR rows
  -no-link-target
    	do not read symlink targets
  -out directory
    	output dataset directory (required unless -count-only)
  -overwrite
    	replace a previous swath-local dataset in -out
  -part-size bytes
    	target bytes per Parquet part (suffixes KiB, MiB, GiB, KB, MB, GB) (default 67108864)
  -path-style
    	S3: path-style addressing
  -region string
    	S3: region (default us-east-1)
  -seed
    	S3: tile the keyspace from one delimiter page before splitting
  -sequential
    	S3: list with one continuation-token chain (no ranges; the reference)
  -workers int
    	listing goroutines (default min(64, max(4, 3*GOMAXPROCS)))

Profiling: SWATH_LOCAL_CPUPROFILE=FILE and SWATH_LOCAL_MEMPROFILE=FILE.
```
<!-- flags:end -->

The one-line summary prints files, dirs, bytes, errors, rows, parts, output bytes, wall and CPU time, peak RSS
(`VmHWM`) and the peak reorder buffer; `-json` prints the full summary, including bounded error samples.

## Schema `swath-local/v0`

One row per entry below the root (the root itself has no row). `key`, `size` and `last_modified` have exactly
the names and types of Swath's canonical schema (contracts §4), so a reader that needs only those works on both.
Everything else replaces Swath's S3-specific columns.

| Column | Parquet physical / logical | Null? | Source |
|---|---|---|---|
| `key` | BINARY (STRING) | required | path relative to the root, `/`-separated, no leading `/`; directories end in `/` |
| `size` | INT64, unannotated | optional | `st_size` (apparent bytes) |
| `last_modified` | INT64 TIMESTAMP(MICROS, UTC) | optional | `st_mtime` |
| `kind` | BINARY (STRING) | required | `FILE`, `DIR`, `SYMLINK`, `FIFO`, `SOCKET`, `CHAR_DEVICE`, `BLOCK_DEVICE`, `UNKNOWN` |
| `mode` | INT32, unannotated | optional | `st_mode & 0o7777` (permission + setuid/setgid/sticky) |
| `uid`, `gid` | INT32, unannotated | optional | `st_uid`, `st_gid` (bit pattern: ids >= 2^31 read back negative) |
| `nlink` | INT32, unannotated | optional | `st_nlink` |
| `inode` | INT64, unannotated | optional | `st_ino` (bit pattern) |
| `dev` | INT64, unannotated | optional | `st_dev` (statx: `makedev(major, minor)`) |
| `blocks` | INT64, unannotated | optional | `st_blocks * 512` = allocated bytes (sparse files show `blocks < size`) |
| `ctime`, `atime` | INT64 TIMESTAMP(MICROS, UTC) | optional | `st_ctime`, `st_atime` |
| `birth_time` | INT64 TIMESTAMP(MICROS, UTC) | optional | statx `stx_btime` (only `--fields statx`, mask `all`, when the filesystem reports it); Windows `CreationTime` |
| `link_target` | BINARY (STRING) | optional | `readlink` result, symlinks only (stat/statx tiers unless `--no-link-target`) |

The Parquet message name stays `swath_listing`. `manifest.json` carries `"schemaId": "swath-local/v0"`.

Cost tiers (`--fields`): `names` = getdents only, key + kind from `d_type`, all other columns null (one
`fstatat` per entry only where the filesystem returns `DT_UNKNOWN`); `stat` (default) = one
`fstatat(dirfd, name, AT_SYMLINK_NOFOLLOW)` per entry fills every stat column, plus one `readlinkat` per symlink;
`statx` = `statx(dirfd, name, AT_SYMLINK_NOFOLLOW|AT_NO_AUTOMOUNT, STATX_ALL)` instead, adding `birth_time`.

Mapping rules:
- Directories produce `kind=DIR` rows with a trailing-slash key (`a/b/`), the Varve convention for an exact
  prefix object; `--no-dirs` omits them. Symlinks are never followed (a symlink to a directory is a `SYMLINK` row).
- No mount-point handling: the walk crosses into other filesystems. `--one-file-system` is future work.
- A name that is not valid UTF-8 is counted (`invalid_utf8_names`) and skipped; for a directory its whole
  subtree is skipped. Known gap: Swath's contract requires well-formed UTF-8 keys, so such entries cannot be
  represented without an escaping rule.
- No 1024-byte key limit is enforced (local paths can reach 4096 bytes; S3 keys cannot exceed 1024).
- Errors (EACCES on open, readdir failures, vanished entries = ENOENT between getdents and stat, readlink
  failures) are counted per category and reported; they are never fatal. Only an unusable root fails the run.

## Output layout

Swath's sorted managed dataset layout (`docs/usage.md` "Directory dataset layout", contracts §4.1):

```text
out/
  data/part-00000.parquet ...   dense, zero-based %05d; name order == key order; parts strictly disjoint
  manifest.json                 Swath consumer manifest fields + schemaId, sourceKind, sourceRoot, totalRows
  symlink.txt                   data/<part> paths
  _swath_local_summary.json     this tool's run report (not Swath's reserved _swath_summary.json)
  _SUCCESS                      empty, written last
```

Parts are written as `data/part-NNNNN.parquet.tmp` (fsync on close), checked by the publication gate (dense
ordinals, strictly ascending disjoint key ranges, rows written == rows listed), renamed in ordinal order, and the
data directory is fsynced; every JSON file is written `.tmp`, fsynced, renamed, then its directory fsynced.
Failures and cancellation remove the `.tmp` parts and never write `_SUCCESS`. `.swath-state.json` (Swath's
resume identity) is not written: there is no resume.

Footer stamps on every part, as Swath's `SortedParquetWriter`: `swath.sort.order`
(`key_bytes_unsigned,version_id_null_first,row_type_rank`; local keys are unique, so this is unsigned key-byte
order), `swath.sort.mode=objects`, `swath.sort.format_version=1`, `swath.sort.file_index` (1-based) and
`swath.sort.file_final=true` on the last part only.

`-out` safety: the root is validated before `-out` is touched; `-out` may not be the root or an ancestor of it
(by path and by dev/inode, along the given and the symlink-resolved root); a non-empty `-out` is refused unless
`-overwrite`, which requires a previous swath-local dataset outside the root and removes only its own entries.
An `-out` inside the root (without `-overwrite`) is skipped by the walk, found by directory identity.

## How it works

See [DESIGN.md](DESIGN.md). In short: the filesystem provider reads each directory with raw `getdents64`
(Linux) or `os.File.ReadDir` (elsewhere), sorts its entries with the trailing-slash rule and stats them relative
to the directory fd; the order stage drains the directory tree depth-first, which is global key order, with a
bounded reorder buffer; the part router cuts deterministic part boundaries and feeds per-part encoders; the
dataset package publishes. There is no sort step and no spill.

Directories with more than 4096 entries are statted in 2048-entry chunks by several workers. Below the root
symlinks are never followed (`O_NOFOLLOW` on Linux; the portable engine re-checks with `Lstat`/`fstat` that an
opened child directory is the same real directory, best-effort). Names that are not valid UTF-8 are counted and
skipped. Mount points are crossed (no `--one-file-system` yet).

## Verification and tests

`verify.py` (DuckDB) checks `_SUCCESS`, that `data/` holds exactly the manifest parts, row counts, strictly
ascending raw key bytes within and across parts, manifest `minKey`/`maxKey`, footer stamps and the summary file
name, and prints the schema as DuckDB sees it.

Go tests cover:
- the listing against a sorted `filepath.WalkDir` oracle, exact sequence, for both engines, every tier, with and
  without DIR rows, at reorder budgets from 1 to unbounded (tricky names `a-b`, `a.b`, `a`, `a0`, `ab` as files
  and as directories, bytes above 0x7f, symlinks, FIFO, non-UTF-8 names, an unreadable directory, a chunked
  5000-entry directory);
- the order stage on a synthetic streaming provider (sequence, memory bound, sink failure stops the walk
  promptly, provider failure, cancellation);
- byte-identical output for 1 to 15 column encoders and across part encoder counts, footer stamps, the footer
  schema against the message string, and the parquet-go copy contract that zero-copy strings rely on;
- the publication gate, overwrite safety, a failed publish leaving no `_SUCCESS`, a mid-walk cancellation and an
  encoder failure leaving no parts; the dirent parser (unit and fuzz).

Benchmarks: `bench/gen_tree.py` (synthetic trees), `bench/restructure.sh` (before/after), `bench/compare.sh`
(other listers). Older scripts in `bench/` drive flags of the pre-restructure binary; see REPORT.md.
