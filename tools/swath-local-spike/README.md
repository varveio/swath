# swath-local (feasibility spike)

`swath-local` lists a local directory tree in parallel and writes a globally key-sorted Parquet dataset in
Swath's sorted-dataset layout (`data/part-NNNNN.parquet`, `manifest.json`, `_SUCCESS`, ...). It is a pure-Go
(`CGO_ENABLED=0`) prototype, not a product. Measurements and findings: [REPORT.md](REPORT.md).

```bash
CGO_ENABLED=0 go build -ldflags='-s -w' -o swath-local ./cmd/swath-local
./swath-local list /some/root --out /tmp/ds            # walk + sort + Parquet
./swath-local list /some/root --count-only             # walk only (benchmarks)
python3 verify.py /tmp/ds                              # independent DuckDB check
go vet ./... && go test -race ./...
```

Flags (`swath-local list --help`): `--out`, `--overwrite`, `--workers N` (default `min(64, max(4, 3*GOMAXPROCS))`),
`--engine getdents|fstatat|parallel-lstat|portable|walkdir` (default `getdents` on Linux, `portable` elsewhere), `--fields names|stat|statx`, `--statx-mask basic|all`,
`--no-dirs`, `--no-link-target`, `--ino-sort`, `--split-entries N` (default 4096), `--sort-buffer-rows N`
(default 2,000,000), `--sort-buffer-bytes B`, `--part-size BYTES` (default 64 MiB), `--row-group-bytes B`
(default 8 MiB), `--codec go|go-entropy`, `--encoders N` (default GOMAXPROCS), `--count-only`, `--json-stats`,
`--encode-scheme column|part`, `--order none|dfs-sequential|dfs-parallel`, `--reorder-rows N`, `--gc-percent N`, `--cpuprofile FILE`. The one-line summary prints files,
dirs, bytes, errors, rows, parts, walk time, sort+write time, total, user/sys CPU and peak RSS (`VmHWM`).

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
`statx` = `statx(dirfd, name, AT_SYMLINK_NOFOLLOW|AT_NO_AUTOMOUNT, mask)` instead, adding `birth_time` with
`--statx-mask all` (`STATX_BASIC_STATS|STATX_BTIME`); `basic` = `STATX_BASIC_STATS`.

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

Matches Swath's sorted managed dataset (`docs/usage.md` "Directory dataset layout", contracts §4.1):

```text
out/
  data/part-00000.parquet ...   dense, zero-based %05d; lexical name order == key order; parts strictly disjoint
  manifest.json                 consumer manifest (below)
  .swath-state.json             {"args_hash": sha256 of argv, "run_id": null}
  symlink.txt                   data/<part> paths
  _swath_local_summary.json     swath-local's own run report (counts, timings, RSS); deliberately NOT named
                                _swath_summary.json, Swath's reserved name whose schema Varve's reader checks
  _SUCCESS                      empty, written last
```

Parts are written as `data/part-NNNNN.parquet.tmp` (fsync on close), renamed in name order after all parts
exist, then the data directory is fsynced; each JSON artifact is written `.tmp`, fsync, rename, directory fsync.
Spill runs live under `_staging/`, deleted before publication. A failed run removes `_staging/` and any
`data/*.parquet.tmp` and never writes `_SUCCESS`.

`--out` safety:
- The root is validated before `--out` is touched.
- `--out` may not be the root or an ancestor of it. This is checked by cleaned absolute path and by directory
  identity (dev/inode), along both the given and the symlink-resolved root path.
- A non-empty `--out` is refused unless `--overwrite`.
- `--overwrite` is refused unless `--out` already holds a swath-local dataset (`manifest.json` with our
  `schemaId`, or `_SUCCESS` plus `data/`). It is also refused when `--out` is inside the root.
- `--overwrite` removes only the dataset's own entries (`data/`, `_staging/`, `manifest.json`, `_SUCCESS`, the
  summary, `.swath-state.json`, `symlink.txt`) and never other content.
- An `--out` inside the root without `--overwrite` is allowed: the walker skips that directory, found by
  identity, in every engine and order mode, so the dataset never lists itself or its own scratch files.
There is no checkpoint or resume (Swath's `.swath/checkpoint.sqlite` has no equivalent here).

`manifest.json` mirrors `io.varve.swath.output.parquet.Manifest` field names and order: `sourceBucket`,
`version` (`"1"`), `creationTimestamp` (ms), `fileFormat` (`"Parquet"`), `fileSchema` (the schema rendered as a
parquet-mr `MessageType.toString()` string), `sorted` (`true`), `sortKey`, and `files[]` of `{key, size,
MD5checksum, rowCount, minKey, maxKey}` (MD5 over the exact emitted bytes; min/max are each file's true
first/last key, not footer statistics). Local-filesystem values: `sourceBucket` is `""` (Java's documented
value for "no bucket"); additive fields after `files[]`: `schemaId`, `sourceKind` (`"local-fs"`), `sourceRoot`
(absolute root path), `totalRows`.

Footer key/value metadata on every final part, as Swath's `SortedParquetWriter`: `swath.sort.order` =
`key_bytes_unsigned,version_id_null_first,row_type_rank` (Swath's order string kept verbatim; local keys are
unique, so it reduces to unsigned key-byte order), `swath.sort.mode=objects`, `swath.sort.format_version=1`,
`swath.sort.file_index` (1-based roll position, per the Java constant's doc), and `swath.sort.file_final=true`
**only** on the last part (stamped at close; absent elsewhere), as in Java.

## Writer

Reused from the October Go Parquet spike (`tools/go-parquet-spike`): parquet-go v0.32.0, the public column-values
write path (no reflective fallback), a schema adapter (`localRoot`) that strips parquet-go's `INT(n,true)`
annotation from integer columns (Swath's `size` is bare INT64) and wraps the key column's indexer so a one-page
chunk reports `BoundaryOrder=ASCENDING`, delta-encoded keys, `DELTA_BINARY_PACKED` integers, dictionary for other
strings, data page v2, page statistics off, served geometry (1024-row pages, 1024-byte column-index bounds,
8 KiB dictionary limit), byte-targeted row groups (8 MiB, checked at page boundaries, 1M-row ceiling), and the
pure-Go `go-entropy` ZSTD codec (klauspost, SpeedDefault, all-literal entropy on). No native codec, no checkpoint.

## Sort

Workers hand row batches (4096 rows) to the sorter, which takes ownership of each batch (no copy; a short
mutex section appends the slice header). When the buffered rows reach `--sort-buffer-rows` (or
`--sort-buffer-bytes`, estimated at 120 B + key + link target per row) the batch list goes to one spill goroutine
(backpressure: at most one buffer queued), which sorts it and writes a temporary Parquet run with the same writer.
At the end the in-memory tail is sorted and k-way merged (heap) with the runs into final parts; with no spills
nothing is re-read. In-memory sorting is over a `[]*Row` pointer permutation: GOMAXPROCS chunks sorted
concurrently, then merged pairwise in parallel. Parts roll at a page boundary once the writer's size estimate
reaches `--part-size`. Order is Go string comparison = unsigned bytes. Parts are written one at a time; within a
page, `--encoders` goroutines fill, encode and compress different columns concurrently (parquet-go column writers
are independent; the ZSTD encoder's `EncodeAll` is concurrency-safe). The bytes are identical to serial encoding
(tested).

## Sorted-order walk (`--order`)

`--order none` (default) walks unordered in parallel, then runs the external sort above. `--order dfs-sequential`
and `--order dfs-parallel` emit keys already in global order and stream them straight into the Parquet writer
(`sortout.Streamer`): no sort, no spill runs, no `_staging/`. Within a directory, entries are ordered by key
bytes, where a subdirectory's key is `prefix/name/` (its DIR row key); the DIR row is emitted, then its subtree,
before the next sibling. `dfs-parallel` reads directories with `--workers` goroutines (children pushed in reverse
sorted order on the LIFO stack) and reassembles depth-first order with a reorder buffer bounded by `--reorder-rows`
(default 2,000,000; 100,000 measured better, see REPORT.md). Workers wait at the bound, and the draining
goroutine reads any directory it needs that no worker has claimed, so the bound cannot deadlock. The writer's
strictly-ascending key check stays on. Large-directory stat splitting is off in these modes. The summary reports
`reorder_max_buffered_rows`.

## Walker

Unit of work = one directory. `--workers` goroutines share a mutex-protected LIFO stack (depth-first) with a
pending counter (queued + in progress) so termination is exact. Engines:

| Engine | Directory read | Per entry | Platform |
|---|---|---|---|
| `getdents` (Linux default) | `open(O_RDONLY\|O_DIRECTORY\|O_CLOEXEC\|O_NOFOLLOW)`, raw `getdents64` into a reused 64 KiB buffer, parsed in place (approach of charlievieth/fastwalk, re-implemented) | `fstatat(dirfd, name, AT_SYMLINK_NOFOLLOW)` | Linux |
| `fstatat` | same open via `os.OpenFile`, `os.File.ReadDir(-1)` (d_type, no stat) | same | Linux |
| `parallel-lstat` | `os.Open` + `ReadDir(-1)` | full-path `os.Lstat` | all |
| `portable` (non-Linux default) | `os.ReadDir` (sorts names) | `DirEntry.Info()` (= full-path lstat on Unix) | all |
| `walkdir` | `filepath.WalkDir`, single goroutine | `os.Lstat` | all |

`--ino-sort` (getdents only) stats each directory's entries in `d_ino` order (a cold-cache optimisation on ext4).
Directories with more than `--split-entries` entries (fd engines, stat tiers; the value must be 0 = off or >= 2)
have their stat phase split into chunks of half that size that other workers take; the directory fd is shared
and reference-counted. Readlink runs only for symlinks. Keys on Windows are `/`-separated.

Symlink swaps: the Linux fd engines open child directories with `O_NOFOLLOW|O_DIRECTORY`. The path-based engines
(`parallel-lstat`; `portable`, the non-Linux default) open by path. They then re-check that the path is still a
real directory (`Lstat`: not a symlink) and is the same object as the opened fd (`fstat`). A mismatch is skipped
and counted as `unsafe_dirs`. This is **best-effort**: a swap between the two checks is still possible. `walkdir`
(a baseline) has no such check.

Portability: `walk_linux.go` holds the fd-relative fast paths; `walk_portable.go` (`!linux`) makes `portable` the
default. `getdents` became the Linux default after measurement (REPORT.md); `fstatat` is the os.File variant. `sysinfo_{linux,windows,other}.go` fill what `os.FileInfo.Sys()` gives (Linux: every stat column;
Windows: `CreationTime` as `birth_time`, `LastAccessTime` as `atime`, no ctime/uid/gid/inode/dev/nlink/blocks;
macOS/BSD: size, mtime, mode, kind only). The module builds with `CGO_ENABLED=0` for linux/amd64, linux/arm64,
darwin/arm64 and windows/amd64. Peak RSS and CPU in the summary are Linux-only (0 elsewhere).

## Verification and tests

`verify.py` (DuckDB) checks `_SUCCESS`, that `data/` holds exactly the manifest parts, manifest `totalRows` ==
DuckDB `count(*)`, per-part row counts, strict ascending raw key bytes within each part (file row order) and
across parts, manifest `minKey`/`maxKey` == true first/last keys, and the footer stamps; it prints the schema as
DuckDB sees it (`DESCRIBE` and `parquet_schema`) and min/max key per part.

Go tests: the walker against `filepath.WalkDir` for every engine x tier x {1,3} workers x {dirs, --no-dirs} on a
tree with nested dirs, spaces and non-ASCII names, symlinks (file, dir, dangling), a FIFO, a non-UTF-8 name, an
unreadable directory and a 5000-entry directory (forced split); the external sort with multiple spills from
concurrent emitters merged into multi-part output equal to the sorted input row-for-row; unsigned order for
bytes > 0x7f; a single-run no-merge case; and a parquet-go round trip checking order, count, raw footer column
names/physical/logical types and stamps; serial vs column-parallel encoding byte-identical; the ordered walks
(both DFS modes, four engines, two tiers, reorder bounds 1/3/2M) emitting exactly the sorted key sequence of the
unordered walk on a tree with `a-b`, `a.b`, `a`, `a0`, `ab` and non-ASCII names as both files and directories; and
the streaming writer rejecting a descending key. The unreadable-directory error assertion only fires when not run as
root (root bypasses permissions); it was run as uid 65534 via `setpriv` (see REPORT.md).

Benchmarks: `bench/gen_tree.py` (synthetic trees), `bench/run.sh warm|cold`, `bench/summarize.py`,
`bench/order_opt.sh` (ordered-walk before/after), `bench/encode_bench.sh` (per-part vs per-column encoding),
`bench/compare.sh` (other listers: gdu, dust, diskus, pdu, dua, fd, find).
