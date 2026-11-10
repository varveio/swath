# Filesystem (`internal/source/localfs`)

A local or mounted directory supports dataset output and direct `du` summaries. A local
`list` requires `-o DIR`; local scans do not resume. Mounted shares use the operating system's
filesystem API. [Object-store request controls](../../running.md#object-store-requests) do not
apply to this provider; metadata and platform limits are described below.

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir`, re-checking that an opened child directory is the one
  that was listed. Its stat tier differs by platform: on macOS one `fstatat` per entry relative to the open
  directory (every stat column and `birth_time`; `os.DirEntry.Info` is an `lstat` of the full path, which
  resolves the path again per entry), on Windows the directory enumeration's own size, times and attributes
  (no call per entry; no `uid`, `gid`, `nlink`, `inode`, `dev` or `allocated_bytes`), elsewhere `Lstat`.
- **Large directories** (getdents engine). An entry holds its name and key as offsets into the directory's
  buffers, no pointer, so growing, sorting and chunking a huge directory costs no GC write barriers. After the
  first 1,024 entries the buffers are grown once to the count the directory's `st_size` suggests (report:
  `presized`). The sort compares keys after the directory's shared prefix; a directory of 32,768 entries or
  more is sorted in runs of at least 16,384, concurrently (up to GOMAXPROCS), then merged pairwise
  (`parallel_sorts`); names in a directory are distinct, so the order is the serial sort's. Chunks take the
  directory's buffers rather than copies. Offsets are 32-bit: a directory whose names, or keys, pass 4 GiB is
  skipped and counted in `read_errors`.
- **Workers: 4 per CPU, at most 64.** Without `--workers` a local tree gets `min(64, 4 × GOMAXPROCS)`
  (GOMAXPROCS follows the CPU affinity and the cgroup quota), not the object stores' 64: a stat waits on the
  disk or the inode cache, not on a round trip. On a 1-CPU, 512 MiB cgroup (ext4 on a persistent disk, cold;
  issue #388; 2 rounds) 4 workers were fastest or tied on a nested tree of ~1M entries (28.3 s, 28.6 s at 64,
  44.6 s at 1) and on 32 directories of 60,000 files (19.9 s and 90 MiB, against 49.1 s and 261 MiB at 64). On
  one directory of 1M files 1 worker was fastest (119 s, 187 s at 4, 176 s at 64), every count there limited by
  the memory a whole directory holds. On a 4-vCPU machine (GOMAXPROCS 16), 1M files in one directory on XFS,
  ext4, btrfs and ZFS plateau from 4 to 16 workers, warm and cold. A count's processor boost still applies at
  the default (`count_procs`). Storage that waits per request (network filesystems) may want more `--workers`;
  nilfs2 wants fewer (#365).
- **A count runs with more processors.** A stat or a directory read that waits on the disk holds its Go
  processor until the scheduler hands it off, so one processor per CPU leaves CPUs idle on a cold tree. A count
  (`swath du`, `swath diag count`) raises GOMAXPROCS for the run, at most `--workers`, and restores it
  (`count_procs` in the report): to 4 per CPU at the `stat` and `statx` tiers, to 2 per CPU at the `names`
  tier, where more processors only add contention when the tree is warm. A count has no encoders, reorder
  storage or readahead to resize; runs that write output keep GOMAXPROCS as it is.
- **NFS: attributes with the names.** The Linux NFS client fetches attributes with a directory's names
  (READDIRPLUS) on its first read, and on a later read only if more than 8 of its entries were looked up since
  the previous one (`nfs_use_readdirplus` in `fs/nfs/dir.c`). A directory read whole before any stat would cost
  one LOOKUP per entry past the first page, so at a stat tier, after a read that fills the buffer, the getdents
  engine stats 9 of its entries before the next (`readdirplus_hints`); fdu, which stats each batch before
  reading the next, keeps READDIRPLUS on the same way. A 200,000-file directory over NFS 4.2, cold, Linux 7.0
  client: 199,722 LOOKUPs to none, 5.85 s to 2.42 s ([measurements](../measurements.md)). The heuristic is
  the kernel's and can change; the client's first reads are short (19, then 5 entries) and are not hinted, as
  hinting after them measured worse (3,277 LOOKUPs).
- **A count does not sort.** Nothing consumes a count's order, so both engines skip each directory's sort
  (`unsorted_dirs` in the report) and the count runs with `order.Options.Unordered`
  (`TestUnsortedCountEqualsSortedListing`). getdents engine, warm, 4 vCPU, paired: ~1M entries −10.6 % at the `names` tier and
  −5.5 % at `stat`; a 200,000-entry directory −19.7 % ([measurements](../measurements.md)).
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `birth_time`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks (skipped with it in a count), the portable engine checks every 1,024 entries, and each stat loop checks the context.
- Symlinks are never followed (`O_NOFOLLOW`). Names are bytes: one that is not valid UTF-8 is listed with
  its exact bytes, like any other. Mount points are crossed unless `--one-file-system` (`Options.OneFilesystem`):
  `New` reads the root's device (refused under the `names` tier, or where the engine reads no device: the
  portable engine on Windows), and `emit` keeps a DIR row whose `dev` differs but hands on no child range for it
  (counter `other_device_dirs`); `Report` adds `one_filesystem` to the coverage's filters, so a dataset says it
  stops at the root's filesystem. `--no-dirs` (`Options.NoDirs`) walks every directory but writes no DIR row, and
  `Report` adds `no_dirs` to the filters likewise.
- A link target is at most 4,096 bytes (`link_target`'s `schema.Spec.MaxBytes`, PATH_MAX): the writer's
  admission prices it at that width, and a longer target stops the run with the typed `schema.ErrValueTooLong`, as
  a key over its cap does; a target within it is written byte for byte.
