# Filesystem (`internal/source/localfs`)

A local or mounted directory supports dataset output and direct `du` summaries. A local
`list` prints text rows or writes a dataset; local scans do not resume. Mounted shares use the operating system's
filesystem API. [Object-store request controls](../../sources/README.md#object-store-requests) do not
apply to this provider; metadata and platform limits are described below.

A range is a directory. Its entries are sorted by key, where a directory's key is `prefix/name/` and anything
else `prefix/name`; byte order puts `a-b` < `a.b` < `a/` < `a/…` < `a0`, so a directory's DIR row followed
by its subtree, before the next sibling, is global order. A directory with more than 4096 entries is split into
chunk ranges of 2048 sorted names that share one reference-counted directory fd, so several workers stat it.

Row batches use the [tree core](../tree-sources.md)'s `EmitRows` for child splicing. localfs keeps its
own ranges, descriptor ownership, unread reporting and count slabs. A child retains its directory
reference only after its preceding rows are accepted; the order stage owns that reference once the child
is submitted, even if submission returns an error.

- **Engines.** `getdents` (Linux): raw `getdents64` into a reused buffer, keys in one arena per directory,
  entries sorted before stat so each result lands in its final slot. `portable` (elsewhere, or
  `SWATH_LOCALFS_ENGINE=portable`): `os.File.ReadDir` on the directory opened as the getdents engine opens it
  (descriptor-relative opens, below). On Linux and macOS its stat tier is one `fstatat` per entry relative
  to the open directory (every stat column, and `created_at` on macOS; `os.DirEntry.Info` would be an `lstat`
  of the full path, which resolves the path again per entry), and it reads a link's target with `readlinkat`
  relative to it; `ReadDir` itself types an entry the filesystem leaves untyped (`DT_UNKNOWN`) with an
  `fstatat` relative to the directory too. On Windows it opens a directory by its path, re-checking that it is
  the one that was listed (`Lstat`, `SameFile`; `unsafe_dirs` otherwise), and reads it itself (`readDir`), with the
  call and class `os.File.ReadDir` uses (`GetFileInformationByHandleEx`: `FileIdBothDirectoryInfo` where the
  volume supports object IDs and opening by ID, else `FileFullDirectoryInfo`), so that each entry keeps what
  Go drops: `allocated_bytes` is its `AllocationSize`, `inode` its file ID (null with `FileFullDirectoryInfo`:
  FAT, exFAT, some shares), beside the size, times and attributes (no call per file; no `uid`, `gid`, `nlink` or
  `dev` but at the `links` tier, below). The enumeration gives a directory an allocation of 0 whatever its index takes, so a directory's own
  `allocated_bytes` is read through a handle that asks for attributes alone (`fillDirBlocks`, one per directory);
  a failed read leaves it null, which `swath du` counts as missing (`BlocksMayLack`, as for `statx`). The entries' modes are Go's (`winMode`, checked against
  `ReadDir` by `TestReadDirIsReadDir`). NTFS keeps a copy of the length and allocation in each directory entry and
  refreshes only the one a file was opened through, so they can lag under a hard link's other names and for a
  file being written until it is flushed or closed; a compressed or sparse file's is its compressed or
  allocated size once written back ([measurements](../measurements.md#space-on-disk-in-a-windows-directory-listing-2026-11-25)).
  A file root's (`Lstat` has neither) are read through a handle that asks for attributes alone (`fillRoot`). Go reports a directory reparse point whose tag is a name surrogate other than a
  symbolic link's without the directory bit and as `ModeIrregular`, kind `unknown`; its attributes (reparse
  point and directory) do not say which tag it has. For such an entry only, at every tier, one `os.Readlink`
  decides (`mountPoint`): it reads `IO_REPARSE_TAG_MOUNT_POINT` (a junction or a volume mount point), which
  becomes kind `symlink` with that target in `link_target` (at a stat tier with link targets), and fails for
  any other surrogate tag (`IO_REPARSE_TAG_WCI_LINK`, say) or a failed read, which stays `unknown` and counts
  no error. It opens the entry without following it and asks for no access, so a junction whose list
  permission is denied (`C:\Documents and Settings`) is read too. None of these is walked, as no symbolic link
  is. A directory reparse point of a tag that is not a name surrogate (a cloud placeholder, a DFS link) keeps
  the directory bit and is walked.
- **Large directories** (getdents engine). An entry holds its name and key as offsets into the directory's
  buffers, no pointer, so growing, sorting and chunking a huge directory costs no GC write barriers. After the
  first 1,024 entries the buffers are grown once to the count the directory's `st_size` suggests (report:
  `presized`). The sort compares keys after the directory's shared prefix; a directory of 32,768 entries or
  more is sorted in runs of at least 16,384, concurrently (up to GOMAXPROCS), then merged pairwise
  (`parallel_sorts`); names in a directory are distinct, so the order is the serial sort's. Chunks take the
  directory's buffers rather than copies. Offsets are 32-bit: a listing skips a directory whose names, or
  keys, pass 4 GiB, counts it in `read_errors` and warns (a count reads in slabs and has no such limit).
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
- **A count does not sort, nor hold a directory.** Nothing consumes a count's order, so both engines skip each
  directory's sort (`unsorted_dirs` in the report) and the count runs with `order.Options.Unordered`
  (`TestUnsortedCountEqualsSortedListing`). getdents engine, warm, 4 vCPU, paired: ~1M entries −10.6 % at the
  `names` tier and −5.5 % at `stat`; a 200,000-entry directory −19.7 % ([measurements](../measurements.md)).
  The getdents engine also reads a count's directory in slabs of about 8,192 entries (`count_slabs`): a slab
  that ends the directory within 4,096 entries is statted by its reader; otherwise its entries go to other
  workers as chunks, followed by a continuation range on the same fd that reads the next slab once the
  chunks are taken (`order.Count` pops last-pushed first and pushes a range's children in reverse). A
  directory then holds about one slab beyond the chunks being statted instead of all its entries (about
  140 MiB for 1M files, which made a count thrash the page cache in 512 MiB; issue #388), and keeps the
  chunks' parallel stats, which a single reader statting batch by batch (as `find` does) lost: 1.6× slower
  cold, 2× warm, on 1M files in one directory on a 4-CPU runner. A listing still reads a directory whole: it
  sorts it.
- **Tiers** (`--metadata`): `names` (no stat; `d_type` gives the kind), `stat` (`fstatat`, plus `readlinkat` for
  symlinks), `statx` (full mask, adds `created_at`), `links` (Windows only; `New` and `OpenDir` refuse it elsewhere,
  where `stat` reads links). The `links` tier is the stat tier plus one handle per entry (`fillLinks`; a file root's
  `fillRoot`), opened as the directory's own handles are (`byHandle`: `FILE_READ_ATTRIBUTES`, every sharing mode,
  `FILE_FLAG_OPEN_REPARSE_POINT`, so a symbolic link, a junction or a cloud placeholder is read as itself, never
  followed or recalled) and read with one `GetFileInformationByHandleEx(FileStandardInfo)`: `nlink`, and the
  current length (but for a directory) and `allocated_bytes`, which replace the enumeration's, stale under a hard
  link's other names. Where the enumeration gave a file ID, `GetFileInformationByHandle` reads it from the same
  handle, so that a name replaced by another file since the directory was read is not given that file's link
  count beside the first file's ID, which would make two files one. `dev` is the volume's serial number, which `readDir` reads with the volume's flags, once per
  directory, and every entry there shares, since none is followed; with the file ID as `inode` it identifies a
  hard-linked file (`LinksRead`), so `swath du` counts it once. The serial is 32 bits and not unique across volumes (a
  cloned disk keeps its own): a listing enters no junction or mount point, but two volumes it reaches through
  a directory the redirector serves elsewhere (a DFS link) could share one, and then a file ID on both would be
  taken for one file. A read that fails, or that finds a directory where the
  listing had a file or the reverse, or another file ID, keeps the enumeration's values, leaves `nlink` and `dev` null (a directory's
  `allocated_bytes` too, as `fillDirBlocks`), lists the entry and counts no error; the totals then count it under
  its name and claim no `links_once`. Owner (`uid`) is not read: it needs `READ_CONTROL` access, which a handle
  for attributes does not ask, and another call per entry, and a SID does not fit the column. The report's
  `entry_handles` counts every handle opened for an entry (`files`, `dirs`) and its failures by reason
  (`failed`: `not_found`, `permission`, `changed`, `other`). It is in the report at the `links` tier only
  (`Handles.IsZero`): the stat tier's report is the one it was without the tier, though the stat tier opens the
  directories (`fillDirBlocks`) and a file root (`fillRoot`) and counts them, and opens no file below a directory
  root (`TestLinksTier`).
- **Cancellation:** a huge directory stays cancellable. getdents checks the context every 64 reads, the sort
  is bracketed by checks (skipped with it in a count), the portable engine checks every 1,024 entries, and each stat loop checks the context.
- **Descriptor-relative opens** (Linux, macOS; `dirfd_unix.go`). The root is opened by its path, as the run
  resolved it (a symbolic link root followed). Every directory below it is opened with
  `openat(parent, name, O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC)` relative to its parent's open descriptor, never by
  its path, so a directory replaced by a symbolic link after its parent was listed is not followed and the
  walk does not leave the root through one. A directory moved within the tree after its parent was opened is
  still reached through the parent's descriptor and lists as it is; a link in place of the directory is refused
  (`unsafe_dirs`, unreadable), any other non-directory or nothing there fails as an open (`open_errors`).
  Descriptors are reference-counted (`dirFD`): the directory's own listing, its chunks and the queued range of
  each subdirectory hold one each, so a directory stays open until its last subdirectory is opened relative to
  it, and the last reference closes it. A range releases its reference when its `Expand` returns, however it
  ends, or when the order stage disposes it unexpanded (a cancelled or failed run, `source.Disposer`).
  **Limit:** the walk keeps at most a quarter of the soft descriptor limit, at most 1,024 (`maxKept`), of
  directory descriptors (`keptFDs`), the root always. Beyond them a worker holds at most two while it opens a
  directory and one while it reads it. A directory opened past the limit is read through its descriptor, whole
  by the worker that opened it (getdents: no chunks, so a large one is statted by that worker alone, and a
  count reads its slabs one after another), so its descriptor closes when that worker is done and open
  descriptors stay within the limit plus two per worker. It is not kept: its subdirectories are opened from
  the nearest kept ancestor, one `openat` per name below it, intermediates `O_PATH` on Linux (no read
  permission needed; read-only on macOS) each closed as the next opens, checking for a stop between names; a name replaced by a link on that way is
  refused as above, its error sample naming the level. The cost is one `openat` per directory, as a path
  open was, plus, past the limit, one per name between the ancestor and the directory, which on a deep
  narrow tree grows with the square of its depth. With `--one-file-system` each directory opened below the
  root, and each intermediate on the way from an ancestor, is `fstat`ed against the root's device; the first
  on another device is not walked and that is no error, as at a mount point (`other_device_dirs`).
  On macOS, `fstatfs` also compares the containing volume (`f_fsid`) with the opened root's, before reading
  entries; equal stat devices with different volume IDs stop as `other_volume_dirs`.
  `filesystem_boundary` names the mechanism (`device`, `device_and_fsid`, or `reparse_point` on Windows). A failed volume lookup
  prevents descent, increments `open_errors` and its subset `filesystem_identity_errors`, and reduces
  coverage; it is never recorded as a boundary. The
  report's `dir_opens` counts `opened`, `redescents` (opens from an ancestor), `refused` (directories not
  kept), `failed` by reason (`not_found`, `permission`, `not_directory`, `symlink`, `name_too_long`,
  `other_device`, `other_volume`, `filesystem_identity`, `other`), `peak_kept` and `max_kept`.
- **A held root** (`OpenDir`, Linux and macOS; no command option uses it). A caller that opened the root
  itself (`O_DIRECTORY`, not following a link) hands the open file to the provider, which never resolves the
  root by a path: it lists the directory at a prefix below it (`""`, or a directory key `a/b/`, keys staying
  relative to the root), opening that directory and every one below it relative to the held descriptor, one
  name at a time with `O_NOFOLLOW` (the root itself as `.`, so the walk has its own read offset), each checked
  against the root's device and, on macOS, its volume, the prefix's ancestors included (`OneFilesystem` is set; a stat tier is
  required). The prefix's own entry is not listed (`Lookup` answers it). The caller keeps the file open until
  every range is expanded or disposed. `Lookup` resolves one key the same way: `ENOENT` or `ENOTDIR` at the
  key, or at a directory on the way whose name is no symbolic link (`fstatat` tells), is absent; a symbolic link at the key is present as itself; a symbolic link or
  another device on the way, `EACCES`, `EPERM`, `ENAMETOOLONG` and other errors are unknown, with the
  contract's class as the reason (`unreadReason`). The walk reports each place it could not read as a worker
  meets it (`Provider.unread`), with its key and a reason of the contract's classes (`reader/listing`), to the
  emitter of the range that met it (`source.Unreader`), so a count whose sink is an `order.UnreadSink`, a
  library walk's, is told them: a directory not opened or not entered (`ErrAuth` for a refusal,
  `ErrNotDirectory` for a symbolic link in its place, `ErrOtherDevice`, `ErrUnsupported` for a name or key too
  long or a directory too large to sort, `ErrUnavailable` for the tree changing under it or I/O), a directory
  whose enumeration did not end (a read error), an entry whose stat failed. A walk that stops reports nothing
  more (`TestUnreadReachesTheSink`). The order stage's `order.SubtreeSink` tells when a directory's subtree
  ended; every report under it was made before.
- **The library's live source** (`OpenLive`, `live_unix.go`; the public package `local`, Linux and macOS,
  `listing.ErrUnsupported` elsewhere). A `live.Store` over a held root, for `source.Source`'s `Page`,
  `Lookup` and `Walk` ([library](../../library.md#a-local-directory)). Its location, which page tokens are bound to, is
  the root's device and inode, never a path. A page of a prefix opens its directory as `Lookup` reaches a key's
  parent (`resolve`, shared: each ancestor with `longpath.Search` and `O_NOFOLLOW`, device-checked, at most two
  descriptors), then the directory itself `O_RDONLY|O_NOFOLLOW`, the root as `.` so that each page has its own
  read offset and concurrent pages share nothing, and checks its device too: another device is
  `listing.ErrOtherDevice`, a symbolic link at the prefix or on the way `ErrNotDirectory` (not followed: not
  reached, never absent), nothing there `ErrNotFound`; `EACCES`/`EPERM` are `ErrAuth`, `ENAMETOOLONG`
  `ErrUnsupported`, other errors `ErrUnavailable`, each message naming the level and the caller's prefix, never
  a name read. A lookup that does not reach its key answers unknown with the same class as its `Reason`
  (`unreadReason`). A walk opens its prefix as a page does, so it fails with the same classes, then lists it
  through a held root of its own (`OpenDir`) with the parallel walker as a count does, its directories
  unsorted and its batches in no key order (`live.Count`, unordered); it does not `Begin`, so it changes no
  `GOMAXPROCS`. A page reads the directory whole (Linux: `getdents64`, an untyped entry typed by `fstatat` relative
  to it and, when that stat fails, no directory; macOS: `os.File.ReadDir`, whose own `fstatat` of an untyped entry
  fails the read when it fails, which APFS and HFS+, typing every entry, never ask), and keeps only the names,
  with `/` on directories, that may still be on the page: of those that sort after the token's key, at most twice
  the page's `Limit`, cut back to the first `Limit`+1 when it passes that, a name sorting after the last of them
  passed over from then on (`levelNames`), so that a page's memory follows its `Limit` and not the directory's
  size (`TestLivePageBounded`: a page of 100 of 200,000 files allocates fewer than 10,000 times, where holding and
  sorting the directory whole allocated once per name). A name the read returned twice is kept once. It stats only
  the page's non-directory entries relative to the directory, checking for a stop of the context before the first:
  one gone since is left out, one that is a directory now (its key sorts elsewhere) too, one whose stat fails is
  `unknown`. A child directory is a common prefix, not statted. The directory's own entry, from `fstat` of its
  descriptor, is the first page's `Self`.
  The store's token is the last key the page read, so the next page is the names after it: nothing is held
  between pages, and a directory changing between them repeats and skips nothing that stayed
  (`source/local/local_test.go`: the root's path replaced by a link, a link at and on the way to a prefix, a
  directory replaced between pages, entries removed and added between pages,
  descriptors after every call, concurrent pages; `TestLiveOtherDevice` through the `lookupFstat` seam;
  `TestLiveUntyped` and `TestLiveCanceledAfterRead` through the `levelGetdents` seam).
- **Paths past PATH_MAX.** A directory deeper than the kernel takes in one path (4,096 bytes on Linux, 1,024 on
  macOS) is opened as any other, one name below its parent, so a tree of any depth lists; its children are
  statted and their links read relative to it. A single name past the filesystem's limit fails as an open
  (`name_too_long`). The report's `long_paths` counts the directories whose path passes the limit: `attempts`,
  `opened`, `failed` by reason (as `dir_opens`), `max_depth` (names below the root) and `max_path_bytes`. A root
  past the limit, and a single-file root there, are resolved name by name (`internal/longpath`: `New`, output
  exclusion and the output's checks against the root; a file root statted and its link read relative to its
  directory); such a root's filesystem space is not measured (`statfs` takes the path). Windows has no
  fallback: Go gives a long path its extended-length form there.
- **Key cap: 32 KiB** (`maxKeyBytes`). A tree past PATH_MAX would otherwise have keys of any length; `emit`
  leaves out an entry whose key passes 32,768 bytes and counts it (`key_too_long`, unreadable, named in the
  error samples), a directory with its subtree. The schema states no key cap, so the writer's admission
  still prices a key at 4,096 bytes; it cuts a page at 4 MiB of keys as well as at 1,024 rows
  ([Parquet output](../parquet-output.md#writer-choices)), which keeps a page of longer keys within that price. The
  manifest allowance (`dataset.ManifestLimits`) prices both bounds of every part at 4,096 bytes too, in
  aggregate: only many thousands of parts all bounded by keys past it would exceed it.
- Symlinks below the root are never followed (`O_NOFOLLOW`, `AT_SYMLINK_NOFOLLOW`). Names are bytes: one that
  is not valid UTF-8 is listed with its exact bytes, like any other. Mount points are crossed unless `--one-file-system` (`Options.OneFilesystem`):
  `New` reads the root's device (refused under the `names` tier, or where the engine reads no device: the
  portable engine on a platform other than Linux, macOS and Windows), and `emit` keeps a `dir` row whose `dev`
  differs but hands on no child range for it (counter `other_device_dirs`). On Windows no device or volume is
  compared (`otherDevice`, `volumeDev`; the `links` tier's `dev` is never compared either): a junction or a volume mount point is a `symlink` row, never walked, with or without the
  option, which is accepted (`filesystem_boundary` `reparse_point`) to record the filter. A directory reparse
  point of another tag (`IO_REPARSE_TAG_DFS`, a cloud placeholder) keeps the directory bit and is walked, and a
  path the redirector sends to another server shows no boundary at all: the option does not stop either. On macOS the opened-handle volume check also stops same-device
  foreign volumes. `Report` adds `one_file_system` to the coverage's filters, so a dataset says it
  stops at the root's filesystem. `--no-dirs` (`Options.NoDirs`) walks every directory but writes no `dir` row, and
  `Report` adds `no_dirs` to the filters likewise. `Report` names the tier and, where a stat tier reads them
  (`--no-link-target` off), `link_target` as collected (the manifest's `collected`).
- **Recursive exclusion pruning.** Before workers start, `Opened.Begin` derives an immutable directory
  predicate from the run's filters (`registry.Env.Filter`, which the command's local source hands it). The filter package's glob compiler recognizes terminal `/**`
  segments without a second glob language. The shared `emit` path retains the directory row for the
  ordinary row filter but submits no child for a matching directory; this applies to ordered listings,
  counts, native chunks/slabs and the portable engine. Includes, metadata filters, directory-row-only
  exclusions and regexes do not themselves stop descent. The run's own output (`Options.Skip`, set by
  `Opened.Exclude` for each path the pipeline names: a dataset directory, or a text file, its temporary and
  its scratch directory) is dropped in `emit` before anything else: its row, and for a directory its
  traversal. A directory is located by identity, a file by its name in its directory as the system resolves
  the path, never cleaned first on POSIX (`link/..` is the parent of `link`'s target; Windows removes `..` by
  its spelling, `GetFullPathName`, and the path is taken so there), located by identity (the file
  may not exist yet, and a commit renames over the name). An existing file is left out under the spelling the
  directory stores (`storedName`): where it ignores case, the one entry equal without case that is the same
  file by identity, never another hard link or a name merely equal without case. `skipped_outputs` counts the entries dropped and
  `coverage.exclusions` lists their keys, sorted.
  The report's `pruning` classifies configured rules: `reason` is `no_excludes`, `row_filters_only`, or
  `recursive_excludes`; `recursive_globs`, `row_only_globs` and `regex_rules` count the rules in each
  class, as given (repeated rules count separately). `pruned_dirs` counts child traversals prevented,
  once per directory even when rules overlap;
  `dirs_read`, `skipped_outputs` and `other_device_dirs` keep their existing meanings. No excluded
  descendant counts or bytes are estimated. Original filter descriptions stay in `coverage.filters`;
  pruning adds neither unreadable entries nor output-directory exclusions.
- An entry whose stat fails is unreadable (`coverage.unreadable`, exit 3) unless the include and exclude rules
  leave it out by its key alone (`filter.Spec.KeyExcluded`, which the row filter applies first; a size, time or
  class filter cannot decide without the stat): a file's key excluded, or a directory's row excluded and its
  descendants pruned, since an entry that is not statted is not walked; with its kind unknown (`DT_UNKNOWN`), both.
  `filtered_stat_errors` counts those, which the listing drops as the filters would have.
- A link target is at most 4,096 bytes (`link_target`'s `schema.Spec.MaxBytes`, PATH_MAX): the writer's
  admission prices it at that width, and a longer target stops the run with the typed `schema.ErrValueTooLong`, as
  a key over its cap does; a target within it is written byte for byte.
- **Filesystem space** (`space.go`, the manifest's `filesystem`). Measured once, when the walk ends (`Report`),
  for the filesystem that holds the root: for a symbolic link to a file, which lists as the link, the link's
  directory; for a link to a directory, which is walked through, the target's. With `--one-file-system` that is
  the filesystem the walk stayed on; without it, nested mounts are listed but not described. `statfs` on Linux
  (fragment units) and macOS; `GetDiskFreeSpaceEx` on Windows, where the total and available bytes are within the
  caller's quota and the free bytes are the volume's, so free can exceed total. It is left out on other
  platforms, when the call fails, for a filesystem that reports a size of 0, and when the root no longer names
  what `New` found (a link retargeted, a directory replaced); a failure never fails the listing. It is what the
  filesystem says of itself, not a disk's capacity: volumes that share a pool (an APFS container, a ZFS pool)
  each report the pool's free space, and the dataset's size is not `total_bytes` minus `free_bytes` (scope, hard
  links, shared blocks, snapshots and the filesystem's overhead differ). Added under `extensions.swath` `version` 1.
