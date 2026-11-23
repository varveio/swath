# Local and mounted filesystems

A path lists a directory and everything below it, or a single file as its one row. Its rows are entries:
files (every kind that is not a directory: regular files, symbolic links, devices) and directories, each
directory's key ending in `/`. swath walks the directory through the operating system, so a mounted share
(NFS, SMB, a FUSE mount) lists as any directory does.

## The smallest command

```bash
swath du -d 1 ~/Downloads        # totals per directory, one level down
swath list ~/Downloads | head    # every entry, as TSV in a pipe
```

No account and no credentials: [Your first inventory](../first-inventory.md) walks through both and a saved
inventory.

## Permissions

swath needs to read the directories it walks and to stat their entries; it never opens a file's contents.
A root directory that does not exist or cannot be opened (permissions) stops the run before anything is
written, with exit 1 and one line on stderr (`swath: DIR: permission denied`).

Entries below the root that cannot be read (permissions) are left out: swath names up to five of them with
their cause on stderr and exits 3, its last line counting them; the report's `outcome.unreadable` and the
dataset's `coverage.unreadable` hold the count ([Troubleshooting](../troubleshooting.md#entries-could-not-be-read)).

## Options for this source

| Option | Effect |
|---|---|
| `--metadata names\|stat\|statx` | what is read per entry: names only (no size or time), `stat` (the default), or `statx`, which adds `created_at` (Linux) |
| `--no-dirs` | omit the directories' rows (they are still walked) |
| `--no-link-target` | do not read symbolic links' targets |
| `--one-file-system` | stop at a filesystem boundary; on macOS, also stop at a different volume even when its device number matches; keep the directory row |

`swath du` leads with the space on disk, as `du` does, and also takes `-A`, `--apparent-size` (the files'
lengths instead), `--allocated` (require the space on disk) and `-l`, `--count-links`
(count a hard-linked file under each of its names; by default it counts once), and `--max-inodes`, the most
hard-linked files it remembers ([How sizes are counted](../totals.md#how-sizes-are-counted)).

On Linux, `--one-file-system` compares device numbers, like `du -x` and `find -xdev`;
a bind mount on the same device is still traversed. On macOS it also checks the containing
volume of each opened directory. A scan of `/` therefore stops at APFS Data through both
firmlinks such as `/Users` and `/System/Volumes/Data`, even when their ordinary device numbers
match System's. Selecting a directory on Data as the root scans that volume normally.
A volume identity lookup failure prevents descent and counts as unreadable, not as a confirmed boundary.
Ordinary `dev` and `inode` columns keep their original stat values. The dataset records the
`one_file_system` filter, but does not identify individual stopped directories.

`--no-dirs` and `--one-file-system` mark a dataset as not the whole directory (`coverage.filters` holds
`no_dirs` or `one_file_system`). `--one-file-system` and `--allocated` need each entry's stat: they are refused
with `--metadata names`.

A SOURCE that is a file is read as the same entry in a listing of its parent: at the `--metadata` tier, and a
symbolic link as the link (kind `symlink`, its target in `link_target`), never followed. Its key is its name.

Recursive exclude globs can prevent descent into entire subtrees ([Filters](../filters.md#globs)); other row
filters do not stop the walk. On a filesystem `--workers` defaults to 4 per CPU, at most 64.

## Writing a dataset

A directory's dataset can be written to a local directory (`-o DIR`) or to a bucket (`-o s3://...`,
`gs://...` or `az://...` with `--work-dir DIR`; see that store's page).

The output may not be the listed directory itself or contain it. A new or empty directory inside the listed
one is accepted as the output: the walk skips it and the dataset records it in `coverage.exclusions`.
`--overwrite` of an output inside the listed directory is refused (exit 2): write the dataset to a new
directory outside it. A text file inside the listed directory is left out the same way, with its temporary
and the attempt's scratch directory beside it: the rows never list the run's own output, on a first run or a
repeat.

## Limits and known differences

- A local listing keeps no journal: an interrupted one is run again, and `swath resume` does not apply.
- Names that are not valid UTF-8 are listed with their exact bytes: escaped in TSV and the table, in
  `key_base64` (with `key` null) in JSONL ([Output](../output.md#text-rows)). A dataset's `key` column is binary for the same
  reason.
- A directory's row carries its own size (`st_size` on Unix); `swath du` counts directories separately and
  leaves their sizes out of its byte totals.
- Paths longer than the system takes at once (4,096 bytes on Linux, 1,024 on macOS) are listed: such a
  directory is opened one name at a time below the root, never following a symbolic link. A key is at most
  32,768 bytes: an entry with a longer one is left out and counted as unreadable (the cause names
  `passes the limit of 32768`), a directory with everything below it. A single name longer than the
  filesystem allows (255 bytes on most) stays an error. Windows takes long paths as they are. An output
  inside such a root, named through a symbolic link to the root rather than below its own path, stops the
  run with an error instead of being left out of the listing.
- A walk is not a snapshot: entries created, changed or removed while swath walks may or may not appear. A
  file removed during the walk is not counted as unreadable; a directory removed after the walk found it and
  before it read it is, so the run reports incomplete and exits 3.
