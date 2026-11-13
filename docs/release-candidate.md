# Differences in 0.4.0-rc.2

These pages describe swath as it will be released in 0.4.0. The newest build you can download is the release
candidate 0.4.0-rc.2, and it behaves differently in the places below. Each row was checked by running both: the
rc.2 release binary and a build of the code these pages describe.

> [!WARNING]
> On 0.4.0-rc.2 a local listing that could not read some entries exits with code 0, and with `-q` it says
> nothing. Automation running rc.2 must not take exit code 0 as proof of a complete scan: check the dataset's
> `coverage.skipped_entries` or the run report's `outcome.entry_errors` for 0 as well.

## Commands and options

| What you run | 0.4.0-rc.2 | These pages |
|---|---|---|
| `swath du --from-dataset DIR` | not available: `unknown flag --from-dataset`, exit 2 | totals a saved local dataset without listing again |
| `swath du` on a tree with hard links | counts a file under each of its names; `--count-links-once` counts it once | counts it once, as `du` does; `-l` (`--count-links`) counts every name |
| `swath diag list --trace FILE` | not available: `unknown flag --trace` | writes a trace of an object-store listing |
| `-o NAME.parquet` | writes a dataset directory with that name | refused: use a name without `.parquet`, or end it with `/` |
| `swath du --max-depth`, `-s`, `--apparent-size`, `-x` | `unknown flag` | names the swath option that stands for it, for example `--depth N` |

## Entries that could not be read

| What happens | 0.4.0-rc.2 | These pages |
|---|---|---|
| An entry cannot be read (a directory without permission) | warning `1 entry could not be read: the output leaves them out` (`the totals` for `swath du`), exit 0; nothing with `-q` | warning, then `swath: 1 entry could not be read and is missing from the output`, also with `-q`; exit 3 (`incomplete`) |
| The source directory itself cannot be read | warning, empty totals or listing, exit 0 | `swath: DIR: permission denied`, exit 1 |
| `swath show` | `coverage ... skipped=1 ...` | `coverage ... unreadable=1 ...` |
| The manifest | `extensions.swath.coverage.skipped_entries` | `extensions.swath.coverage.unreadable` |
| The run report | version 5: `outcome.unreadable` is true or false and `outcome.entry_errors` the count; class `completed` | version 7: `outcome.unreadable` is the count; class `incomplete` |

## Listings

| What happens | 0.4.0-rc.2 | These pages |
|---|---|---|
| A recursive exclude, `--exclude 'dir/**'` | the walk still enters `dir/`, and warns about entries there it cannot read | the walk skips `dir/` |
| Text output inside the listed tree, `-o TREE/rows.tsv` | the listing includes its own temporary file and scratch directory | they are left out |
| `swath diag list --max-keys N` stops before the end | text rows exit 0; a dataset exits 1 | exit 3 (`incomplete`) for both |
| A malformed source URL (`s3://`) or a `--sort-buffer` below 128 KiB | exit 1 | exit 2, as a usage error |
| `--max-duration` ends a text-file run | the statistics count `1 file` although it is discarded | `no file written`, and the next step: run it again, or `swath resume DIR` for a dataset |

These pages also describe two changes not checked here by running rc.2: a `swath du` of a million rows or more
on a terminal ends with a hint to save an inventory and total it with `--from-dataset`, and a run stopped by
Ctrl-C or `SIGTERM` now ends by that signal (the shell still reports 130 or 143), where rc.2 exited with the
code.

The full list is the Unreleased section of the [changelog](../CHANGELOG.md). Install rc.2 from
[Install](install.md).
