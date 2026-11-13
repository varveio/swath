# swath

swath lists object stores and file trees in parallel, into a sorted Parquet dataset, as text rows, or as
size totals.

## Synopsis

```text
swath COMMAND [OPTIONS] ...
swath --help | -h
swath --version | -V
```

## Description

swath reads listing metadata (names, sizes, times, storage classes) through each object store's listing API
or by walking a directory tree. It never reads or changes object contents.

| Command | What it does |
|---|---|
| [`swath list`](swath-list.md) (also `swath ls`) | list SOURCE as text rows on stdout or into a `.tsv` or `.jsonl` file, or into a sorted Parquet dataset with `-o DIR` |
| [`swath du`](swath-du.md) | total SOURCE's objects and bytes, per prefix down to `-d` levels |
| [`swath resume`](swath-resume.md) | continue an interrupted dataset run |
| [`swath show`](swath-show.md) | print a dataset's summary, or an interrupted run's recorded command and progress |
| [`swath completion`](swath-completion.md) | print a shell completion script |
| `swath help COMMAND` | the same as `swath COMMAND --help` |
| `swath version` | the same as `swath --version` |

`swath diag` runs `list` or a count with options meant for experiments and comparisons, not for operating the
tool; it is described with the [internals](../internals/diagnostics.md).

### SOURCE

| Form | Source |
|---|---|
| `s3://BUCKET[/PREFIX]` | an Amazon S3 bucket, or an S3-compatible store with `--endpoint-url` |
| `gs://BUCKET[/PREFIX]` | a Google Cloud Storage bucket |
| `az://ACCOUNT/CONTAINER[/PREFIX]` | an Azure Blob Storage container; `https://ACCOUNT.blob.core.windows.net/CONTAINER[/PREFIX]` is the same source |
| a path | a local or mounted directory, or a single file |

The scheme is matched in any case (`S3://bucket`). The prefix is a key prefix, not a directory:
`s3://bucket/logs` lists `logs/` and `logs-old/`, `s3://bucket/logs/` only `logs/`. Each source's URL form,
credentials and limits are in [sources.md](../sources.md).

### Command line

Options are parsed GNU-style: short options bundle (`-vq`) and take attached values (`-d1`, `-oPATH`); long
options take `--name value` or `--name=value`; options and operands may come in any order; `--` ends the
options. A long name with one dash (`-depth`) is refused. A switch that is on by default is turned off by
its `--no-` spelling (`--no-progress`, `--no-s3-path-style`). A list option (`--columns key,size`,
`--storage-class STANDARD,GLACIER`) refuses an empty element (`key,`) with exit 2.

The [global options](#options) (`-v`, `-q`, `--color`, `--log-format`) may stand anywhere on the line, before
or after the command word, and count only before `--`. A command line refused early, an unknown option among
them, still reports in the `--log-format` it names:

```text
$ swath list /data --bogus --log-format json
{"ts":"...","level":"error","event":"outcome","msg":"unknown flag --bogus","class":"usage","exit_code":2,"resumable":false,"message":"unknown flag --bogus"}
```

An unknown option is a usage error that names the nearest one and the help to read:

```text
$ swath list s3://bucket --region eu-west-1
swath: unknown flag --region (did you mean --s3-region?)
See 'swath list --help'.
```

An option that belongs to one provider carries its prefix (`--s3-region`, `--gcs-credentials-file`,
`--azure-sas-token`) and is refused, with exit 2, for another provider's source. Options every object store
shares (`--endpoint-url`, `--anonymous`) and the local tree's own (`--one-file-system`) are bare. An older
spelling (`--endpoint`, `--profile`, `--region`) gets the same suggestion of the current name.

A value that can carry a secret is never shown or recorded as typed: `--azure-sas-token`,
`--s3-bearer-token-command`, and the user information, query or fragment of SOURCE, `--endpoint-url` or a URL
given to `-o` are `[redacted]` in errors, logs, the report and the journal. Plain paths are shown as typed.

`-h` prints a command's short help (the usage, the examples and the most used options), `--help` all of it.
Both go to stdout and exit 0.

### Sizes and durations

A SIZE is a decimal number and an optional unit, in any case: bytes without one (or `B`); `K`, `M`, `G`,
`T` and `KiB`, `MiB`, `GiB`, `TiB` are binary; `KB`, `MB`, `GB`, `TB` are decimal. `64M` is 64 MiB, `64MB`
64,000,000 bytes. `1.5G` is accepted; a size that does not come to whole bytes (`1.3B`) is refused. What swath
prints for people uses binary units only (`KiB`, `MiB`, `GiB`); TSV, JSONL and the reports carry exact bytes.

A DURATION is a number with a unit: `90s`, `1h30m`, and `d` (24 hours) and `w` (7 days) besides (`2d`,
`1d12h`). A negative duration is refused. `0` turns off `--max-duration`, `--stall-timeout` and
`--part-interval`.

### stdout and stderr

stdout carries data only: text rows, totals, or the run report with `--report -`. A run that writes a dataset
(`-o DIR`) or a text file (`-o FILE`) prints nothing on stdout unless `--report -` puts the report there; `-o -`
sends the rows to stdout. Everything else goes to stderr:

- **Errors and warnings**, each one line starting `swath:`. A usage error adds the help to read.
- **The progress line**, on a terminal that does not also show the rows, redrawn every second:

  ```text
  listing · 31,000 objects · 30,989 keys/s (avg 15,500) · 0 parts · 1 in flight · 2.0s elapsed · 80 API calls
  ```

  A local tree counts `entries` (and `entries/s`), `--all-versions` counts `rows`. When stderr is not a
  terminal, `--progress` appends the same record as a line every 30 seconds (`--progress-interval` sets it).
  There is no percentage or ETA: a listing does not know its total in advance. A warning erases the line and
  it is drawn again below.
- **The statistics**, at the end of a run that took over 1.5 seconds, wrote output or did not complete:

  ```text
    60,004 objects in 0.2s · 317,481 keys/s
    214 API calls · in flight avg 28.06 · peak 58
    1 file · 142 KiB written · peak RSS 91.3 MiB
  ```

  A run that did not complete starts with `INCOMPLETE (class)` and, when it can be resumed, the command to do
  it. `--stats` and `--no-stats` force them on or off. `--color` colors the progress line and the statistics
  (`INCOMPLETE` in red).

`-v` adds a line saying where the rows go (`swath: listing s3://bkt to all.tsv.zst (attempt 57ee...)`) and
shows progress as appended records; `-vv` adds every failed request and retry with its cause; `-vvv` every
request, its credentials and tokens redacted. `-q` drops the warnings and the automatic progress and
statistics; `-qq` also drops the final error line, leaving the exit code and the reports.

When requests keep failing with one cause for 10 seconds without an answer, swath says so without `-v`, and
again at most once a minute while it lasts:

```text
swath: warning: S3 127.0.0.1:9: connection refused (retrying; 14 attempts)
```

`--log-format json` writes stderr as one JSON object per line instead, with no redrawn line and no statistics
block: [reference/log-events.md](../reference/log-events.md).

On Unix, `SIGUSR1` (and `SIGINFO`, Ctrl-T, on macOS and the BSDs) prints one progress record on stderr, whatever
the progress settings.

## Options

These options are taken by every command, before or after the command word. `-h, --help` and `-V, --version`
are described above.

<!-- flags:begin -->
```text
  -v, --verbose               a start line, progress records; -vv, -vvv: the request log
                                -vv adds each failed request and retry with its cause, -vvv every
                                request (credentials and tokens redacted).
  -q, --quiet                 no warnings, no automatic progress or statistics (-qq: no errors)
                                With -qq the exit code and the reports stand alone.
      --color auto|always|never
                              color the progress line, the statistics and the help (default auto)
                                auto: on a terminal, without NO_COLOR or TERM=dumb; CLICOLOR_FORCE
                                forces it.
      --log-format text|json  stderr as text, or json: one JSON object per line (default text)
                                json: warnings, the start line, progress records, the request log
                                and the outcome as events, no redrawn line or statistics block.
```
<!-- flags:end -->

## Examples

List a prefix as text rows and keep the first ten:

```bash
swath list s3://bucket/logs/ | head
```

Write a sorted Parquet dataset:

```bash
swath list s3://bucket/logs/ -o logs-inventory
```

Total a local tree one level down:

```bash
swath du -d 1 /data
```

Print the short help of `swath du`:

```bash
swath du -h
```

## Exit status

| Code | Meaning |
|---|---|
| 0 | completed (also a reader that closed the pipe early, as `head` does) |
| 1 | failed, and a resume will not fix it |
| 2 | a usage error, or a refusal that is the caller's to fix |
| 3 | finished, but the output is incomplete: entries could not be read, or `--max-keys` stopped the listing |
| 74 | the output's filesystem is full |
| 75 | stuck: no progress for `--stall-timeout` |
| 124 | `--max-duration` reached |
| 130 | interrupted (SIGINT) |
| 143 | terminated (SIGTERM) |

What each one means and what to do: [reference/exit-codes.md](../reference/exit-codes.md).

## Environment

| Variable | Effect |
|---|---|
| `NO_COLOR` | set and not empty: no color with `--color auto` |
| `CLICOLOR_FORCE` | color even when stderr is not a terminal |
| `TERM` | `dumb`: no color with `--color auto` |
| `TZ` | the time zone of the tables' local times |
| `TMPDIR` | where text rows on stdout spill (see [running.md](../running.md#sizing-a-run)) |
| `AWS_*`, `GOOGLE_APPLICATION_CREDENTIALS`, `AZURE_STORAGE_*` | credentials, regions and endpoints: [sources.md](../sources.md) |
| `SWATH_CPUPROFILE`, `SWATH_MEMPROFILE` | write Go CPU and allocation profiles to that file and beside it, split at the end of listing: `FILE.listing`, `FILE.tail` (CPU), `FILE.exit` (allocations); `SWATH_MEMPROFILERATE` sets the allocation sample rate ([phase profiles](../internals/diagnostics.md#phase-profiles)) |

## See also

[swath-list](swath-list.md), [swath-du](swath-du.md), [swath-resume](swath-resume.md),
[swath-show](swath-show.md), [swath-completion](swath-completion.md),
[getting-started.md](../getting-started.md), [output.md](../output.md), [sources.md](../sources.md)
