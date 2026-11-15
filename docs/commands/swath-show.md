# swath show

swath show prints a committed dataset's summary, or an interrupted run's recorded command and progress.

## Synopsis

<!-- synopsis:begin -->
```text
swath show DIR [OPTIONS]
```
<!-- synopsis:end -->

## Description

For a committed dataset, `swath show` prints the manifest's summary for people, in reading order: the verdict
first, then labelled lines for the source, when it was listed (local time, the zone named), what the dataset
holds (its rows, the bytes of the objects they list when every size is known, and the key range, clipped to
a terminal's width and left out where it cannot fit), the
coverage, and last how it is stored (the Parquet parts and their bytes, the SeekParquet format and the swath
schema):

```text
$ swath show media-inventory
media-inventory: complete
  source    /home/you/media (whole tree)
  listed    2026-11-15 21:03:25 to 21:03:25 CET (0.0s)
  contents  12 entries, 101 MiB; keys "archive/" .. "videos/trip.mp4"
  coverage  nothing unreadable; no filters; no exclusions
  storage   1 part, 4.05 KiB of Parquet (SeekParquet format 0 draft 1, schema swath/local-entries/v1)
```

The verdict is `complete` when nothing is missing: the listing reached the end of its scope and read every
entry. Otherwise it is `partial:` and what is missing: `partial: 2 unreadable entries` (entries that could not
be read), `partial: --max-keys reached`. The coverage line counts the unreadable entries, names the filters the
run applied by their rules (`filtered: include=*.jpg, min_size=200k`), and what was left out by design
(`excluded: ...`, an output directory inside the tree). A complete dataset without filters covers the source
on the line below the verdict, minus its exclusions
([Understand your results](../results.md#coverage-and-unreadable-entries),
[The swath dataset](../dataset.md#the-swath-extension)). There is no machine form of this summary: a program
reads `manifest.json`.

For the work directory of a dataset written to a bucket (`-o s3://...`, `gs://...` or `az://...` with
`--work-dir DIR`), it prints the same summary of the manifest the run committed there, under the destination
its commit record (`DIR/commit.json`) names, and a last line saying the commit came from that work directory
and whether its cleanup is finished (`finalized`) or a `swath resume DIR` still finishes it:

```text
$ swath show work
s3://out/inv/: complete
  ...
committed from the work directory work (finalized)
```

For an interrupted run, it prints the command the journal recorded, the frozen identity, the parts saved so
far and where `swath resume` will continue:

```text
$ swath show logs-inventory
logs-inventory: interrupted run
  started   2026-11-15 21:04:35 CET (journal version 1)
  command   swath list --endpoint-url=http://127.0.0.1:19480 --anonymous --output=logs-inventory --request-rate=10 --part-interval=1s s3://bkt/logs/
  identity  provider s3, schema swath/s3-objects/v1, writer swath/parquet-r4, part size 64.0 MiB, source s3://bkt/logs/, endpoint http://127.0.0.1:19480, region us-east-1, path style true, fips false, dual-stack false, fetch owner false
  saved     1 part, 1,000 rows; swath resume lists after "logs/2026/00/app-00999.log.gz"
```

The command is recorded in its canonical spelling, each option `--name=value`, then SOURCE. It is printed as a
shell line: an argument the shell would split or expand (a path with a space, a leading `=`) is quoted, in
single quotes, or on Windows in double quotes, so the line can be pasted as it is.

`swath show` reads no rows. `swath du --from-dataset DIR` totals them per prefix
([swath du](swath-du.md#totals-from-a-saved-dataset)); to query a dataset otherwise, read the parts its manifest
lists ([Running swath](../running.md#reading-the-output)).

## Options

<!-- flags:begin -->
- <a id="option-journal"></a>[`--journal`](#option-journal)\
  dump the journal's header and records as JSON lines (diagnostic: the journal's private format, which may change in any release)

<details>
<summary>Plain text, in the <code>--help</code> layout</summary>

```text
      --journal  dump the journal's header and records as JSON lines (diagnostic: the journal's
                 private format, which may change in any release)
```

</details>
<!-- flags:end -->

`swath show` also takes `-v`, `-q`, `--color` and `--log-format` ([swath(1)](swath.md#options)).

## Examples

Summarize a dataset:

```bash
swath show logs-inventory
```

Dump an interrupted run's journal:

```bash
swath show logs-inventory --journal
```

`--journal` is a diagnostic: it prints the journal, swath's private resume record, as it is stored. Its
members, their spelling and their types are not an interface and may change in any release; a program reads
the run report, `progress.json`, `error.json` and the manifest instead.

## Exit status

0 when it printed the summary, or with `--journal` the journal (an interrupted run's, or the one a committed
dataset's run kept); 1 when DIR does not exist, holds neither a dataset nor an interrupted run, or with
`--journal` holds no journal (a committed dataset whose run kept none); 2 for a usage error.

## Files

`DIR/manifest.json`, `DIR/.swath/journal`; for a bucket dataset's work directory `DIR/commit.json` and the
manifest it committed, under `DIR/.swath/`.

## See also

[swath list](swath-list.md), [swath resume](swath-resume.md), [The swath dataset](../dataset.md)
