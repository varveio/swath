# Stop, resume and refresh

A dataset run of an object store that stops before it finishes can be resumed: `swath resume DIR` keeps what
the run saved and lists the rest. To list the same bucket again later, write a new dataset and pass the previous
one with `--seed-from`.

<a id="stopping-and-resuming"></a>

## Resume an interrupted run

### A run that can be resumed

A run that stops early says so in its closing lines and names the command that continues it. Here a deadline
stops a listing of a public NOAA prefix (`--request-rate 2` slows this small listing so that the deadline
stops it partway, and `--part-interval 1s` saves parts as it goes):

```text
$ swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 \
    -o inventory --request-rate 2 --part-interval 1s --max-duration 11s
  stopped: --max-duration reached; 2,033 saved, 2,033 listed in 11.0s · 185 keys/s → inventory (2 parts, 50.7 KiB)
  next: swath resume inventory
swath: deadline: --max-duration reached (swath resume inventory --max-duration 0 continues it without a deadline; a plain resume keeps the recorded one)
```

Ctrl-C ends the same way, with `interrupted:` and `swath: stopped by interrupt`. `saved` counts the rows in
the parts written so far; the rest of what was listed is listed again.

`DIR/error.json` holds the outcome, and `"resumable": true` there means `swath resume DIR` continues the run.
The exit status says the same: 124 (`--max-duration`), 130 (Ctrl-C), 143 (SIGTERM) and 129 (SIGHUP: the
terminal or SSH session closed, unless the run is under `nohup`) can be resumed, and so can 75 (stuck), 74
(disk full) and 1 (once its cause is fixed) when `error.json` says so
([Exit codes](reference/exit-codes.md#what-a-supervisor-does-with-each-code)).

Press Ctrl-C once: only the first signal counts, and swath exits on its own at most 20 s after it. A kill
(SIGKILL, an out-of-memory kill) or a crash loses nothing the run saved, and `swath resume DIR` continues it the
same way; its `error.json` is missing then, or an earlier attempt's.

Only a dataset run of an object store (S3, GCS or Azure Blob with `-o DIR`) can be resumed: it keeps a journal
of what it saved in `DIR/.swath/`. Leave that directory in place and do not edit its files: without it there
is nothing to resume. A run of a local or mounted filesystem, a text output (`-o rows.tsv`) and `swath du` keep no
journal and are run again: a filesystem dataset with `--overwrite`, or into a new directory.

### Resume it

`swath show DIR` says where a stopped run is, and `swath resume DIR` continues it:

```text
$ swath show inventory
inventory: interrupted run
  started   2026-11-17 12:04:05 UTC
  command   swath list --anonymous --s3-region=us-east-1 --output=inventory --request-rate=2 --part-interval=1s --max-duration=11s s3://noaa-gestofs-pds/stofs_2d_glo.20230113/
  saved     2 parts, 2,033 rows; swath resume lists after "stofs_2d_glo.20230113/stofs_2d_glo.t06z.guam.f126.grib2"
$ swath resume inventory --max-duration 0
  complete: 5,252 objects (2,033 from earlier attempts), 383 GiB in 4.0s · 795 keys/s → inventory (4 parts, 128 KiB)
  next: swath show inventory · swath du --from-dataset inventory
```

The resume runs the command the journal recorded, the one `swath show` prints, with every option as recorded.
The options `swath resume --help` lists (workers, credentials, time limits, the spill cap, reporting) may be
given again, and the last value wins: here `--max-duration 0` lifts the recorded 11-second deadline
([swath resume](commands/swath-resume.md)). Every other option is fixed for the run, and changing it is
refused:

```text
$ swath resume inventory --all-versions
swath: --all-versions is fixed for the run and cannot change on resume
See 'swath resume --help'.
```

Secrets are never recorded: a run started with `--azure-sas-token` (or a SAS in `AZURE_STORAGE_SAS_TOKEN`) or
`--s3-bearer-token-command` needs it again. `swath resume` writes a report only when given `--report`; give
each attempt a new path to keep each attempt's report.

A resume of a dataset that is already complete has nothing to do and exits 0, so a supervisor may resume until
exit 0:

```text
$ swath resume inventory
inventory is already complete: nothing to resume
```

### What a resume keeps and lists again

- **It keeps every saved part**, and checks each against the journal (its size and SHA-256) before it lists
  anything. That reads every saved part in full once, so budget for it on a very large dataset: from the read
  rates measured on a 4-vCPU VM, 10,000 parts of 64 MiB would take roughly 30 to 45 minutes, an estimate
  ([measurements](internals/measurements.md#recovery-and-part-count)). Meanwhile the progress line reads
  `verifying`, with the bytes verified so far.
- **It lists after the last saved key**, the one `swath show` prints. What the stopped run listed and had not
  saved is listed again, except the ranges it had kept whole in its spill files on disk: the resume reads
  those back instead of listing them. They stay in `DIR/.swath/`, so resume from the same directory: on the
  same disk, or on a persistent disk moved to another machine
  ([on a preemptible or spot machine](production.md#on-a-preemptible-or-spot-machine)). How the spill files are
  checked and reused is in [Ordering (internals)](internals/ordering.md#durable-spill).
- **The closing line counts what it kept:** `(2,033 from earlier attempts)`.

What was saved or kept on disk before the stop stays as it was listed then: a key deleted from the bucket since
stays in the dataset, and one added among those keys is missed. Everything else is listed as the bucket is
during the resume. On a bucket that did not change, the rows are the same as an uninterrupted run's. To list
the bucket as it is now, start over with `--overwrite`, or remove `DIR/.swath/spill/durable/` before resuming,
which lists everything after the last saved part again.

### When a resume is refused

A refused resume leaves the directory as it was.

| What swath says | Why | What to do |
|---|---|---|
| exit 2, `--NAME is fixed for the run and cannot change on resume` | the option is not one a resume may change | give only the options `swath resume --help` lists, or start over with `--overwrite` |
| exit 2, `cannot resume: region was us-east-1, now eu-west-1 (start over with swath list --overwrite, or resume with the original settings)` | the environment or `--s3-profile` resolves another region or endpoint than the run's | restore the original environment, or start over with `--overwrite` |
| exit 2, `nothing to resume: no interrupted run in the output directory`, or `text output ... is not resumable` | no journal: a filesystem run, a text output, or another directory | run `swath list` again |
| exit 1, `journal version N is not supported`; exit 2, `an interrupted run written by swath 0.3`; exit 2, `this swath narrows the listing to the --include's literal prefix` (Azure Blob) | another swath version started the run | finish it with that version, or start over with `--overwrite` |
| exit 1, `another swath process owns this output directory` | the stopped run has not exited yet, or a wrapper was killed and swath under it lives on | wait for that process to exit (stop a wrapper's whole process group), then resume |
| `"resumable": false` in `error.json` | the run cannot be continued as recorded: the bucket does not exist, it is in another region, or the service returned an invalid page (`cause` `protocol`) | fix the command, and start over with `--overwrite` |
| exit 3, `DIR is partial: ... it cannot be resumed` | a committed dataset that left out entries it could not read is final | fix the permissions and list again |

The rarer refusals (a saved part or the journal changed on disk, a part that cannot be listed the same way
again) are in [The resume is refused](troubleshooting.md#the-resume-is-refused).

### A dataset in a bucket

A run that writes its dataset to a bucket (`-o s3://BUCKET/PREFIX`, `gs://…` or `az://…`, with
`--work-dir DIR`) is resumed by its work directory: `swath resume DIR`. Before it lists, the resume reads every
part already uploaded back from the bucket to check it, so it downloads what was published once. An Azure
destination's `--output-azure-sas-token` must be given again. A lost work directory cannot be resumed: start
again at a new prefix. The rest, with what a resume of a run that reached its commit does, is in
[Writing to a bucket](production.md#writing-to-a-bucket) and
[on a preemptible or spot machine](production.md#on-a-preemptible-or-spot-machine).

<a id="overwrite-and-refresh"></a>

## Start over with `--overwrite`

A directory that already holds a dataset is refused as an output (exit 2):

```text
$ swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 -o inventory
swath: inventory: output directory is not empty (use --overwrite to replace a previous dataset)
```

One that holds an interrupted run is refused too, and the message says so: `it holds an interrupted run
(swath resume DIR continues it; --overwrite starts over)`. To replace the dataset, or to discard the
interrupted run, run the command again with `--overwrite`:

```text
$ swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 -o inventory --overwrite
  complete: 5,252 objects, 383 GiB in 0.9s → inventory (1 part, 121 KiB)
  next: swath show inventory · swath du --from-dataset inventory
```

The previous dataset stays readable until the new one commits: the new run lists into `.swath/next/` inside
the output, and its commit puts the new dataset in place of the old one, whose parts are removed after. A run
that fails or is stopped before its commit leaves the previous dataset as it was; a stopped run of an object
store can then be resumed (`swath show DIR` says so), and a new `--overwrite` starts it over. Do not query the
dataset while the new one is put in place: it reuses the part names (`data/part-NNNNN.parquet`), so a reader
can find parts missing or replaced. An interrupted installation (a crash, or on Windows
[another program holding one of its files](troubleshooting.md#the-output-directory-is-refused)) leaves the
previous dataset or none, never a manifest naming missing parts, until `swath resume DIR` or a new
`--overwrite` finishes it. A dataset in a bucket is never replaced: write each run to a new prefix.

Where readers must always find a dataset, even while a new one is written (a refresh read around the clock),
write each run to a new directory and never overwrite it. Switch readers through a pointer outside the datasets
(a generation file, a database row, a symlink swapped by rename) once the new directory's `manifest.json`
exists, and delete an old generation only when no reader uses it.

<a id="seeding-a-refresh-from-the-previous-dataset"></a><a id="warm-start-for-repeat-listings"></a>

## Refresh from the previous dataset

To list a bucket again, write a new dataset and pass the previous one as `--seed-from`:

```text
$ swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 \
    -o inventory-2 --seed-from inventory --report inventory-2.json
  complete: 5,252 objects, 383 GiB in 0.5s → inventory-2 (1 part, 121 KiB)
  next: swath show inventory-2 · swath du --from-dataset inventory-2
```

The previous dataset says where the bucket's keys fall, so the run divides the work into ranges of about equal
numbers of keys from the start instead of discovering the key space as it goes. This run sent 7 requests
(the report's `requests.http_attempts`) where the same listing without `--seed-from` sent 32. With a dataset (or
a cut file with counts, below) a run's requests approach one per 1,000 keys on S3 and GCS and one per 5,000 on
Azure Blob. The report's `invocation.prior` names what was used (swath calls the previous dataset the prior):

```json
{
  "path": "inventory",
  "source": "dataset",
  "cuts": 6,
  "counted": true,
  "cuts_file": "read"
}
```

**It does not skip unchanged objects.** A seeded run lists every key again and copies nothing from the previous
dataset: the new dataset has the same rows as a listing without `--seed-from`. What it saves is the work of
finding how the keys are spread: requests and time at the start, not the listing itself.

**The source and scope must match.** The previous dataset's source kind, bucket (on Azure, container) and
prefix are compared with the run's (the prefix an `--include` narrows the listing to, if any), and a mismatch
is refused before listing (exit 2): a dataset of `logs/` does not seed `logs/2026/`.

```text
swath: --seed-from inventory: unusable prior: the prior was taken under the prefix "stofs_2d_glo.20230113/", the run lists "stofs_2d_glo.20230113/00/"
```

The endpoint, the region and the Azure account are not recorded, so they are not compared: make sure the
previous dataset is of the same account. The previous dataset cannot be the output directory, so `--overwrite`
of the directory it is in is refused: write each refresh to a new directory, as above.

**When it helps.** On 251 production key lists served by a local test server at 50 ms per page, with 64
workers, a seeded run took a median 0.76 of the unseeded run's time, and one list in 251 was more than 5 %
slower; seeded from a dataset of a bucket that had since grown by about 45 %, runs still listed within 1.11 to
1.15 times the ideal for 64 workers ([measurements](internals/measurements.md#warm-start-from-the-previous-dataset---seed-from)).
Seeded from a dataset or a cut file with counts, a sorted run also lists the bucket front to back, so the rows
held ahead of the output, and the spill disk they take, stay bounded instead of growing with the bucket. That
is expected, not guaranteed: a region that grew since the previous dataset can still take more, so keep the
spill disk sized for the bucket's growth ([disk sizing](size-a-run.md#disk-sizing);
[how](internals/ordering.md#the-frontier-window)). The previous dataset also gives the size to plan for:
`swath show PREVIOUS` prints its object count.

**Its limits:**

- Local and mounted filesystems take no `--seed-from`: `unusable prior: ... the local source does not list in
  key ranges` (exit 2).
- A previous dataset far out of date (a part of the keyspace grown many times since), at a very high worker count
  under a spill cap far below what the listing needs, can crawl or time out; it was seen at 2,048 workers under
  a spill cap of about a tenth of what the listing needed. Give a larger `--spill-budget` or more spill disk,
  fewer workers, or list without `--seed-from` (tracked in [#576](https://github.com/varveio/swath/issues/576)).
- On Azure Blob, a previous dataset whose key pages end at a blob name longer than 1,024 bytes fails the run,
  and its resume fails at the same name: list without `--seed-from`.

These are also in [Known limits](troubleshooting.md#known-limits).

**A dataset in a bucket.** `--seed-from` reads a local path: copy the dataset's `PREFIX/cuts.txt` and pass that
file ([the cut file](dataset.md#the-cut-file)).

**A cut file from elsewhere.** Anything else that knows the keys (an S3 Inventory, a list of prefixes) can seed
a run as a cut file, but a cut file records no source, so only its keys are checked: prefer the dataset. A cut
file holds one key per line, ascending bytewise without repeats, each inside the scope, optionally followed by a
tab and the number of keys expected since the previous line (on every line or none); at most 262,144 lines and
16 MiB (thin a larger one: every nth line, its count the sum of the lines it replaces). An unsorted or repeated
key, a key outside the scope or a file past those bounds is refused (exit 2). A cut file without counts divides
the work but does not bound the spill.

**Resuming a seeded run.** `swath resume` reads `--seed-from` again and may change or drop it
(`--seed-from=`); a resume whose previous dataset can no longer be read warns and lists without it.
