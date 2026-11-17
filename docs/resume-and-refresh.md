# Stop, resume and refresh

How a dataset run stops, how `swath resume` continues it, and how to keep an inventory fresh: replace it in place,
or write each listing to a new directory seeded from the previous one.

## Stopping and resuming

A dataset run of an object store that stops with exit 74, 75, 124, 129, 130 or 143 can be resumed, and so can
one that stopped with exit 1 when its outcome says `resumable` (a fixed credential or permission); what a
supervisor does with each code is in [Exit codes](reference/exit-codes.md#what-a-supervisor-does-with-each-code).

"Resumable" means an object-store run whose journal is intact. A filesystem run keeps no journal: start it
again with `--overwrite`, which replaces the interrupted output, or in a new or empty directory.

**Signals.** SIGINT, SIGTERM and SIGHUP (a closed terminal or SSH session; not under `nohup`) stop the run
gracefully: published parts and the journal stay, and `error.json` and the report are written. Only the first
signal counts; the forced exit follows 15 s later. SIGKILL, a panic, SIGQUIT or an OOM kill loses nothing
durable: the journal is crash-safe and the resume reconciles.

**The workspace.** `DIR/.swath/` holds `lock` (one process per output), `journal` (`swath show --journal`),
`parts/` (encoded parts not yet published), `spill/` (disposable spill, removed by the next run, and
`spill/durable/`: the certified spill files and their `ledger`, kept for the resume and removed when the
dataset commits), `suspect/` (a part a resume must reproduce) and `progress.json`. Do not edit the journal or
its parts by hand; removing `spill/durable/` only costs a resume the relisting it would have saved.

A SIGKILL (or an OOM kill, a power loss) leaves the published parts, the journal, encoded parts not yet
published, the disposable spill and the durable spill: certified files with their ledger records, and files
whose record never reached the ledger. The resume validates every recorded file (size, checksums, every frame,
its certificate) and unlinks every other file; a missing or damaged one is relisted, never an error.

**`swath resume DIR`** replays the command recorded in the journal. Only the resource, request, credential
and reporting options that `swath resume --help` lists may be given again, the last value winning
([swath-resume](commands/swath-resume.md)); `--max-duration` and the other runtime values are replayed from the
first attempt unless given again. Pass a fresh `--report` path each time.

The resume keeps every published part, verifies each against its journal record (size and SHA-256), which reads
every retained part in full once: budget for it on a very large dataset. From the read rates measured on a
4-vCPU VM ([measurements](internals/measurements.md#recovery-and-part-count)), 10,000 parts of 64 MiB take
roughly 30-45 minutes, an estimate;
progress shows as `verified_bytes`. It then continues after the last published key K.
Rows listed but not yet in a published part are listed again, except those in a **certified spill file**:
a range that spilled ahead of the frontier sealed its file with a certificate (every row of a key interval,
as listed), and the resume replays each certificate it can validate instead of listing it, then lists only
the gaps between them: the ranges still listing at the stop, and those not certified
([Ordering (internals)](internals/ordering.md#durable-spill)). Once the gaps are listed, the parallel tail
writes the rest, certified rows included, as on a fresh run. The certificates must fit the resume's spill cap:
a recorded or given `--spill-budget`, else the default derived from the free space the resume finds, the certified
files it kept counted as free (on the same disk the stopped run's cap, less what was written there since, or half
of it when the output is there too); those
that do not fit are listed again (`rejected.budget`). The report says what it reused: `diagnostics.resources.spill.durable.resume` (`accepted`,
`replayed_rows`, `gaps`, `requests_saved_est`, `rejected` by reason).

A range is certified when it finishes; the number of certificates scales with the spill cap (one per 64 KiB of it,
at most 262,144, reached at 16 GiB), so a listing with fewer ranges than that certifies every finished range unless
its spill turns over more than about twice (`no_slot`, `history_exhausted` and `metadata_exhausted` in
`diagnostics.resources.spill.durable` count the ranges that were not). Certified files stay in the work directory,
so resume on the same disk, or on a persistent disk moved to another machine
([on a preemptible or spot machine](production.md#on-a-preemptible-or-spot-machine)).

Secrets are never recorded: a run started with `--azure-sas-token` (or a SAS in `AZURE_STORAGE_SAS_TOKEN`) or
`--s3-bearer-token-command` must be given it again.

A resume of a dataset that is already complete has nothing to do and exits 0, so a supervisor may resume until
exit 0. A dataset committed with partial coverage, one that left out entries it could not read, is final:
nothing continues it, and its resume exits 3 (class `incomplete`); the report keeps the count
(`outcome.unreadable`).

Local trees and text output keep no journal; resume refuses them, and they are run again. DIR is
judged by what is on disk, not by its name: a directory named `rows.tsv/` that holds an interrupted dataset run
resumes, and a text file `rows.tsv` is refused as text output.

**Retained observation.** A replayed certificate serves its rows as the interrupted attempt listed them: a key
deleted from the bucket since stays in the dataset, one added inside a certified interval is missed, and
metadata and filtering are as observed then. The gaps, and every key a run lists, reflect the bucket as it is
during the resume. On a static source the dataset is byte-identical to an uninterrupted run either way. A run
that must see the bucket as of the resume starts over with `--overwrite` (or resumes after removing
`DIR/.swath/spill/durable/`, which relists everything above K). Durable spill needs spill on, `--spill-budget`
of at least 16 MiB, and a parallel range-engine provider that declares coverage fences (S3, GCS and Azure Blob;
not `--sequential`).

**Identity check.** The source, the route (endpoint, addressing, signing region; for GCS the API base and the
billed project), `--s3-fetch-owner`, the schema and the part size are frozen. A resume whose environment or
`--s3-profile` resolves another region or endpoint is refused (exit 2, `refused`) and the directory is left as it
was. Restore the original environment, or start over.

**Refused resumes** and what they mean:

| Message or class | Meaning | Action |
|---|---|---|
| exit 2, identity mismatch | the resume would list another route | fix the environment, or `swath list --overwrite` |
| exit 2, nothing to resume | no journal in the directory | start a fresh `swath list` |
| exit 1, another swath process owns this output directory | a process still holds the output lock: the killed run has not exited yet, or a wrapper was killed and swath under it lives on. The resume is refused, not queued, and changes nothing | wait for that process to exit (stop a wrapper's whole process group, not the wrapper alone), then resume |
| exit 1, `cause` `protocol` | the service returned an invalid page; the journal is marked unresumable | `swath list --overwrite` |
| exit 1, journal version | written by another build | finish with that build, or `--overwrite` |
| exit 1, part altered or journal corrupt | a retained part or the journal changed on disk | `--overwrite` |
| exit 1, ambiguous tail | the part set aside in `.swath/suspect/` cannot be reproduced: the source changed in that part's range | `--overwrite` to start over, or remove `.swath/suspect/` to accept the source as it is now |

### A dataset in a bucket

A run that writes to a bucket (`-o gs://BUCKET/PREFIX`, `-o s3://BUCKET/PREFIX` or `-o az://ACCOUNT/CONTAINER/PREFIX`, with `--work-dir DIR`) is resumed by its work directory:
`swath resume DIR` replays the recorded command with that work directory (it may have moved; the bucket and
prefix stay as recorded). Each retained part's object is read whole and checked against its record (its
version: the GCS generation, the S3 version id or ETag, the Azure ETag; its size and SHA-256), and a resume of a run that reached
its commit first reads the bucket's manifest back: the run's own means the run is complete, and the resume first
finishes what the commit left (this run's leftover parts deleted, `run.json` beside the manifest). A commit that
ended unresolved (exit 75) is resumed the same way: the resume creates the very manifest the run saved. The
destination's credentials may be given again (`--output-gcs-credentials-file`, `--output-s3-profile`), and an
Azure SAS must be (`--output-azure-sas-token`, never recorded); its bucket (Azure: account and container),
prefix, endpoint and S3 region are fixed. A lost work directory loses the
resume: the parts already uploaded are not reused. A new run at the same prefix is not refused (only a prefix
holding a `manifest.json` is), but nothing removes the lost run's parts, so start it at a new prefix
([on a preemptible or spot machine](production.md#on-a-preemptible-or-spot-machine)).

## Overwrite and refresh

`--overwrite` keeps the previous dataset readable until the new one commits: the new run lists into the
output's private workspace (`.swath/next/`), and its commit installs the new dataset in place of the old one,
whose parts are removed after. A run that fails or is stopped before its commit leaves the previous dataset as
it was; a stopped object-store run waits there for `swath resume DIR`, which `swath show DIR` names, and a new
`--overwrite` starts it over. The installation that follows the commit reuses the part paths
(`data/part-NNNNN.parquet`), so a reader reading the dataset meanwhile can find parts missing or replaced: do
not query a dataset while `--overwrite` replaces it. An installation that is interrupted (a crash, or on
Windows [another program holding one of its files](troubleshooting.md#the-output-directory-is-refused)) leaves
either the previous dataset or no dataset, never a manifest naming missing parts, until `swath resume DIR` or
a new `--overwrite` finishes it.

Where readers must always find a dataset, even then (a periodic refresh read around the clock), write each run
to a new directory and never overwrite it. Switch readers through a pointer outside the datasets (a generation
file, a database row, a symlink swapped by rename) once the new directory's `manifest.json` exists, and delete
an old generation only when no reader uses it.

### Seeding a refresh from the previous dataset

A scheduled relisting of the same bucket should pass the previous generation as `--seed-from` (S3, GCS and Azure Blob):

```bash
swath list s3://bucket/logs/ -o /data/logs-2026-11-08 --seed-from /data/logs-2026-11-07
```

The previous dataset's cut file (`cuts.txt`, [the cut file](dataset.md#the-cut-file)) or, without one, its parts'
key page index divide the scope into ranges of about equal expected keys, so the run skips the delimiter seed
and most early splits. For a dataset in a bucket, copy its `cuts.txt` and pass that file. With the default page policy, its requests approach one
per 1,000 keys on S3/GCS and one per 5,000 keys on Azure Blob with a counted prior. The prior
must be the same source and scope: the manifest's source kind, bucket/container and prefix are compared
with the run's (exit 2 on a mismatch; a dataset of `logs/` does not seed `logs/2026/`). The account URI
is not compared for Azure; verify that the prior belongs to the same Azure account yourself. Endpoint
and region are not checked either. A bad prior is refused before listing pages, but Azure namespace
admission requests can already have run. It cannot be the output directory, and `--overwrite` of the prior's
own directory is refused for that reason: write each generation to a new directory, as above. A bucket that
changed shape since is still listed whole and in order; the run report's source counters
(`diagnostics.source.stats.counters.values`) `seed.from.error.grown` and `seed.from.error.shrunk` count the
ranges whose keys moved by more than a factor of 2 (adaptive splits covered them), and
`seed.from.expected_keys` against the counter `objects` is the growth.

Two limits apply to a seeded run: a seed far out of date, run at a very high worker count under a spill cap
much smaller than the listing needs, can crawl or time out; and on Azure Blob a seed whose key pages end at a
blob name longer than 1,024 bytes fails the run ([known limits](troubleshooting.md#known-limits)).

A cut file seeds from anything else that knows the keys (an S3 Inventory, a prefix list, a dataset's
`cuts.txt`), but carries no identity; prefer the dataset. A cut file holds one key per line, ascending bytewise without repeats, each
inside the scope, optionally followed by a tab and the keys expected since the previous cut (on every line or
none), at most 262,144 lines and 16 MiB (thin a larger one: every nth line, its count the sum of those it
replaces). An unsorted or repeated cut, a cut outside the scope, a cut file past those bounds, `--seed-from`
on a local tree, and a prior that is the output directory are refused (exit 2).

The prior divides the work into ranges of about equal expected keys
([warm start](internals/range-engine.md)); the splits stay adaptive and the rows are the same. A counted prior
(a dataset, or a cut file with counts) also sets the order of a sorted run: it lists the prior's tiles front to
back inside a frontier window at the output, so rows listed ahead of the output, and the spill they take, stay
near the window instead of growing with the bucket ([warm start](#warm-start-for-repeat-listings)). `swath resume` reads `--seed-from`
again and may change or drop it (`--seed-from=`); a resume whose prior is unreadable warns and lists without
it. The report's `invocation.prior` names what was used.

### Warm start for repeat listings

`--seed-from PREVIOUS-DATASET` divides the bucket into ranges from the previous dataset's key distribution.
The run then sends no delimiter requests and makes far fewer early splits. On 251 production key lists
served through a fake at 50 ms, a seeded run took a median 0.76x the cold run's wall time and sent 1.02 requests
per 1,000 keys (`warm1`, 2026-11-07). A prior that is 45 % out of date still listed at 1.11 to 1.15x the
64-worker ideal (`stale4`, 2026-11-07). The prior must be the same bucket and prefix, or the run is refused
(exit 2). The output is the same with or without a prior.

A prior with counts (a previous dataset, or a cut file whose every line carries a count) also changes what is
listed ahead of the output: a sorted run lists front to back, taking the prior's tiles in key order only while the rows listed and not
yet written stay within a window of 8 million rows, smaller when `--spill-budget` and `--sort-buffer` hold less
([the frontier window](internals/ordering.md#the-frontier-window)). On a prior that is current, spill then stays
near half the spill budget instead of growing with the bucket, and a small `--spill-budget` is expected to refuse
no frame. Expected, not guaranteed: the window admits tiles, it does not stop the listing inside a tile, so a
range far ahead inside an admitted tile (its neighbour stalled), a region that grew since the prior, keys added
after its last key, and certified durable files waiting to be retired can still take spill past it, so keep the disk sized for
the bucket's growth, and read `diagnostics.resources.window` and `diagnostics.resources.spill.peak_bytes` in the report. With thousands
of workers and a cap of a few hundred MB or less, the window holds fewer tiles: it admits none while the spill
ahead of the output is past half the cap (`diagnostics.resources.window.spill_rows` is the mean rows held at its
refusals), and it evicts the durable certificates behind the output to keep room, so a crash relists those keys.
Rows still leave through one part writer while listing, so such a run can be bound by that writer rather than by
listing. A cut file without counts keeps the cold run's spill ([disk sizing](size-a-run.md#disk-sizing)). The previous dataset also gives
the size to plan for: its row count is in its manifest (`swath show PREVIOUS-DATASET`).
