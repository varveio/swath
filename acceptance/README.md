# Acceptance suite

A black-box check of built `swath` and `swath-replay` binaries, run the way a user runs them. It does not
import or build the code: it is a module of its own (`varve.io/swath-acceptance`), outside `go.work`, with no
requirement on `github.com/varveio/swath`. The listings, totals and dataset rows a scenario expects come from
how it builds its input (a file tree, a key list served by `swath-replay`, a dataset written by 0.4.0), and
datasets are read by DuckDB, not by swath. A few checks compare swath with itself on purpose, where agreement
is the behaviour under test: the command aliases (`swath version` and `-V` print what `--version` prints, bare `swath`
what `-h` prints, `swath help list` what `list --help` prints) and the resumed run, whose parts must equal those of a run never stopped.
That a resume lists after the last saved key is witnessed by the replay server, which logs every request it answers:
no request of the resume without a continuation token starts below the last key of the parts the stopped run left in
`data/`, which DuckDB reads before the resume. A request with a token is not checked (the log does not say where the
token points), and the log cannot say which spilled ranges a resume read back rather than listed again.

## Changing it

Any change under `acceptance/`, an addition as much as an edit or a deletion, needs the owner's say and the
`acceptance-change` label, which only the owner applies. A pull request that changes it lists each scenario it
changes with the reason. The suite exists to notice when swath's behaviour changes; editing it in the same
change that changes swath defeats that.

## Running it

```bash
cd acceptance
SWATH_BIN=/abs/path/swath SWATH_REPLAY_BIN=/abs/path/swath-replay SWATH_EXPECT=current \
  GOWORK=off go test -count=1 ./...
```

Each scenario is a subtest named after its file: `-run 'TestScenarios/signals$'` runs one. CI runs every scenario
twice for a commit, with the v0.4.0 `swath-replay` and then with the commit's own.

| Input | What it is |
|---|---|
| `SWATH_BIN` | absolute path to the `swath` executable under test |
| `SWATH_REPLAY_BIN` | absolute path to the `swath-replay` executable that serves the object-store scenarios |
| `SWATH_EXPECT` | `v0.4.0` or `current`: which side of a changed behaviour to expect (below) |
| `duckdb` on `PATH` | the DuckDB CLI, which reads the datasets |

A missing or wrong input fails the run with a message naming it; nothing is skipped. The binaries must answer
`--version` with `swath ` and `swath-replay ` respectively. They are copied under their own names (`.exe` on
Windows) into a directory the scripts find first on `PATH`.

The run needs no network and no credentials: object stores are `swath-replay` on `127.0.0.1`. It must not run
as root (`local-permissions` fails if it does).

## Scenarios

Each file in `testdata/` is a [testscript](https://pkg.go.dev/github.com/rogpeppe/go-internal/testscript)
script: commands as a user types them, then the files they use.

| Scenario | What it checks |
|---|---|
| `interface` | the version line, the commands, the options the other scenarios use, and the exit code and message of each kind of mistake |
| `local` | a tree with hostile names (spaces, non-ASCII, byte-order traps, an empty directory, a hard link, a symbolic link, a non-UTF-8 name on Linux, a path over 260 characters) as text, totals and a dataset |
| `local-permissions` | an unreadable directory: exit 3, the entries left out, the dataset's coverage (not on Windows) |
| `s3-replay`, `gcs-replay`, `azure-replay` | a key list served over each provider's API: the listing equals it exactly and in byte order, filters, columns, formats, totals, the dataset's schema, rows, manifest and report |
| `deadline-resume` | a run stopped by `--max-duration` (exit 124) after committing parts, then resumed, gives the same parts byte for byte as a run never stopped, listing after the last saved key; a new run over the interrupted one is refused before it sends a request |
| `resume-kill` | a run killed outright (SIGKILL; TerminateProcess on Windows) once a part is in `data/`: `swath show`, then `swath resume` gives the same parts as a run never stopped, listing after the last saved key |
| `signals` | SIGINT, SIGTERM and SIGHUP (Unix): exit by the signal, `error.json` with 130, 143 or 129, then a resume as in `resume-kill` |
| `resume-spill` | a resume reads back the stopped run's sealed durable spill (only its report's `replayed_rows` says so), lists after the last saved key, and gives the same parts as a run never stopped; the server's ceiling makes the stop land before the listing could end |
| `early-close` | a reader that closes the pipe (`\| head`): exit 0, nothing on stderr, the report's `downstream_closed`, and the server never asked for the last page |
| `prefix` | `s3://`, `gs://` and `az://` sources naming a prefix list the keys that start with that string (a trailing slash makes it a directory's), with nothing on stderr; a local directory with and without a trailing separator, a file and a symbolic link as SOURCE |
| `local-options` | `--metadata names`, `stat`, `statx` (Linux), `--no-link-target`, `--max-prefixes`, `--max-inodes`, `--allocated` and `--work-dir` on a local tree |
| `s3-owner` | `--s3-fetch-owner` fills the owner columns from the server and leaves the others as listed, and their absence leaves them null (the replay server serves no versioned listing and no Azure metadata, so `--all-versions` and `--azure-fetch-*` are not checked) |
| `errors` | the first errors a new user meets offline: `show`, `resume` and `du --from-dataset` on what is not a dataset, a missing source, `--report` into a missing directory, an output into an unwritable one: each exit code, its one message and nothing on stdout |
| `compat-v040` | a dataset written by 0.4.0 (`testdata/compat-v040/`, provenance in the script) is read by `swath show`, `swath du --from-dataset` and `--seed-from`, and replaced by `--overwrite` |

Commands beside testscript's own:

| Command | Does |
|---|---|
| `exits N PROGRAM ARGS...` | runs PROGRAM and requires exit status N (`! exec` only says it failed) |
| `mktree DIR SPEC` | builds a tree from SPEC (`dir PATH MTIME`, `file PATH SIZE MTIME`, `link PATH TARGET`): files mode 0644, directories 0755, modes and times set last |
| `seq FILE N FORMAT` | appends N lines `fmt.Sprintf(FORMAT, i)` to FILE, for key lists and expected rows too long to write out |
| `replay FLAGS...` | starts `swath-replay serve --port 0 FLAGS` with its stderr written straight into a file, waits up to 30 s for its startup line, sets `$REPLAY` to its endpoint, and kills it when the script ends |
| `replaylog ENDPOINT FILE` | writes to FILE the whole lines the server at ENDPOINT logged since it started or since the previous `replaylog` of it; with `--slow-request-log-ms 0` the server logs every listing request before answering it |
| `signal SIG PATH WANT PROGRAM ARGS...` | runs PROGRAM as `exec` does, waits up to 60 s for PATH to exist, sends SIG (`INT`, `TERM`, `HUP` or `KILL`), and requires PROGRAM to end within 60 s as WANT says: an exit status, or `killed` (ended by that signal; Unix) |
| `head N WANT PROGRAM ARGS...` | runs PROGRAM as `exec` does with stdout on a pipe, reads N lines within 60 s and closes the pipe (`\| head -n N`), and requires PROGRAM to end with exit status WANT within 60 s; the N lines are its stdout. A PROGRAM still running when a wait runs out is killed |
| `dataset DIR SQL` | checks each part `DIR/manifest.json` names (bytes, SHA-256, rows, no other part in `data/`, every part with the first part's Parquet schema), then runs SQL in DuckDB over table `ds` (the parts' rows in stored order) and `ds_schema` (the parts' Parquet columns), printing rows tab-separated with LF line ends |
| `json FILE PATH...` | prints the JSON value at each dotted PATH; `! json FILE PATH` requires PATH absent |
| `cmpform FILE WANT` | compares FILE (or `stdout`, `stderr`) with WANT line by line, `$VAR` expanded, every character exactly except the placeholders `{int}` (an integer, zero too), `{pos}` (a positive integer), `{time}` (a UTC timestamp in the years 2000 to 2099) and `{path}` (a temporary path) |
| `digests DIR` | prints every directory and file below DIR, each file with its size and SHA-256, to show that a command changed nothing; fails if DIR is not a directory, so a file in place of an empty directory does not print nothing too |

Every program the scripts start gets the default disposition of SIGINT, SIGTERM and SIGHUP, even when the suite
itself runs with one ignored (under `nohup`, SIGHUP), which a program would otherwise inherit.

`signal` and `head` run PROGRAM with the variables the script started with, as `env` has changed them; a variable
the script adds with `env` does not reach PROGRAM.

Conditions beside testscript's own: `[expect:v0.4.0]` and `[expect:current]`, true when `SWATH_EXPECT` names
them. Beside testscript's variables, `$TESTDATA` is this directory, `$UID` the user the suite runs as (not on
Windows), and `TZ` is `UTC`.

## Expectations by version

A line without an `[expect:…]` condition must hold for every build. Where `main` changed behaviour on purpose
after 0.4.0, the script keeps both expectations next to each other, the 0.4.0 one under `[expect:v0.4.0]` and the
current one under `[expect:current]` with a comment naming the pull request that changed it. The pipeline says
which side to check through `SWATH_EXPECT`: `v0.4.0` for the published 0.4.0 binaries, `current` for a build of
`main`. Nothing parses `swath --version`. When the next release ships, it becomes the baseline: its side stays,
unmarked, and the 0.4.0 side is deleted.

Platform differences are conditions too (`[windows]`, `[darwin]`, `[linux]`, `[unix]`, `[symlink]`), each with its reason
and the exact value on each side.

Values that differ from run to run or machine to machine are matched by their form, by name: times a run takes
or records, the build revision, temporary paths, directories' own sizes, link counts and access times, allocated
bytes, inode, device and group numbers, change and birth times, and the local time zone's name on Windows.
Where the filesystem guarantees a positive value (on Unix, a directory's own size, a non-empty file's allocated
bytes, an allocation total over one), the form is `{pos}`, so a zero there fails.

## What it relies on in DuckDB

`read_parquet` with `file_row_number`, `parquet_schema` (`file_name`, `name`, `type`, `repetition_type`,
`converted_type`, `num_children`, its other columns compared as a whole), `list` over rows, `IS DISTINCT FROM`,
`DESCRIBE`, `SELECT * REPLACE`, `decode`, `hex`, `replace`, `chr`, `epoch_us`, `regexp_matches`, `error` and
`printf`, and the CLI
flags `-no-init -batch -bail -list -noheader -nullvalue -separator`. Rows come back in the order the table was
built in, the manifest's part order and each part's row order.

The resume scenarios also run the CLI themselves (`exec duckdb`, their `saved.sql` and `witness.sql` on stdin,
`-cmd 'SET VARIABLE run = ...'`), and rely on `getvariable`, `read_parquet` of a glob, `COPY ... TO` a Parquet file,
`read_text`, `string_split`, `unnest`, `regexp_extract`, `nullif`, `coalesce`, `greatest` and string comparison in
byte order.
