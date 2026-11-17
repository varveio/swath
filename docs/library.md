# The Go library

swath's listing code is also a Go library: a program can open an S3 bucket and ask it, during its own run, for the
next children of a prefix (a **page**) or for what is at exactly one key (a **lookup**), with no swath process and
no output files. A `Source` links neither swath's command, its flags nor its Parquet writer. A program can also run
the whole command line ([Run the command line](#run-the-command-line)).

The library is **experimental**: it is implemented and tested, but its API may change in any release.

| Package | What it holds |
|---|---|
| `github.com/varveio/swath` | `Source`, an opened live location, and its `Page`, `Lookup` and `Close` |
| `github.com/varveio/swath/s3` | `Open`, which opens a bucket of Amazon S3 or of an S3-compatible store as a `Source` |
| `github.com/varveio/swath/reader/listing` | the requests, results, entries, evidence and error classes ([the listing contract](../reader/README.md#the-listing-contract)) |
| `github.com/varveio/swath/command` | `Main`, the whole swath command line as a function ([Run the command line](#run-the-command-line)) |

Pages and lookups answer from the bucket as it is during the call. To ask the same questions of a saved inventory
instead, open it with the `reader/view` package ([the inventory view](../reader/README.md#the-inventory-view)).

## Open, page, look up, close

```go
ctx := context.Background()
src, err := s3.Open(ctx, "example-bucket", s3.Options{Region: "eu-west-1"})
if err != nil {
	log.Fatal(err)
}
defer src.Close()

req := listing.PageRequest{Prefix: []byte("logs/"), Limit: 100}
for {
	page, err := src.Page(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	if page.Self != nil {
		fmt.Printf("an object at exactly %s\n", page.Self.Key)
	}
	for _, e := range page.Entries {
		fmt.Println(e.Kind, string(e.Key))
	}
	if page.Next == "" {
		break
	}
	req.Token = page.Next
}

r, err := src.Lookup(ctx, []byte("logs/2026-11-17.gz"))
if err != nil {
	log.Fatal(err)
}
switch {
case r.State == listing.LookupPresent:
	fmt.Println("present, read at", r.Evidence.Finished)
case r.State == listing.LookupAbsent:
	fmt.Println("absent")
case errors.Is(r.Reason, listing.ErrAuth):
	fmt.Println("unknown: not authorized to tell")
default:
	fmt.Println("unknown:", r.Reason)
}
```

The same program is the package's [`Example`](../s3/example_test.go), which the tests compile.

`Open` builds the bucket's client and may load credentials; it lists nothing, so a bucket that is not there fails
the first call. `s3.Options` takes the region, an S3-compatible endpoint and path-style addressing, a profile, a
credentials file, unsigned access to a public bucket, and two limits on the requests the source sends: how many at
once (an adaptive limit that backs off while the service throttles) and how many per second. Credentials otherwise
come from the environment, as for the command ([Authentication and permissions](sources/s3.md#authentication-and-permissions)).
A `Source` is safe for concurrent use and keeps its client and limits for as long as the program keeps it.

## Pages

A page is one delimiter listing of one level, by `/` only: one request, the client's retries included, and no
request per child. Its entries come in byte order, so a prefix `a/` sorts after an object `a.txt`, and every page
continues after the last key of the one before. A child prefix is an entry of kind `prefix`; it says nothing about an
object whose key is exactly that prefix. Such an object comes back once, as the `Self` of the page that reads it,
never as a child. A prefix is taken literally: `a` holds `a.txt`, `a0` and the prefix `a/`.

A page may be short, or empty, and still carry a token: only an empty `Next` ends the level. The token is opaque and
bound to the bucket, the endpoint and region it is read through, and the prefix; given with another, it is refused
before any request. Every page is checked before it is returned: a key outside the prefix, below one of its
children or out of order, and a continuation token from the store that repeats the one sent, are
`listing.ErrInvalidPage`. A live page is a traversal, not a snapshot: a key added or removed while a level is paged
may or may not be in it.

## Lookups

A lookup has three answers: present, with the entry; absent; or unknown, with the reason. Absence is never the
fallback. On S3 a key is absent only when HeadObject finds no object and HeadBucket then shows the bucket is there.
Without list permission S3 answers a missing key with 403, not 404, so a 403 leaves the key unknown
(`listing.ErrAuth`), as do a missing bucket (`listing.ErrNotFound`) and a probe that fails.

A page needs `s3:ListBucket` on the bucket. A lookup needs `s3:GetObject` on the key, and `s3:ListBucket` to show a key
absent.

## Evidence and errors

Every answer carries its evidence: it is live, and was read between the call's start and finish, which it gives.

A call that fails returns an error that `errors.Is` matches to one of the contract's classes, read from the failure
as swath's command reads it for its outcome: `ErrAuth` for credentials that do not load or a refusal,
`ErrNotFound` for a bucket that is not there or is in another region, `ErrThrottled` and `ErrUnavailable` once the
client's retries are spent, `ErrInvalidPage`, `ErrInvalidToken`, `ErrInvalidRequest` and `ErrUnsupported`.
`listing.Retryable` says whether asking again later can succeed. A call whose context ends returns the context's
error. Error messages pass through swath's redaction and carry no credential.

To count or trace the requests a call sends, put an `objfile.Observer` on its context (`objfile.WithObserver`), as
for dataset reads: every attempt, retries included, is reported to it, a page's as a list request and a lookup's
HeadObject and HeadBucket as metadata requests.

## Run the command line

A program that needs a whole run, with its flags, output, progress files and exit codes, rather than one page or
lookup, can run the swath command line itself. `command.Main(ctx, args, stdin, stdout, stderr)` runs one command
line (`args` without the program name) and returns its exit code; the swath binary is
`command.Exit(command.Main(...))` and nothing else. This package links the whole command, its providers and its
Parquet writer, which `Source` does not.

```go
command.Host = "example-host"
command.Exit(command.Main(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
```

`Main` is for a process dedicated to the call, such as a child the host starts for one command line. It sets the
command's process-wide policy when it is called, never when the package is imported, and does not undo it when it
returns: the Azure SDK's own log is turned off, SIGINT, SIGTERM and SIGHUP stop the run, SIGPIPE is ignored, and a
run that has not finished stopping within the shutdown grace ends the process. Terminal detection (`--format auto`,
color, the live progress line) follows the `stdout` and `stderr` given to `Main`: a writer that is not an `*os.File`
is not a terminal. `command.Exit` ends the process as the binary does: on Unix, a run a signal stopped dies by that
signal.

`command.Host` names the program this copy is built into, and `--version` then says so. The version that
`--version`, the run report, the journal, Parquet `created_by` and the User-Agent give is swath's own: the version
of swath the host requires, or `devel` for a local replacement or a workspace module, with no revision.

## Not in the library yet

The library answers Page and Lookup on S3. Walk and Summarize, which list a whole prefix, and the GCS, Azure and
local sources are not in it yet. A source's request limits hold within one process only.

## A local directory

`github.com/varveio/swath/local` opens a directory as a `Source`, on Linux and macOS. The program opens the
directory itself, without following a symbolic link, and hands swath the open file: swath never resolves the
directory by its path, so what it reads is the directory the program opened, even once that path names
something else.

```go
ctx := context.Background()
root, err := os.OpenFile("/srv/data", os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
if err != nil {
	log.Fatal(err)
}
defer root.Close() // after the source's Close
src, err := local.Open(root, local.Options{})
if err != nil {
	log.Fatal(err)
}
defer src.Close()

req := listing.PageRequest{Prefix: []byte("logs/"), Limit: 100}
for {
	page, err := src.Page(ctx, req)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range page.Entries {
		fmt.Println(e.Kind, string(e.Key))
	}
	if page.Next == "" {
		break
	}
	req.Token = page.Next
}
```

The same program is the package's [`Example`](../local/example_test.go), which the tests compile.

The program owns the directory: it keeps it open until the source's `Close` returns, and `Close` does not close
it. Keys are relative to it. A page's prefix is `""` for the directory itself or a directory's key, such as
`logs/2026/`, which ends in `/`; a lookup's key names an entry of any kind, `logs/a.gz` or `logs`. What the
library guarantees:

- **Descriptor-relative, never followed.** Each page and lookup opens what it reads relative to the program's
  directory, one name at a time, never following a symbolic link, and checks each directory it opens against
  the device of the program's directory. A symbolic link is listed as an entry of kind `symlink`. A call holds
  at most two descriptors at once and none once it returns; a source keeps nothing open between calls and is
  safe for concurrent use.
- **A page is one directory level, in byte order of keys.** A subdirectory is an entry of kind `prefix`
  (`logs/2026/`); any other entry has its kind (`file`, `symlink`, `other` for a FIFO, socket or device, or
  `unknown` when its stat fails) and what `lstat` gives of it: size, space on disk, modification time,
  device, inode and link count. The prefix's own directory comes back as the `Self` of the level's first page,
  and counts toward its `Limit`. A directory on another device is listed as a prefix like any other, and a page
  of it fails with `listing.ErrOtherDevice`.
- **Pages continue by key.** A token resumes the level after the last key it read, so a directory that changes
  between pages repeats no key, keeps byte order across pages and skips no name that stayed; a name added
  after the last key read comes in a later page, one removed does not. It is bound to the directory's device
  and inode and to the prefix. A page reads its whole directory and sorts it to choose its entries:
  `Options.MaxHeldBytes` (64 MiB by default, a name counting its length plus 64 bytes) bounds the names it
  holds, and a directory past it fails the page with `local.ErrTooWide`, which also matches
  `listing.ErrUnsupported`. Each page reads the directory again; the request's `MaxDuration` bounds that work,
  its `MaxScanned` does not yet.
- **Failures in their class.** A prefix that is not there is `listing.ErrNotFound`; one naming something other
  than a directory, a symbolic link included, `listing.ErrNotDirectory`; one with a symbolic link on the way,
  which is not followed, `listing.ErrUnsupported`: not reached, never absent. A refusal is `listing.ErrAuth`, a
  name too long for the system `listing.ErrUnsupported`, an I/O error `listing.ErrUnavailable`. A message
  names where a call failed by its depth below the directory and by the program's own key, never by more of a
  path.
- **A lookup is absent only when shown.** `ENOENT` or `ENOTDIR` at the key, or at a directory on the way that is
  no symbolic link, is absent; a symbolic link at the key is present, as itself; a key naming a directory
  answers with its directory key (`logs` with `logs/`). A symbolic link or another device on the way, a refusal,
  a name too long and an I/O error leave the key unknown: it was not reached.
- **Names are bytes, keys UTF-8 for now.** A `Source` refuses a key that is not valid UTF-8, so a page of a
  directory holding such a name fails with `listing.ErrInvalidPage`, and such an entry cannot be looked up.

To count or trace a call's work, an `objfile.Observer` on its context is told of each page as one list request
and each lookup as one metadata request, named by the program's key.
