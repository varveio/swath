# The Go library

swath's listing code is also a Go library: a program can open an S3 bucket and ask it, during its own run, for the
next children of a prefix (a **page**), for what is at exactly one key (a **lookup**), for everything under a
prefix (a **walk**) or for the totals under several prefixes (a **summary**), with no swath process and no output
files. A `Source` links neither swath's command, its flags nor its Parquet writer. A program can also run
the whole command line ([Run the command line](#run-the-command-line)).

The library is **experimental**: it is implemented and tested, but its API may change in any release.

| Package | What it holds |
|---|---|
| `github.com/varveio/swath` | `Source`, an opened live location, and its `Page`, `Lookup`, `Walk`, `Summarize` and `Close` |
| `github.com/varveio/swath/s3` | `Open`, which opens a bucket of Amazon S3 or of an S3-compatible store as a `Source` |
| `github.com/varveio/swath/reader/listing` | the requests, results, entries, evidence and error classes ([the listing contract](../reader/README.md#the-listing-contract)) |
| `github.com/varveio/swath/command` | `Main`, the whole swath command line as a function ([Run the command line](#run-the-command-line)) |

Pages, lookups, walks and summaries answer from the bucket as it is during the call. To ask the same questions of a saved inventory
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

## Walks

A walk hands a function of yours every object under a prefix, each once, in no order: the object at exactly the
prefix included, and the prefix taken literally, as for a page. It lists the prefix flat, one request per thousand
objects however deep their keys, and in parallel: workers list key ranges at once, and a range splits at a page
boundary while workers are idle, so a prefix that fits in one page is one request and only a larger one spreads.
`WalkRequest.Workers` says how many (64 when zero; 1 lists one continuation chain, never split). Each walk keeps its
own ranges; walks of one `Source` at once share its client and its request limits, nothing else.

Your function is called from the goroutine that called `Walk`, one batch at a time. A batch belongs to the walk
until the function returns: copy what you keep. A worker whose batch the function has not taken waits, so a slow
function slows the walk down, and an error from it stops the walk: `Walk` returns that error once the workers have
stopped, and calls the function no more. Every entry is checked before it reaches the function: inside the prefix,
no NUL byte in its key, and a size (an object S3 lists without one is `listing.ErrInvalidPage`, never a size of
zero). The ranges are checked too: no key range overlaps another, so no key reaches the function twice.

A budget bounds a walk: `MaxEntries` stops it after the batch that reaches that many entries, `MaxDuration` once
that time has passed, the function's own time included. A walk a budget stops returns no error: its
`Evidence.Stop` names the bound, and its last batches hand the function, as `Unread` extents of class `budget`,
the key intervals `(Key, Hi]` it did not read (an empty `Key` from the prefix's start, an empty `Hi` to its end).
What the walk handed over and those intervals hold every key under the prefix once, so walking the intervals
afterwards completes it. A walk whose context ends returns the context's error, even if the function took every
batch.

A walk with a depth (`MaxDepth`) reads one level of `/` at a time instead, one request at a time whatever
`Workers` says: the objects within the depth, and each folder at the depth as a `KindPrefix` entry, which counts
toward no budget and stands for the keys below it. A walk with a depth shows nothing below it. A budget stops it
between requests; the level it was reading comes back as an `Unread` extent of scope `listing` at the level's
prefix (the children it handed over are read), and each level it had not begun as a `subtree`.

```go
var objects, size int64
var unread []listing.Unread
req := listing.WalkRequest{Prefix: []byte("logs/"), Budget: listing.Budget{MaxDuration: time.Minute}}
res, err := src.Walk(ctx, req, func(b listing.WalkBatch) error {
	for _, e := range b.Entries {
		objects++
		size += *e.Size
	}
	for _, u := range b.Unread {
		unread = append(unread, listing.Unread{Key: bytes.Clone(u.Key), Hi: bytes.Clone(u.Hi), Scope: u.Scope, Class: u.Class})
	}
	return nil
})
if err != nil {
	log.Fatal(err)
}
fmt.Printf("%d objects, %d bytes\n", objects, size)
if res.Evidence.Stop != listing.StopCompleted {
	fmt.Printf("stopped at the %s: %d key intervals not read\n", res.Evidence.Stop, len(unread))
}
```

## Summaries

A summary answers, for each of several targets, how many objects lie at or under it, their bytes and their newest
last-modified time, and what is at exactly its key. A target is a subtree (`Subtree`: every key that begins with
`Key`, which the contract also calls a prefix) or one exact key. The subtrees are walked: one walk for each that lies
under no other, whose objects count toward it and toward every target nested in it, so nested targets are each
counted in full and listed once. A subtree target is present when an object has exactly its key, absent when the
walk read where it would be and found none, and unknown at the root (the empty key), or when its walk dropped
extents it did not read (below). An exact target is a lookup.

The budget bounds the whole call, every walk taking what is left of it. A target a walk stopped inside counts what
was read and carries, as its `Unread`, the key intervals inside it that were not: its totals are a lower bound. A
target the call never reached is unknown, with `listing.ErrBudgetExceeded` as its reason, counts nothing and is
unread whole. A total with an `Unread` is never a complete one.

```go
targets := []listing.Target{
	{Key: []byte("logs/"), Subtree: true},
	{Key: []byte("logs/2026/"), Subtree: true}, // nested: counted in full, in the same walk
	{Key: []byte("README")},                    // exact: a lookup
}
sums, ev, err := src.Summarize(ctx, targets, listing.SummaryOptions{Budget: listing.Budget{MaxDuration: time.Minute}})
if err != nil {
	log.Fatal(err)
}
for i, s := range sums {
	fmt.Printf("%s: %s, %d objects", targets[i].Key, s.State, s.Entries)
	if s.Bytes != nil {
		fmt.Printf(", %d bytes", *s.Bytes)
	}
	if len(s.Unread) > 0 {
		fmt.Print(", at least: not all of it was read")
	}
	fmt.Println()
}
fmt.Println("the call:", ev.Stop)
```

Both programs are the s3 package's examples ([`Example_walk`, `Example_summarize`](../s3/example_test.go)). A walk
and a summary need `s3:ListBucket` on the bucket; a summary's exact targets need what a lookup does.

## Evidence and errors

Every answer carries its evidence: it is live, and was read between the call's start and finish, which it gives.
A walk's and a summary's also say whether the call completed or which bound stopped it (`Evidence.Stop`), and
locate every gap as an `Unread` extent; a walk that met more of them than it can hold drops the rest and says so
with a nil `Evidence.Unlocated`.

A call that fails returns an error that `errors.Is` matches to one of the contract's classes, read from the failure
as swath's command reads it for its outcome: `ErrAuth` for credentials that do not load or a refusal,
`ErrNotFound` for a bucket that is not there or is in another region, `ErrThrottled` and `ErrUnavailable` once the
client's retries are spent, `ErrInvalidPage`, `ErrInvalidToken`, `ErrInvalidRequest` and `ErrUnsupported`.
`listing.Retryable` says whether asking again later can succeed. A call whose context ends returns the context's
error. Error messages pass through swath's redaction and carry no credential.

To count or trace the requests a call sends, put an `objfile.Observer` on its context (`objfile.WithObserver`), as
for dataset reads: every attempt, retries included, is reported to it, a page's and each of a walk's as a list
request and a lookup's HeadObject and HeadBucket as metadata requests.

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

The library answers Page, Lookup, Walk and Summarize on S3. The GCS, Azure and local sources are not in it yet. A source's request limits hold within one process only.

## Google Cloud Storage

`github.com/varveio/swath/gcs` opens a Google Cloud Storage bucket as a `Source`, with the same pages, lookups,
tokens, evidence, error classes and observer as S3 (the package's [`Example`](../gcs/example_test.go)):

```go
src, err := gcs.Open(ctx, "example-bucket", gcs.Options{})
```

`gcs.Options` takes a JSON API endpoint (an emulator's or a proxy's), a credentials file, anonymous access to a
public bucket, the project billed for a requester-pays bucket, and the same two request limits as S3. Credentials
otherwise come from Application Default Credentials, as for the command
([Authentication and permissions](sources/gcs.md#authentication-and-permissions)). What differs from S3:

- A page is one `objects.list` with the delimiter `/`. GCS may answer with a page that holds nothing and still
  carries a token: the level goes on. A zero-length object whose name ends in `/`, such as a folder placeholder,
  is an object like any other: in a page of its own prefix it is the `Self`.
- A lookup reads the object's metadata (`objects.get`). GCS answers a missing object and a missing bucket with the
  same 404 with reason `notFound`, so after one the lookup reads the bucket's (`buckets.get`), and the key is absent
  only when that succeeds. A 403 to either, a 404 without that reason, a missing bucket (`listing.ErrNotFound`) and
  a probe that fails leave the key unknown.
- A page needs `storage.objects.list` on the bucket. A lookup needs `storage.objects.get`, and
  `storage.buckets.get` to show a key absent: without it a missing key is unknown (`listing.ErrAuth`).
- A token is bound to the bucket and the endpoint it is read through; the billed project is not part of it.
- The observer sees a lookup's `objects.get` and `buckets.get` as metadata requests, the bucket's with an empty
  name.
