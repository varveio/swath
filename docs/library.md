# The Go library

swath's listing code is also a Go library: a program can open an S3 or GCS bucket, an Azure container or a local
directory and ask it, during its own run, for the next children of a prefix (a **page**), for what is at exactly
one key (a **lookup**), for everything under a prefix (a **walk**) or for the totals under several prefixes (a
**summary**), with no swath process and no output files. A `Source` links neither swath's command, its flags nor its Parquet writer. A program can also run
the whole command line ([Run the command line](#run-the-command-line)).

The library is **experimental**: it is implemented and tested, but its API may change in any release.

| Package | What it holds |
|---|---|
| `github.com/varveio/swath` | `Source`, an opened live location, and its `Page`, `Lookup`, `Walk`, `Summarize` and `Close` |
| `github.com/varveio/swath/s3` | `Open`, which opens a bucket of Amazon S3 or of an S3-compatible store as a `Source` |
| `github.com/varveio/swath/gcs` | `Open`, which opens a Google Cloud Storage bucket as a `Source` ([Google Cloud Storage](#google-cloud-storage)) |
| `github.com/varveio/swath/azblob` | `Open`, which opens an Azure Blob Storage container as a `Source` ([Azure Blob Storage](#azure-blob-storage)) |
| `github.com/varveio/swath/local` | `Open`, which opens a directory the program opened as a `Source` ([A local directory](#a-local-directory)) |
| `github.com/varveio/swath/reader/listing` | the requests, results, entries, evidence and error classes ([the listing contract](../reader/README.md#the-listing-contract)) |
| `github.com/varveio/swath/command` | `Main`, the whole swath command line as a function ([Run the command line](#run-the-command-line)) |

Pages, lookups, walks and summaries answer from the location as it is during the call. To ask the same questions of a saved inventory
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
never as a child. A prefix is taken literally: `a` holds `a.txt`, `a0` and the prefix `a/`, so ask for `a/` to list
the folder `a`. A prefix exists only through the keys that begin with it: one with none pages empty, never
`listing.ErrNotFound`. `Limit` bounds the children; `Self` comes beside them, though the page that holds it may hold
one child fewer, since the store's own page size counts it.

A page may be short, or empty, and still carry a token: only an empty `Next` ends the level. The token is opaque and
bound to the bucket, the endpoint and region it is read through, and the prefix; given with another, it is refused
before any request. Every page is checked before it is returned: a key outside the prefix, below one of its
children or out of order, and a continuation token from the store that repeats the one sent, are
`listing.ErrInvalidPage`. A live page is a traversal, not a snapshot: a key added or removed while a level is paged
may or may not be in it. Totals for each child folder (`ChildTotals`) come from an inventory only: a live page that
asks for them is `listing.ErrUnsupported`, before any request.

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

## Azure Blob Storage

`azblob.Open` takes the account and the container as `ACCOUNT/CONTAINER`:

```go
src, err := azblob.Open(ctx, "exampleaccount/logs", azblob.Options{})
if err != nil {
	log.Fatal(err)
}
defer src.Close()
```

The rest is as on S3: the same `Page`, `Lookup`, `Walk`, `Summarize` and `Close`, checks, tokens, evidence and error classes. The
package's [`Example`](../azblob/example_test.go) is the whole program. `azblob.Options` takes an endpoint that
replaces `https://ACCOUNT.blob.core.windows.net` (Azurite, say), unsigned access to a public container and the same two
request limits; credentials otherwise come from the environment, as for the command: `AZURE_STORAGE_SAS_TOKEN`, else
`AZURE_STORAGE_KEY`, else Entra ID ([Authentication and permissions](sources/azure.md#authentication-and-permissions)).
`Open` sends nothing.

What differs from S3:

- **Order.** Azure lists a level in UTF-16 order, not byte order: names at U+10000 and above (emoji, say) come before
  those in U+E000..U+FFFF (fullwidth forms). An account with a hierarchical namespace lists in that namespace's own
  order, which puts `a/` before `a-b`. A level that one answer of Azure holds (up to 5,000 entries) is sorted into
  byte order, and while a level is sorted each of its pages reads it whole again. A longer level is paged in Azure's
  order and checked, and is `listing.ErrUnsupported` from the page where it shows a key out of byte order: sorting each
  page cannot make byte order across pages.
- **Hierarchical namespaces.** Unlike the command, the library lists them. Azure lists each directory as a prefix and
  as a blob marked `hdi_isfolder=true`; a page has it once, as the prefix, and a lookup of the directory's name answers
  present with the prefix's entry. A level longer than one answer that holds a directory is `listing.ErrUnsupported`.
  User metadata can mark a flat namespace's blob the same way, so the first marked blob makes the source ask the
  account's namespace, as the command's admission does (Get Account Information, then the Data Lake host), once:
  in a flat namespace a marked blob is an object like any other, and where the namespace cannot be read (an
  anonymous caller on an endpoint other than Azure's own, say) a page holding one is `listing.ErrUnsupported` and a
  lookup of one unknown.
- **Lookups.** A key is absent only when Get Blob Properties finds no blob and a listing of the container then
  succeeds; a 403, a missing container (`listing.ErrNotFound`) and a failed listing leave it unknown. A page needs
  permission to list the container's blobs; a lookup needs permission to read the blob's properties, and to list to show
  a key absent. A request is reported to an `objfile.Observer` as a list request (List Blobs, the probe included) or a
  metadata request (Get Blob Properties, and the namespace's calls, unnamed).
- **Walks.** A walk without a depth lists a flat namespace only, as the command does: each walk first asks the
  account's namespace with the same calls, and a hierarchical namespace, or one they cannot read, is
  `listing.ErrUnsupported` before any listing. Its ranges are listed in byte order whatever order Azure lists in.

## Google Cloud Storage

`github.com/varveio/swath/gcs` opens a Google Cloud Storage bucket as a `Source`, with the same pages, lookups,
walks, summaries, tokens, evidence, error classes and observer as S3 (the package's [`Example`](../gcs/example_test.go)):

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
- A walk's ranges are `objects.list` bounded by `startOffset` and `endOffset`. The client reads an object listed
  without a size as size zero, as a page does: a walk cannot refuse it as on S3.
- A page and a walk need `storage.objects.list` on the bucket. A lookup needs `storage.objects.get`, and
  `storage.buckets.get` to show a key absent: without it a missing key is unknown (`listing.ErrAuth`).
- A token is bound to the bucket and the endpoint it is read through; the billed project is not part of it.
- The observer sees a lookup's `objects.get` and `buckets.get` as metadata requests, the bucket's with an empty
  name.

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
  beside its children: `Limit` bounds the children only. A directory on another device is listed as a prefix like any other, and a page
  of it fails with `listing.ErrOtherDevice`.
- **Pages continue by key.** A token resumes the level after the last key it read, so a directory that changes
  between pages repeats no key, keeps byte order across pages and skips no name that stayed; a name added
  after the last key read comes in a later page, one removed does not. It is bound to the directory's device
  and inode and to the prefix. A page reads its whole directory and sorts it to choose its entries:
  `Options.MaxHeldBytes` (64 MiB by default, a name counting its length plus 64 bytes) bounds the names it
  holds, and a directory past it fails the page with `local.ErrTooWide`, which also matches
  `listing.ErrUnsupported`. Each page reads the directory again; the request's `MaxDuration` bounds that work.
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

The library answers Page and Lookup on S3, GCS, Azure Blob Storage and a local directory, and Walk and Summarize on
the three object stores. On a local directory a walk, and a summary with a subtree target, are
`listing.ErrUnsupported`. A source's request limits hold within one process only.
