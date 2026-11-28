# The Go library

swath's listing code is also a Go library: a program can open an S3 or GCS bucket, an Azure container or a local
directory and ask it, during its own run, for the next children of a prefix (a **page**), for what is at exactly
one key (a **lookup**) or for everything under a prefix (a **walk**), with no swath process and no output files. A
`Source` links neither swath's command, its flags nor its Parquet writer. A program can also run
the whole command line ([Run the command line](#run-the-command-line)).

The library is **experimental**: it is implemented and tested, but its API may change in any release.

| Package | What it holds |
|---|---|
| `github.com/varveio/swath/source` | `Open`, which opens a `Source` from an address through the providers the program links; `Source`, an opened live location, and its `Page`, `Lookup`, `Walk` and `Close` |
| `github.com/varveio/swath/source/all` | nothing but imports: it links every provider `source.Open` takes an address of |
| `github.com/varveio/swath/source/s3` | `Open`, which opens a bucket of Amazon S3 or of an S3-compatible store as a `Source` |
| `github.com/varveio/swath/source/gcs` | `Open`, which opens a Google Cloud Storage bucket as a `Source` ([Google Cloud Storage](#google-cloud-storage)) |
| `github.com/varveio/swath/source/azblob` | `Open`, which opens an Azure Blob Storage container as a `Source` ([Azure Blob Storage](#azure-blob-storage)) |
| `github.com/varveio/swath/source/local` | `Open`, which opens a directory the program opened as a `Source` ([A local directory](#a-local-directory)) |
| `github.com/varveio/swath/command` | `Main`, the whole swath command line as a function ([Run the command line](#run-the-command-line)); `List`, one `swath list` run from typed options ([Run a listing from Go](#run-a-listing-from-go)) |
| `github.com/varveio/swath/reader/listing` | the requests, results, entries, evidence and error classes ([the listing contract](../reader/README.md#the-listing-contract)) |
| `github.com/varveio/swath/reader/du` | `New`, a `Tally` that sums a walk's entries per key prefix by du's rules ([Sum a walk as du does](#sum-a-walk-as-du-does)) |
| `github.com/varveio/swath/reader/du/tree` | `Select` and `Render`, which choose and draw the view `swath du` shows on a terminal, with columns of the program's own ([Draw the view as swath du does](#draw-the-view-as-swath-du-does)) |

Pages, lookups and walks answer from the location as it is during the call. To ask the same questions of a saved inventory
instead, or for the totals under several prefixes (a summary), open it with the `reader/view` package
([the inventory view](../reader/README.md#the-inventory-view)).

## Open by address

```go
import (
	"github.com/varveio/swath/source"
	_ "github.com/varveio/swath/source/s3" // links s3:// addresses
)

src, err := source.Open(ctx, "s3://example-bucket", source.Options{Region: "eu-west-1"})
```

`source.Open` takes `s3://BUCKET`, `gs://BUCKET` or `az://ACCOUNT/CONTAINER`, as the command does. An address names
a bucket or container only: a key prefix goes in each page, lookup or walk request, and an address with one is
`listing.ErrInvalidRequest`. `Open` opens the providers the program links, as `database/sql` opens its drivers:
import one provider's package for its side effect, several, or `github.com/varveio/swath/source/all` for every
one; a program links only the cloud SDKs of the providers it imports. An address whose provider is not linked is
`listing.ErrUnsupported`, and its message names the import to add. `source.Options` holds every provider's
settings, each field naming the providers that read it; the others ignore it.

A program that names its provider in code can call that package's own `Open` and `Options` instead, as below, and
the compiler checks them. A local directory has no address: `local.Open` takes the directory the program opened.

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

The same program is the package's [`Example`](../source/s3/example_test.go), which the tests compile.

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

A walk sets no limit of its own. To stop one, cancel its context: `Walk` then returns the context's error, even
if the function took every batch, together with a `WalkSummary` that counts what the function took. A walk
returns `listing.ErrInvalidPage` instead when entries a store handed on had already failed the walk's own check,
whether before or after the context ended. Any other failure of a store once the context has ended still returns
the context's error. A walk that returns no error finished.

A walk that cannot read a place goes on past it and hands the function an `Unread` for it as it meets it, in a
batch of its own: the place's key and a `Reason` that `errors.Is` matches to one of the error classes. Only a local
walk has them (a directory it cannot open or read, an entry it cannot stat, a directory on another device): on an
object store a listing error ends the walk, a page whose client retries are spent included (`ErrUnavailable`):
where swath's command asks such a page again, a walk does not, so a store that stops answering ends the walk
without a deadline of yours. The walk keeps none of them once the function has taken it, however
many there are; `WalkSummary.Unread` counts them. `WalkSummary.Complete` says the walk read everything under its
prefix: it finished and handed on no `Unread`, and, over an inventory, the inventory's coverage is complete.

```go
walkCtx, cancel := context.WithTimeout(ctx, time.Minute)
defer cancel()
var objects, size int64
res, err := src.Walk(walkCtx, listing.WalkRequest{Prefix: []byte("logs/")}, func(b listing.WalkBatch) error {
	for _, e := range b.Entries {
		objects++
		size += *e.Size
	}
	return nil
})
switch {
case errors.Is(err, context.DeadlineExceeded):
	fmt.Printf("not finished in a minute: at least %d objects, %d bytes\n", res.Entries, size)
case err != nil:
	log.Fatal(err)
default:
	fmt.Printf("%d objects, %d bytes\n", objects, size)
}
```

The program is the s3 package's [`Example_walk`](../source/s3/example_test.go). A walk needs `s3:ListBucket` on the
bucket.

## Sum a walk as du does

The package `github.com/varveio/swath/reader/du` sums a walk's entries per requested key prefix by du's rules:
space on disk with directories' own blocks, and a file with several hard links once within each prefix that holds
any of its names. It lists nothing and imports only `reader/listing`. It is experimental, and `swath du` itself
does not use it ([the du tally](../reader/README.md#the-du-tally) has its counting rules).

```go
tally, err := du.New(du.Options{Prefixes: []string{"", "projects/", "projects/a/"}})
if err != nil {
	return err
}
_, err = src.Walk(ctx, listing.WalkRequest{}, func(b listing.WalkBatch) error {
	return tally.Add(b.Entries)
})
results := tally.Result() // one du.Totals per prefix, in the order requested
```

For every prefix, `Allocated` is a lower bound of `du -sB1` of the prefix on an acyclic traversal scope, also
after a walk stopped early. The walk covered a prefix when it returned no error, an inventory's walk reported
`WalkSummary.Complete`, and no `Unread` holds the prefix or lies under it. Where it did and `AllocatedMissing` and
`Unidentified` are 0, `Allocated` equals `du -sB1` with three differences: a directory on another
device counts its own blocks and not its contents (GNU `du -s` enters it, `du -sx` leaves out the mount point's
own blocks); the prefix's own directory counts only when its entry is fed, and a walk of the root hands on none,
so feed a `KindDir` at `""` with the root's blocks; and a same-device bind mount of an ancestor is a cycle that
swath's walk enters and counts again, where GNU du skips it. Every bound and equality holds on acyclic traversal
scopes only, and on a tree that does not change during the walk.

## Draw the view as swath du does

The package `github.com/varveio/swath/reader/du/tree` draws [the view](totals.md#the-view) of `swath du` from
totals the program holds, live or from an inventory, and `swath du` draws its own with it. `Select` chooses the
rows, at most as many as asked for, from a `Tree` (the total and its prefixes and objects, each with its bytes,
objects, children and newest time); `Render` writes them for a width, with color or without, and with the
program's own columns: a heading, an alignment and one formatted cell per row, a `(N more)` row's included or left
empty. It imports only the standard library and `golang.org/x/text`'s width tables, and is experimental.

```go
rows := tree.Select(&tree.Tree{Total: total, Nodes: nodes}, 20)
cost := tree.Column{Heading: "cost/mo", Align: tree.Right}
for _, r := range rows {
	cost.Cells = append(cost.Cells, monthly(r.Bytes)) // a fold's cell: what it holds
}
err := tree.Render(os.Stdout, tree.Layout{Label: "s3://bucket", Width: width}, rows, cost)
```

As the width shrinks, the bar goes, then the share, then the date, then the program's columns from the last to
the first; the size, the count and the path stay, a path cut in the middle. The package's documentation says how
a `du.Totals` becomes a node and why the totals must add up (a hard-linked file with names under two prefixes
is counted under one of them), and its example draws a column.

## Evidence and errors

Every answer carries its evidence: it is live, and was read between the call's start and finish, which it gives.
A walk that did not finish and a lookup left unknown have a nil `Evidence.Unlocated`, so their evidence never reads
as complete. No call has a time limit of its own: bound any call with a context deadline, and it returns
`context.DeadlineExceeded`, but for the one exception of a live walk ([Walks](#walks)).

A call that fails returns an error that `errors.Is` matches to one of the contract's classes, read from the failure
as swath's command reads it for its outcome: `ErrAuth` for credentials that do not load or a refusal,
`ErrNotFound` for a bucket that is not there, `ErrInvalidRequest` for a request the contract refuses and for a
bucket that is there but in another region than the source's (the message names the bucket's region),
`ErrUnavailable` once the client's retries are spent (throttling included), `ErrInvalidPage`, `ErrInvalidToken` and
`ErrUnsupported`. A call whose context ends returns the context's error, but a live walk returns `ErrInvalidPage`
when entries a store handed on had already failed the walk's own check ([Walks](#walks)). A walk has two exceptions, which need not
match a class: an error your function returns, which `Walk` returns unchanged, and a panic in the walk's own
goroutines, which `Walk` recovers and returns as an error. A panic in your function is not recovered. Error messages
pass through swath's redaction and carry no credential.

To count or trace the requests a call sends, put an `objfile.Observer` on its context (`objfile.WithObserver`), as
for dataset reads: every attempt, retries included, is reported to it, a page's and each of a walk's as a list
request and a lookup's HeadObject and HeadBucket as metadata requests.

## Azure Blob Storage

`azblob.Open` takes the account and the container:

```go
src, err := azblob.Open(ctx, "exampleaccount", "logs", azblob.Options{})
if err != nil {
	log.Fatal(err)
}
defer src.Close()
```

The rest is as on S3: the same `Page`, `Lookup`, `Walk` and `Close`, checks, tokens, evidence and error classes. The
package's [`Example`](../source/azblob/example_test.go) is the whole program. `azblob.Options` takes an endpoint that
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
- **Walks.** A walk lists a flat namespace only, as the command does: each walk first asks the
  account's namespace with the same calls, and a hierarchical namespace, or one they cannot read, is
  `listing.ErrUnsupported` before any listing. Its ranges are listed in byte order whatever order Azure lists in.

## Google Cloud Storage

`github.com/varveio/swath/source/gcs` opens a Google Cloud Storage bucket as a `Source`, with the same pages, lookups,
walks, tokens, evidence, error classes and observer as S3 (the package's [`Example`](../source/gcs/example_test.go)):

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

`github.com/varveio/swath/source/local` opens a directory as a `Source`, on Linux and macOS. The program opens the
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
src, err := local.Open(root)
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

The same program is the package's [`Example`](../source/local/example_test.go), which the tests compile.

The program owns the directory: it keeps it open until the source's `Close` returns, and `Close` does not close
it. Keys are relative to it. A page's prefix is `""` for the directory itself or a directory's key, such as
`logs/2026/`, which ends in `/`; a lookup's key names an entry of any kind, `logs/a.gz` or `logs`. What the
library guarantees:

- **Descriptor-relative, never followed.** Each page and lookup opens what it reads relative to the program's
  directory, one name at a time, never following a symbolic link, and checks each directory it opens against
  the device of the program's directory. A symbolic link is listed as an entry of kind `symlink`. A call holds
  at most two descriptors at once and none once it returns; a source keeps nothing open between calls and is
  safe for concurrent use.
- **A page is one directory level, in byte order of keys.** Each entry has its kind (`dir` for a subdirectory,
  keyed with a final `/` as in `logs/2026/`, `file`, `symlink`, `other` for a FIFO, socket or device, or
  `unknown` when its stat fails) and what `lstat` gives of it: size, space on disk, modification time,
  device, inode and link count. The prefix's own directory comes back as the `Self` of the level's first page,
  beside its children: `Limit` bounds the children only. A directory on another device (or another macOS volume) is listed like any
  other, and a page of it fails with `listing.ErrOtherDevice`.
- **Pages continue by key.** A token resumes the level after the last key it read, so a directory that changes
  between pages repeats no key, keeps byte order across pages and skips no name that stayed; a name added
  after the last key read comes in a later page, one removed does not. It is bound to the directory's device
  and inode and to the prefix. A page reads its whole directory, again for each page, but holds only the names
  that may still be on it, so its memory follows the request's `Limit`, not the directory's size; its time
  follows the directory's size. A context deadline bounds the reading.
- **Failures in their class.** A prefix that is not there is `listing.ErrNotFound`; one naming something other
  than a directory, a symbolic link included, `listing.ErrNotDirectory`, and so is one with a symbolic link on
  the way, which is not followed: not reached, never absent. A refusal is `listing.ErrAuth`, a name too long for
  the system `listing.ErrUnsupported`, an I/O error `listing.ErrUnavailable`. A message names where a call
  failed by its depth below the directory and by the program's own key, never by more of a path.
- **A lookup is absent only when shown.** `ENOENT` or `ENOTDIR` at the key, or at a directory on the way that is
  no symbolic link, is absent; a symbolic link at the key is present, as itself; a key naming a directory
  answers with its directory key (`logs` with `logs/`). A symbolic link or another device on the way, a refusal,
  a name too long and an I/O error leave the key unknown: it was not reached, and its `Reason` has the class a
  page through the same place fails with (`listing.ErrNotDirectory` past a symbolic link).
- **Names are bytes.** A key is any bytes but NUL, so a name that is not valid UTF-8 pages and is looked up
  like any other.
- **A walk is the command's walker.** A walk takes `""` or a directory key, ending in `/`; any other prefix is
  `listing.ErrInvalidRequest`, never matched literally. It fails where a page of the same prefix fails, then hands
  on the directory's own entry (below the root) and lists what is under it with the parallel walker the command
  lists a tree with, by descriptor and following no symbolic link. Each directory's entries come unsorted and the batches in no
  key order. A directory it cannot open or read, an entry it cannot stat and a directory on another device (listed,
  not entered) reach your function as an `Unread` as the walk meets them. It has no depth limit, but no key longer
  than 32 KiB: such an entry, a directory with its subtree, is an `Unread` of class `listing.ErrUnsupported`. A walk
  changes no process-wide setting.

To count or trace a call's work, an `objfile.Observer` on its context is told of each page as one list request
and each lookup as one metadata request, named by the program's key.

## Run the command line

A program that needs a whole run, with its flags, output, progress files and exit codes, rather than one page or
lookup, can build the swath command line into its own binary. `command.Main(ctx, host, args, stdout, stderr)` runs one
command line (`args` without the program name) and returns its exit code; the swath binary is
`command.Exit(command.Main(...))` and nothing else. This package links the whole command, its providers and its
Parquet writer, which `Source` does not. Like the rest of the library it is experimental.

```go
command.Exit(command.Main(context.Background(), "example-host", os.Args[1:], os.Stdout, os.Stderr))
```

Call `Main` from a process dedicated to it, such as a child the host starts for one command line. It sets the
command's process-wide policy when it is called, never when the package is imported, and does not undo it when it
returns: the Azure SDK's own log is turned off, SIGINT, SIGTERM and SIGHUP (unless SIGHUP was ignored at start)
stop the run, SIGPIPE is ignored, and a run that a signal, `--max-duration`, `--stall-timeout` or
`--spill-wait-timeout` stopped and that has not finished stopping within the shutdown grace ends the process.
Cancelling `ctx` stops the run too, without that forced exit. Terminal detection (`--format auto`,
color, the live progress line) follows the `stdout` and `stderr` given to `Main`: a writer that is not an `*os.File`
is not a terminal. `command.Exit` ends the process as the binary does: on Unix, a run a signal stopped dies by that
signal.

`host` names the program this copy is built into (the binary passes `""`), and `--version` then says so. The version that
`--version`, the run report, the journal, Parquet `created_by` and the User-Agent give is swath's own: the version
of swath the host requires, or `devel` for a local replacement or a workspace module, with no revision, unless
the host's build sets swath's version or revision at link time.

## Run a listing from Go

`command.List(ctx, opts)` runs the work of `swath list` in the calling process from a `command.ListOptions` instead of
arguments: `Source` is the SOURCE operand, and each other field is the flag its tag names, with the meaning, values
and default `swath list --help` gives it. No field is parsed as an argument, so a source or a value that begins with
`-` is taken as it is. `List` goes through the code `Main` runs: the same rows, dataset, run report, progress file and
outcome. It returns a `ListResult` (completed, canceled or failed, with the class, cause and exit code `swath list`
would end with, [Exit codes](reference/exit-codes.md)) and, for a run that did not complete, a `*command.ListError`
that `errors.Is` matches to a stable reason, so a program branches without reading messages:

| Reason | When |
|---|---|
| `command.ErrOutputInUse` | another run, in this process or another, holds the output's lock; that run is left as it is |
| `command.ErrSourceNotFound` | the bucket, the container or the local root does not exist |
| `command.ErrSourceDenied` | the source refused the credentials or the access, or there were none, or the local root cannot be opened |
| `command.ErrUsage` | the options were refused before the run started (class `usage`, exit 2) |
| `context.Canceled`, `context.DeadlineExceeded` | the run stopped because `ctx` ended (its error is `ctx`'s error or cause), or `ctx` ended before the call got its turn: the result's `Status` is `ListCanceled`. A `ctx` that ends after the run failed otherwise, while the output is finalized or `Progress` runs, leaves a failed run failed |

A failure of the output (a bucket output that is not there, a directory that cannot be made) or of a file the run reads
besides its source (`--seed-from`, `--ca-bundle`) matches none of the source's reasons; a source's failure matches its
reason whether the output is a directory or a bucket. The error's text is the line
`swath list` prints and may change in any release.

```go
r, err := command.List(ctx, command.ListOptions{
	Source:   "s3://example-bucket/logs/",
	Output:   "/data/logs-inventory",
	Progress: func(p command.ListProgress) { log.Printf("%s: %d rows", p.State, p.RowsFetched) },
})
switch {
case errors.Is(err, command.ErrOutputInUse):
	// another run is writing there
case errors.Is(err, context.Canceled):
	// r.Resumable: swath resume can continue it
case err != nil:
	log.Fatal(err)
}
```

The package's [`ExampleList`](../command/example_test.go) is a whole program, which the tests run.

`Progress`, when set, is handed the run's progress (`ListProgress`, the fields of
[`progress.json`](watch-a-run.md)) at the progress file's cadence, from one goroutine at a time; a slow callback
misses the snapshots taken meanwhile and gets the latest, and holds up neither the progress file nor the stall
timeout. The callback samples on the tick the progress file is written, from its own snapshot. A run that started
hands it its final progress once the run has ended and its dataset, report and outcome are written, before `List`
returns; only that progress says `done` or `failed`. `List` waits for every call to return, and a call that calls `List` waits for the run that calls it to end, as
any other call does. The progress file is written as before.

Unlike `Main`, `List` sets no signal handling and no forced exit: the run stops when `ctx` ends, and `List` returns
once it has stopped and recorded its outcome. Like `Main` it turns the Azure SDK's own log off. Calls in one process
run one at a time: a call waits for the run before it, or for its own `ctx` to end, when it returns canceled and runs
nothing, as a call whose `ctx` had ended does. A panic out of `List` (from a `Stdout` or `Stderr`) leaves the next call
its turn. A process should not call `Main` and `List` at once. `Stdout` and `Stderr` take what the
command writes there (nil discards it), and a writer that is not an `*os.File` is not a terminal. For a run isolated
from the program, call `List` in a child process the program starts. A local source is given by its path:
`ListOptions` takes no opened directory yet.

## Not in the library yet

A walk on a local directory reports no observer requests. A source's request limits hold within one process only.
