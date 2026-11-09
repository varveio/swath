# Providers

**Does the range engine apply?** Dynamic range splitting (ranges, owner-side splits at synthesized pivots,
seeding) applies to a source only when all three hold: it has **no rank oracle** (nothing can say how many
keys precede a key, which would allow an exact partition instead), it has a **fabricable seek** (a listing can
start after any key the caller makes up, as S3's `start-after` and GCS's `startOffset`), and its pages are **byte-ordered** (every
page ascends in unsigned byte order and continues the last). A listing sorted in another order (UTF-16 code
units put U+10000 before U+E000, the reverse of byte order, and the disorder can cross pages) or unsorted does
not qualify, even if the provider sorts its output: it normalizes behind `source.Provider` instead. A source
with a token-only seek is listed as one sequential chain per root; one with no seek reads each range whole and
sorts it inside the provider. Each provider declares this in `source.Capabilities` (`Seek`: `fabricable`,
`token_only`, `none`; `Order`: `bytes`, `other`, `none`; `RankOracle`; `RangeEngine()` is the test, and
`keyrange.New` refuses a `Description.Listing` that fails it): S3 and GCS are fabricable and byte-ordered without a
rank oracle (`token_only` with `--sequential`); Azure Blob lists in UTF-16 code-unit order, and is byte-ordered
through its provider's order adapter, which serves every engine query in byte order ([providers/azblob.md](providers/azblob.md)); the filesystem has no seek and no order (a directory is a
range, read whole and sorted).

## Pages

The provider pages, the shared list clients and the range engine each have their own page; this one keeps what
every provider shares (the contract, the registry and the capabilities).

| Page | What it covers |
|---|---|
| [providers/local.md](providers/local.md) | the local filesystem (`internal/source/localfs`): engines, metadata tiers, chunked directories |
| [providers/s3.md](providers/s3.md) | S3 (`internal/source/s3`): credentials, the adaptive limiter, versions, the schema, the range engine's page fetch |
| [providers/gcs.md](providers/gcs.md) | GCS (`internal/source/gcs`): credentials, generations, the schema, route parity |
| [providers/azblob.md](providers/azblob.md) | Azure Blob (`internal/source/azblob`): credentials, admission, the byte-order adapter, the schema |
| [clients.md](clients.md) | the list clients of the object-store providers: one per provider on the vendor's SDK service client, and what swath keeps around it |
| [range-engine.md](range-engine.md) | the range engine (`internal/source/keyrange`): splitting, seeds and priors, admission, resume, group mode |

## The contract (`internal/source`)

```go
type Emitter[R, P any] interface {
    Rows(rows []schema.Row[P]) error // next rows of this range, ascending; the slice now belongs to the caller
    Child(r R) error                 // a child range whose whole subtree sorts here
}
type Provider[R, P any] interface {
    Root() R
    Expand(ctx context.Context, r R, e Emitter[R, P]) error
}
```

`P` is the provider's row payload (`fs.Entry`, `s3.Object`), bound to columns by its `schema.Schema[P]`, which
lives beside the provider.

A range is a provider-defined value. `Expand` emits its content in ascending key order: every key of a child's
subtree sorts after everything emitted before the child and before everything emitted after it. `Expand` may
call `Rows` many times (streaming) and runs concurrently for different ranges. Per-entry problems (permission
denied, a vanished entry) are counted by the provider; `Expand` returns an error only when the listing must
stop, including any error returned by the emitter (cancellation or a failed sink).

## Registry and optional interfaces

A provider registers a `registry.Factory` from `init`: its URL scheme (`s3`; the empty scheme takes local
paths), its own flags (`Flags(fs, diag, shared) any` returns the provider's options value, filled by the
parse), `Defaults`, and `Open(ctx, Request) (Source, error)`. Beside its flags it declares what the command
would otherwise know by name: `Usage` (the root form in the help's SOURCE line), `Secrets` (flags never
recorded, with the environment variable that may stand in on resume), `Operational` (its flags a resume may
change: how requests are authenticated; secrets are operational without being listed) and `Hints` (every flag
it registers, in help order, as `source.Hint{Flag, When}`: a flag with a `When` text is shown under the help's
Limits with that advice, a diagnostic one under the diagnostics, any other under Source and output). The S3
factory, for one:

```go
registry.Register(registry.Factory{Scheme: "s3", KeyPrefix: keyPrefix, WithPrefix: withPrefix,
	Flags: flags, Shared: []*source.FlagGroup{source.StoreGroup, keyrange.Group}, Defaults: defaults, Open: open,
	Secrets:     []registry.Secret{{Flag: "bearer-token-command"}},
	Operational: []string{"profile", "bearer-token-refresh-interval"},
	Hints: []source.Hint{{Flag: "region"}, {Flag: "path-style"}, {Flag: "profile"}, ...},
	Usage: "s3://bucket[/prefix]", ...})
```

**Shared flag groups.** Flags several providers take alike are a `source.FlagGroup` the providers name in
`Shared`: the command registers each group once (`registry.Groups`, in the order the factories by scheme first
name them), passes its value to every provider that names it (`source.SharedValues`, keyed by the group's
address, so two groups never alias by name; a provider reads it with `source.Shared[T]`, which panics on a group
it did not name or a value of another type: a programming error, not a dropped flag), and records each of its flags as
owned by those providers, so it is accepted for any of their roots and refused for any other (a local tree).
A group declares its own `Operational` flags, `Hints` and `URLs` (flags whose value a recorded command line
redacts when it carries user information, `registry.RedactArgv`) once, not per provider. Two groups exist.
The store group (`source.StoreGroup`, value `*source.Store`), which S3, GCS and Azure Blob name: `--endpoint`
(the provider's own service replaced; part of each provider's resume identity, `endpoint` with its canonical
route), `--request-timeout` (default 30 s) and `--anonymous` (the only way a request goes unauthenticated); the
last two are operational. The range engine's (`keyrange.Group`, value `*keyrange.Common`: a `keyrange.Policy`,
an `objhttp.Admission` and `--all-versions`): the policy flags `--seed`, `--structure`, `--slow-split`,
`--floors`, `--sequential`, `--max-keys`, `--page-size` and the admission flags `--limiter`,
`--limiter-slow-start`, `--request-rate`; all but `--limiter`, `--request-rate` and `--all-versions` diagnostic,
registered with `diag` only. A provider's own flags keep its prefix (`--gcs-credentials`, `--azblob-sas`; S3's
are unprefixed, the Java tool's names). Each provider's `Open` reads the group values into its options. A test
or a benchmark registers one provider with its groups on a flag set of its own (`Factory.RegisterFlags`).

The command imports providers only for that registration and passes the options value for the root's scheme as
`listing.Config.Source.Options`, opaque to the pipeline; it names no provider, scheme or provider flag
(`TestImportBoundary`; `TestCommandNamesNoProvider` in `internal/source` is a tripwire on such spellings in its source, [testing.md](../testing.md)). `Open` validates the root without
touching the output. An object store's factory also gives `KeyPrefix(root)`, the key prefix the root names
(each provider's `ParseURL`): `swath du` counts its depth from it up to its last `/` (`logs/2026/0` from
`logs/2026/`), taken from the root as given, before an `--include` narrows it; without one (local paths) keys
are relative to the root. `WithPrefix(root, prefix)` (S3 and GCS) names another prefix of the same bucket, through
which an anchored `--include` narrows the listed prefix; Azure has none, and lists its root as given.
`CheckTotals` (the local provider) refuses per-prefix totals its options cannot give (`--allocated` or
`--count-links-once` with `--metadata names`) before the source opens.

A `registry.Source` is the provider bound to its schema (`registry.Bind[R, P]`: the range type erased by
`source.Erase`, ranges boxed once each, rows untouched; `Run` instantiates the order stage and the part router
for `P`), its schema `Info`, its `Locator` (kind, root, container, local path) and `Listed` (rows emitted, checked
against the rows ordered). Optional interfaces, each found by a type assertion:

| Interface | Meaning | S3 | Azure Blob | fs |
|---|---|---|---|---|
| `Identified` | its part of the resume identity: named fields, each with a canonical form for comparison | source, endpoint (canonical: `CanonicalRoute`), region, path style, fips, dual-stack, fetch owner | source, endpoint (canonical: `CanonicalRoute`), account, container, flat_asserted | – |
| `Resumable` | `Restart(after)` before `Root`: list only keys after the last published key | `(K, ⊤]` | `(K, ⊤]` | – |
| `Capable` | per-instance capabilities (`Resume`): an instance may decline what its type implements | `Resume` | `Resume` | – |
| `Excluder` | leaves an output directory inside its root out, after the output is prepared | – | – | yes |
| `Reporter` | its stats under its name in the summary, a text line, error count, metadata tier, and for a remote source its `Coverage` (target, filters, complete, stop reason) and `Activity` for swath's run summary | `s3`, coverage | `azblob`, coverage | `listing` |

The other optional interfaces are the live run's: `Live` (`Answered`), `Advancing` (`Frontier`), `Requested`
(`Requests`), `SeedTimed`, `Faulted` (`Faults`), `Contended`, and beside `Resumable` the resume's `Seeded`
(`SeedFrom`), `Reusing` (`Reuse`) and `Fencing` (`Fences`). A provider on the range engine has them all, and they
are listed once, as `keyrange.Registered` (with `Listed`): its opened source embeds that interface set to the
provider, beside the `registry.Bound` of the same provider, so the methods are promoted and nothing else of the
provider is (not its `Root`, `Expand`, `Stats`, `Options` or counter `Set`); a provider that lacks one fails to
compile there, where an absent method would otherwise silently turn its capability off. The provider, not its
`Engine`, satisfies it: `Restart` also records the resume in the provider's options, and `Answered`, `Requests` and
`Faults` are its client's, read through an `objhttp.Activity` the provider embeds, observed once the client is
chosen (each part reads zero where a supplied client does not count it). What an object store's `register.go`
writes is then its flags, `open` (the options, the provider, the opened struct), and the provider's own methods:
`Locator`, `Identity`, `Capabilities` (whether this instance can resume), `Report`, `Tariff` where it has one,
and a `var _` block asserting the opened type's whole set per row type (`registry.Source`, `keyrange.Registered`,
`Identified`, `Capable`, `Reporter`, `Priced`). `internal/source` `TestRegisteredSources` opens each provider
through `registry.Lookup` and checks the interfaces and capabilities.

A source that is not resumable (or declines `Resume`) runs without a journal; it still takes the output lock
like every run with an output. `swath resume` on it is an error.

## Provider capabilities

What a provider must state about itself (built: `source.Capable` with `Resume`, `Seek` and `RankOracle`,
`source.Resumable`, `source.Disposer`):
- **Applicability:** its seek kind and whether it has a rank oracle (above).
- **Order:** output is unsigned byte order; a provider whose native order differs sorts or partitions inside
  itself; there are no per-range comparators.
- **Bounds:** explicit bound inclusivity, as S3's `(lo, hi]`.
- **Bounded cost:** one `Rows` call has a bounded cost; no unbounded in-memory directory or page.
- **Restart:** start after a key; the journal stores no provider state. A provider that can only replay
  tokens must re-list to the key or declare `Capabilities.Resume` false.
- **Disposal:** a cancelled run releases every queued range's resources: the order stage disposes ranges it
  never expands, including children an `Expand` emitted before failing (`Child` takes ownership even when it
  returns an error). A range a worker has taken is the provider's: its `Expand` releases it however it ends,
  a return on an already-cancelled context included. Filesystem chunks release their shared directory
  descriptor both ways (`TestCancelReleasesChunkFDs`, `TestCancelledChunkReleasesItsFD`).
