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
| [providers/s3.md](providers/s3.md) | S3 (`internal/source/s3`): credentials, overload signals, versions, the schema, the range engine's page fetch |
| [providers/gcs.md](providers/gcs.md) | GCS (`internal/source/gcs`): credentials, generations, the schema, route parity |
| [providers/azblob.md](providers/azblob.md) | Azure Blob (`internal/source/azblob`): credentials, admission, the byte-order adapter, the schema |
| [clients.md](clients.md) | the list clients of the object-store providers: one per provider on the vendor's SDK service client, and what swath keeps around it |
| [range-engine.md](range-engine.md) | the range engine (`internal/source/keyrange`): splitting, seeds and priors, admission, resume, group mode |

## The contract (`internal/source`)

```go
type Emitter[R, P any] interface {
    Rows(rows []schema.Row[P]) error // next rows of this range, ascending; ownership passes to the emitter
    Child(r R) error                 // a child range whose whole subtree sorts here
}
type Provider[R, P any] interface {
    Root() R
    Expand(ctx context.Context, r R, e Emitter[R, P]) error
}
```

`P` is the provider's row payload (`localfs.Entry`, `s3.Object`), bound to columns by its `schema.Schema[P]`, which
lives beside the provider. A versions listing binds a schema with declared
[version roles](parquet-output.md#version-roles); generic lister consumers read those roles rather than
provider-specific column names.

A range is a provider-defined value. `Expand` emits its content in ascending key order: every key of a child's
subtree sorts after everything emitted before the child and before everything emitted after it. `Expand` may
call `Rows` many times (streaming) and runs concurrently for different ranges. Per-entry problems (permission
denied, a vanished entry) are counted by the provider; `Expand` returns an error only when the listing must
stop, including any error returned by the emitter (cancellation or a failed sink).

## Registry and optional interfaces

A provider package knows nothing of the command line or of the registry. The command's composition
(`internal/cli`, one file per provider) registers a `registry.Factory` for each from its `init`: the URL scheme
(`s3`; the empty scheme takes local paths), the provider's own flags (`Flags(fs, diag, shared) any` returns
the options value, filled by the parse), `Defaults`, and `Open(ctx, Request) (Source, error)`. Beside the flags
it declares what the command would otherwise know by name: `Title` (the help group of the provider's flags),
`Usage` (the root form in the help's SOURCE line), `Secrets` (flags never recorded, with the environment
variable that may stand in on resume) and `Operational` (flags a resume may change: how requests are
authenticated; secrets are operational without being listed). The S3 factory, for one (`internal/cli/s3.go`):

```go
registry.Register(registry.Factory{
	EncoderBytes: parquetout.EncoderBytes(&s3.Schema), Schemas: []schema.Info{s3.Schema.Info, s3.VersionsSchema.Info}, Classify: objhttp.Label,
	Scheme: "s3", KeyPrefix: s3.KeyPrefix, WithPrefix: s3.WithPrefix, Suggest: s3Suggest,
	Flags: s3Flags, Shared: []*registry.FlagGroup{StoreGroup, EngineGroup}, Defaults: s3Defaults, Open: openS3,
	Secrets:     []registry.Secret{{Flag: "s3-bearer-token-command"}},
	Operational: []string{"s3-profile", "s3-bearer-token-refresh-interval"},
	Title:       "S3", Example: "s3://bucket/logs/", Usage: "s3://bucket[/prefix]"})
```

**Shared flag groups.** Flags several providers take alike are a `registry.FlagGroup` the providers name in
`Shared`: the command registers each group once (`registry.Groups`, in the order the factories by scheme first
name them), passes its value to every provider that names it (`registry.SharedValues`, keyed by the group's
address, so two groups never alias by name; a provider reads it with `registry.Shared[T]`, which panics on a group
it did not name or a value of another type: a programming error, not a dropped flag), and records each of its flags as
owned by those providers, so it is accepted for any of their roots and refused for any other (a local tree).
A group declares its own `Operational` flags and `URLs` (flags whose value a recorded command line
redacts when it carries user information, `registry.RedactArgv`) once, not per provider. Two groups exist.
The store group (`cli.StoreGroup`, value `*source.Store`), named by S3, GCS and Azure Blob, supplies `--endpoint-url`
(the provider's own service replaced; part of each provider's resume identity, `endpoint` with its canonical
route), `--request-timeout` and `--anonymous` (the only way a request goes unauthenticated); the
last two are operational.

The range engine's group (`cli.EngineGroup`, value `*keyrange.Common`) holds a `keyrange.Policy`,
an `objhttp.Admission` and version-mode selection. It registers the policy comparisons and
slow start only when `diag` is true; the ordinary help shows the supported controls. The generated CLI
help ([commands/](../commands/swath.md)) owns option syntax and defaults. A provider's own flags keep its prefix (`--gcs-credentials-file`, `--azure-sas-token`; S3's
are unprefixed, the Java tool's names). Each factory's `Open` reads the group values, with the provider's own
flags and the environment variables they stand for, into the provider's typed options (`s3Flagged.options`,
`gcsFlagged.options`, ...), refusing the combinations that conflict. A test
or a benchmark registers one provider with its groups on a flag set of its own (`Factory.RegisterFlags`).
The flags are written in package `cli`'s flag table (`cli.Flag`, `cli.Define` and its values: `cli.Switch`,
`cli.Text`, ...), which also holds the two groups; `internal/source`, `keyrange` and the providers keep only
the plain configuration the flags fill (`source.Store`, `keyrange.Common`, each provider's `Options`) and import
no flag package.

The command imports no provider: it imports `internal/cli`, whose `init` registers them, and passes the options
value for the root's scheme as `listing.Config.Source.Options`, opaque to the pipeline; a test that opens a
provider by its scheme imports `internal/cli` the same way. The command names no provider, scheme or provider flag
(`TestImportBoundary`; `TestCommandNamesNoProvider` in `cmd/swath` is a tripwire on such spellings in its source: each constant string expression the type checker evaluates there, not values built at run time, [testing.md](testing.md)). `Open` validates the root without
touching the output. An object store's factory also gives `KeyPrefix(root)`, the key prefix the root names
(the provider's `KeyPrefix`, from its `ParseURL`): `swath du` counts its depth from it up to its last `/` (`logs/2026/0` from
`logs/2026/`), taken from the root as given, before an `--include` narrows it; without one (local paths) keys
are relative to the root. `WithPrefix(root, prefix)` (S3, GCS and Azure Blob) names another prefix of the same bucket, through
which includes sharing a literal prefix narrow the listed prefix. It returns the root unchanged to decline
(Azure: a prefix its URL cannot carry beside the SAS), and the root is listed as given with
`invocation.narrowing` `provider_declined_prefix`; without the hook it is `provider_lists_root`.
`CheckTotals` (the local factory) refuses per-prefix totals its options cannot give (`--allocated` with `--metadata names`; it turns off counting hard links once there, the names tier reading no
link counts) before the source opens, and `CheckFilter` (the local factory) refuses
filters on values its options do not read (sizes and times with `--metadata names`, storage classes).

A provider's typed opener (`s3.Open`, `s3.OpenVersions`, `localfs.Open`, ...) takes the root and the provider's
`Options` and returns its opened view (`s3.Opened[P]`, `localfs.Opened`): the provider bound to its schema
(`Bound`, a `bind.Bound` from `bind.Provider[R, P]`: the range type erased by `source.Erase`, ranges boxed once
each, rows untouched) and the methods a run asks of a source besides its rows. The binding chooses no sink: a
caller of `bind` lists the provider into its own (`Count`, unordered). A `registry.Source` is that view joined in
`internal/cli` with `registry.From(view.Bound())`, whose `Run` instantiates the order stage and the part router
for `P` and makes the run's sink (the part router, a text stream, or none for a count) once
`bind.Bound.Prepare` has checked the run's settings against the schema. It gives the schema's `Info`, the view's
`Locator` (kind, root, container, local path) and `Listed` (rows emitted, checked against the rows ordered). The local
view's `Begin` readies its one listing before `Run` (the directories the filters prune, the unreadable
directories' observer, and a count's unordered walk and the processors it runs with), which the command's
local source calls. Optional interfaces, each found by a type assertion:

| Interface | Meaning | Object stores | Local filesystem |
|---|---|---|---|
| `Identified` | Source-owned resume identity fields and their canonical forms | Each provider page owns its identity fields | – |
| `Resumable` | `Restart(after)` before `Root` | Promoted from the engine ([resume](range-engine.md#resume)) | – |
| `Capable` | Per-instance capabilities; an instance may decline resume despite implementing it | S3, GCS and Azure | – |
| `Excluder` | Leaves an output directory inside its root out after output preparation | – | yes |
| `Reporter` | Named stats, text line, errors, metadata tier, coverage and run-summary activity | `s3`, `gcs`, `azblob` | `listing` |

The other optional interfaces are the live run's: `Live` (`Answered`), `Advancing` (`Frontier`), `Requested`
(`Requests`), `SeedTimed`, `Faulted` (`Faults`), `Contended`, and beside `Resumable` the resume's `Seeded`
(`SeedFrom`), `Reusing` (`Reuse`) and `Fencing` (`Fences`), and `Traced` (`TraceTo`, the run trace:
[trace.md](trace.md)). A provider on the range engine has them all, and they
are listed once, as `keyrange.Registered` (with `Listed`): the opened view embeds that interface set to the
provider, and the command's source embeds the view beside the `registry.Bound` of the same provider, so the
methods are promoted and nothing else of the provider is (not its `Root`, `Expand`, `Stats`, `Options` or
counter `Set`); a provider that lacks one fails to compile there, where an absent method would otherwise
silently turn its capability off. The provider satisfies it by composing the engine's methods, including
[engine-owned restart](range-engine.md#resume), with the client's request activity through
[`objhttp.Activity`](clients.md#retry-policy-and-accounting). Set `Registered: p`, not `p.Engine`: the engine
alone does not supply the client's `Answered`, `Requests` or `Faults`. What an object store writes beside its
engine is then its openers and the view's own methods (`open.go`): `Locator`, `Identity`, `Capabilities`
(whether this instance can resume), `Report`, `Tariff` where it has one, and a `var _` block asserting the view's
set per row type (`keyrange.Registered`, `Identified`, `Capable`, `Reporter`, `Priced`); its file in
`internal/cli` asserts the joined source's again, with `registry.Source`, since a method two embedded halves
both had would be dropped. `internal/cli` `TestRegisteredSources` opens each provider through `registry.Lookup`
and checks the interfaces and capabilities.

A source that is not resumable (or declines `Resume`) runs without a journal; it still takes the output lock
like every run with an output. `swath resume` on it is an error.

## Adding a provider

The runtime integration is one directory under `internal/source/` and one file in `internal/cli`.
The directory holds the provider's range and payload types, constructor and options, schema accessors and
spill codec, page mapping, URL parsing, identity and reporting, tests and fixtures. An object store also
holds its `client/` SDK adapter and any service-specific ordering adapter. A filesystem provider implements
its own expansion instead of using the range engine.

The provider exports typed openers: `Open(ctx, root, Options)`, and `OpenVersions` for a versions listing,
parse the root into the options, construct the provider and bind it to its row schema (`bind.Provider`),
returning its opened view (`Opened[P]`). A range-engine view embeds `keyrange.Registered` set to that same
provider, and supplies `Bound`, `Locator`, `Identity`, `Capabilities`, `Report` and, where known, `Tariff`,
with compile-time interface assertions for each row type. The provider exports the root's own syntax too:
`ParseURL`, `KeyPrefix` and `WithPrefix`. It imports no flag package, registry or writer, and does not
register itself (`cli` `TestProviderImportBoundary`).

Its file in `internal/cli` (`s3.go`, `gcs.go`, `azblob.go`, `localfs.go`) registers the factory described above
from `cli`'s `init`: it writes the provider's flag declarations; turns the parsed flags, the shared group
values and the environment variables they stand for into the provider's options, refusing conflicting
ones; dispatches `Open` on them; and returns the source a run takes, a struct embedding
`registry.From(view.Bound())` and the view, with its own interface assertions. The factory declares
classification (`objhttp.Label` for the object stores), encoder admission, prefix helpers, other tools' root
forms (`Suggest`, `FromURL`), root redaction, secrets, operational flags and root usage as applicable, and its
bound schemas' `Info` (`Schemas`), by which `swath du --from-dataset` reads its datasets. Command dispatch,
scheme lists, help, flag ownership, resume overrides and argument redaction derive from registration; the
command imports `internal/cli`, never a provider.

These integrations still require work outside the provider directory:

| Touch point | When needed |
|---|---|
| `internal/cli/<provider>.go`, `internal/cli/<provider>_test.go` | The registration: flags, options, the factory and the joined source; the tests of its flags, environment variables and conflicts. The command's spelling guard discovers the registered roster; it needs no provider import. |
| `reader/schema.go` | A public dataset schema specification, including version roles when applicable. A directly declared producer schema can list without reader support. |
| `reader/rows.go` | Current-object projection in `objectColumns` and any new typed row decoder. |
| `dataset_schema.py`, `verify.py` | Independent schema and verifier support. |
| `cmd/swath/flagcheck_test.go` | Exact expected owners for another provider that takes a shared flag group. |
| `internal/cli/registered_test.go` | Central registration, interface, restart, locator and client-reporting coverage. |
| `internal/listing/producerschema_test.go` | Producer schema golden coverage (`testdata/producer-schema-*.golden`). |
| `internal/listing/reader_fixtures_test.go`, `scripts/ci/seekparquet-conformance.sh`, `scripts/ci/seekparquet_conformance.py` | When adding conformance support: produce the schema's fixture in `TestSeekParquetFixtures`, select it in the shell runner and declare projected columns in Python's `COLUMNS`. The scripts live at the repository root. |
| `internal/replay/store/sorted.go` | When adding replay support: register the schema projection in `objectProjections`; unknown schema IDs currently replay keys only. |
| `go.mod`, `go.sum` | Additional SDK dependencies, if needed. |

Shared help in `internal/cli/groups.go` (the store's and the range engine's groups) contains provider examples;
updating them is an optional wording change. Documentation and generated help blocks also need review.
No command branch or pipeline import of the provider is needed.

The three object stores compose `keyrange.Engine` and `Registered`, [`objhttp` client plumbing](clients.md),
and the [schema bindings](parquet-output.md#schemas) and [payload codec helpers](ordering.md#payload-delta-codec);
their registrations compose `cli.StoreGroup` and `cli.EngineGroup`. Explicit provider ownership remains important:
credential selection and warnings, endpoint grammar and identity, namespace admission, accepted bounds,
wire request and row mapping, status/code precedence, decoding checks, report shape, pool-size probes and
pricing. The shared helpers do not choose these policies.

## Provider capabilities

What a provider must state about itself (built: `source.Capable` with `Resume`, `Seek` and `RankOracle`,
`source.Resumable`, `source.Disposer`):
- **Applicability:** its seek kind and whether it has a rank oracle (above).
- **Order:** output is unsigned byte order; a provider whose native order differs sorts or partitions inside
  itself; there are no per-range comparators.
- **Bounds:** explicit bound inclusivity, as S3's `(lo, hi]`.
- **Bounded batches:** stream rows in batches; the order stage splits large `Rows` calls into publish
  batches. Provider staging sits outside that budget: object-store clients cap response bodies, while the
  filesystem reads and sorts a directory's names before handing out chunks ([local.md](providers/local.md)).
- **Restart:** start after a key; the journal stores no provider state. A provider that can only replay
  tokens must re-list to the key or declare `Capabilities.Resume` false.
- **Disposal:** a canceled run releases every queued range's resources: the order stage disposes ranges it
  never expands, including children an `Expand` emitted before failing (`Child` takes ownership even when it
  returns an error). A range a worker has taken is the provider's: its `Expand` releases it however it ends,
  a return on an already-canceled context included. Filesystem chunks release their shared directory
  descriptor both ways (`TestCancelReleasesChunkFDs`, `TestCancelledChunkReleasesItsFD`).
