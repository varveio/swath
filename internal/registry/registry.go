// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package registry connects providers to the pipeline. Providers register a
// Factory by URL scheme from an init function; the command imports them only
// for that. An opened source is bound (Bind) to the typed pipeline stages
// with its range and row types, which the pipeline itself never names:
// listing, order, parquetout, dataset, schema and journal import no
// provider.
package registry

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/pflag"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source"
	"github.com/varveio/swath/go/internal/textout"
	"github.com/varveio/swath/go/internal/totals"
)

// A Factory opens one kind of source.
type Factory struct {
	// Scheme is the root's URL scheme ("s3" for s3://bucket/prefix). The
	// factory with the empty scheme takes roots without one (local paths).
	Scheme string
	// Flags registers the provider's own flags on fs and returns its
	// options value, which the flags fill when fs is parsed. The value is
	// opaque to everything but the provider: it travels in
	// Request.Options. diag adds the provider's diagnostic flags
	// (comparison modes and policy toggles, for swath diag list only).
	// shared holds the values of the groups the provider names (Shared),
	// read with source.Shared.
	Flags func(fs *pflag.FlagSet, diag bool, shared source.SharedValues) any
	// Shared are the flag groups the provider shares with others (the
	// store's, the range engine's): the command registers each once and
	// passes its value to every provider naming it.
	Shared []*source.FlagGroup
	// Operational are the provider's own flags a resume may change: how
	// requests are authenticated, which no identity holds. Secrets are
	// operational without being listed here.
	Operational []string
	// Example is a root of the provider's for the help's examples
	// ("s3://bucket/logs/", "/data").
	Example string
	// Title names the provider for people: the help group of its own flags
	// ("S3", "Local") and the messages that name it.
	Title string
	// FromURL, if set, returns the provider's root for a source given in
	// another form (a service URL a console shows), and whether root is
	// one; Canonical tries it.
	FromURL func(root string) (string, bool)
	// Defaults returns the options value used when none was given.
	Defaults func() any
	// Open validates the root and returns a source ready to list. It must
	// not touch the output directory.
	Open func(ctx context.Context, req Request) (Source, error)
	// KeyPrefix, if set, returns the key prefix root names (s3://b/logs/ →
	// "logs/"): the keys it lists start with it. Without it the keys are
	// relative to the root (a local tree's).
	KeyPrefix func(root string) (string, error)
	// EncoderBytes is the working set admitted per Parquet encoder of the
	// provider's schema (parquetout.EncoderBytes), known before the source
	// is opened; 0 prices it as parquetout.DefaultEncoderBytes.
	EncoderBytes int64
	// Workers, if set, returns the provider's default --workers, known
	// before the source is opened; without it the listing's default
	// (listing.DefaultWorkers) applies.
	Workers func() int
	// Classify, if set, names the class of a failure the provider caused
	// ("throttle", "access_denied", ...), for the run's outcome; "" if it
	// does not know the error.
	Classify func(error) string
	// Redact, if set, returns the root with any credential it carries
	// replaced (an Azure SAS in its query), for everything that shows or
	// records a root: the command line, the terminal and the report. The
	// root itself still goes to Open.
	Redact func(root string) string
	// Secrets are the provider's flags that hold a secret, or a command
	// that prints one: the command never records their values, and a resume
	// must be given them again.
	Secrets []Secret
	// Usage is the root form for the command's help ("s3://bucket[/prefix]";
	// "a local directory" for local paths).
	Usage string
	// CheckTotals, if set, refuses per-prefix totals (swath du) that the
	// provider's options value cannot give, before the source is opened.
	CheckTotals func(options any, t totals.Options) error
	// CheckFilter, if set, refuses filters on columns the provider's
	// options do not fill (filter.Spec.Reads), before the source is opened.
	CheckFilter func(options any, f *filter.Spec) error
	// WithPrefix, if set, returns root naming the key prefix prefix
	// instead of its own (KeyPrefix): an anchored --include narrows the
	// listed prefix through it. Without it the root is listed as given.
	WithPrefix func(root, prefix string) string
}

// RegisterFlags registers f's flags and its shared groups' on fs, a flag
// set of this provider alone (a test's, a benchmark's), and returns its
// options value. The command instead registers each group once for every
// provider (Groups).
func (f Factory) RegisterFlags(fs *pflag.FlagSet, diag bool) any {
	shared := source.SharedValues{}
	for _, g := range f.Shared {
		shared[g] = g.Flags(fs, diag)
	}
	return f.Flags(fs, diag, shared)
}

// Groups returns the flag groups the registered factories share, each
// once (by identity), in the order the factories (by scheme) first name
// them.
func Groups() []*source.FlagGroup {
	var out []*source.FlagGroup
	for _, f := range Factories() {
		for _, g := range f.Shared {
			if !slices.Contains(out, g) {
				out = append(out, g)
			}
		}
	}
	return out
}

// Secret is a provider flag whose value is never recorded (Flag, without
// dashes) and the environment variable the provider reads the same
// credential from when the flag is absent ("" when none), which on resume
// may stand in for the flag.
type Secret struct{ Flag, Env string }

// Request is what the pipeline tells a factory.
type Request struct {
	Root    string       // as given, scheme included
	Options any          // the provider's options value (Flags or Defaults)
	Workers int          // the order stage's worker count
	Warn    func(string) // one-time warnings for the user; never nil
}

// Source is an opened provider bound to the pipeline (see Bind). It may
// also implement the optional interfaces of package source (Identified,
// Resumable, Capable, Excluder, Reporter).
type Source interface {
	Schema() schema.Info
	Locator() source.Locator
	// Listed is the number of rows emitted so far; after a listing it
	// must equal the rows the order stage handed on.
	Listed() int64
	// Run lists the source through the order stage into a part router
	// (or nowhere, for a dry run).
	Run(ctx context.Context, env Env) (order.Stats, parquetout.Encoder, error)
}

// Env is everything about a run except the row type.
type Env struct {
	Order order.Options
	// Router, if set, routes the rows into parts; nil discards them.
	Router *parquetout.Options
	// Count counts the rows without ordering them (order.Count): a dry
	// run, whose rows nothing consumes; Router and Text must be nil.
	Count bool
	// Empty lists nothing: a resume whose final part is durable.
	Empty bool
	// Ordered, if set, counts rows the order stage has handed on.
	Ordered *atomic.Int64
	// Text, if set (and Router nil), writes the rows as text there.
	Text *textout.Out
	// Filter, if set, drops the rows it does not accept before the order
	// stage, counting them in Filtered.
	Filter   *filter.Spec
	Filtered *atomic.Int64
	// Sizes, if set, sums the object sizes of the rows handed to the order
	// stage (after the filters): the report's output.object_bytes.
	Sizes *Sizes
	// Versions counts the attempt's emitted version rows after filtering.
	Versions *VersionCounts
	// Totals, if set, sums the counted rows per key prefix (swath du); it
	// needs Count.
	Totals *totals.Tally
}

// Sizes is the object bytes a run emitted. Measured says the schema has a
// size column (Run sets it before listing; read it after Run returns).
type Sizes struct {
	Bytes    atomic.Int64
	Measured bool
}

// Bound is a provider bound to its schema; embed it in an opened source.
type Bound[P any] struct {
	p source.Provider[any, P]
	s *schema.Schema[P]
	// v is s's version roles (schema.BindVersions), nil for none.
	v *schema.Versions[P]
}

// Bind erases p's range type and binds it to its row schema and its
// version roles. It panics on roles that do not define the rows
// (schema.BindVersions), as schema.Bind does: a producer's bug, found when
// the source is opened, before any output exists.
func Bind[R, P any](p source.Provider[R, P], s *schema.Schema[P]) Bound[P] {
	v, err := schema.BindVersions(s)
	if err != nil {
		panic(err.Error())
	}
	return Bound[P]{source.Erase[R, P](p), s, v}
}

// Schema is the bound schema's description.
func (b Bound[P]) Schema() schema.Info { return b.s.Info }

// Run implements Source.Run with the provider's row type.
func (b Bound[P]) Run(ctx context.Context, env Env) (order.Stats, parquetout.Encoder, error) {
	// The grouping policy is the schema's: the order stage, Count and the
	// router (which reads the schema) all take it from there.
	env.Order.Groups = b.s.Info.Groups
	keep, err := filter.Compile(env.Filter, b.s)
	if err != nil {
		return order.Stats{}, nil, err
	}
	vers := b.v
	var sink order.Sink[P] = discard[P]{}
	var enc parquetout.Encoder
	if env.Router != nil {
		r, err := parquetout.NewRouter(*env.Router, b.s)
		if err != nil {
			return order.Stats{}, nil, err
		}
		sink, enc = segSink[P]{r, env.Ordered}, r
	} else if env.Text != nil {
		t, err := textout.NewSink(env.Text, b.s)
		if err != nil {
			return order.Stats{}, nil, err
		}
		sink = t
	}
	if _, seg := sink.(segSink[P]); env.Ordered != nil && !seg { // segSink counts its own rows
		sink = countSink[P]{sink, env.Ordered}
	}
	counted := env.Versions != nil && vers != nil
	observer := versionSink[P]{counts: env.Versions, v: vers}
	if counted {
		env.Versions.Enabled, env.Versions.HasDeleteMarkers = true, vers.HasDeleteMarkers()
		observer.Sink = sink
		sink = observer
		if sg, ok := observer.Sink.(order.Segmenter[P]); ok { // the tail's segments are counted too
			sink = versionSegmenter[P]{observer, sg}
		}
	}
	p := b.p
	if env.Empty {
		p = source.Empty[P]{}
	} else if keep != nil {
		p = newFiltered(p, keep, b.s, env.Filtered)
	}
	if env.Sizes != nil && !env.Empty {
		if size := schema.ObjectSize(b.s); size != nil {
			env.Sizes.Measured = true
			p = newSized(p, size, &env.Sizes.Bytes)
		}
	}
	if env.Totals != nil && !env.Count {
		return order.Stats{}, nil, errors.New("registry: per-prefix totals need a count")
	}
	if env.Count {
		var observe order.Sink[P]
		if counted {
			observer.Sink = discard[P]{}
			observe = observer
		}
		if env.Totals != nil {
			if observe, err = bindTotals(env.Totals, b.s, vers, observe); err != nil {
				return order.Stats{}, nil, err
			}
		}
		st, err := order.CountWithSink(ctx, p, env.Ordered, env.Order, observe)
		return st, nil, err
	}
	opt := env.Order
	if enc != nil {
		// Once listing has ended, the encoders the router reserved for
		// the drain are admitted (parquetout.Options.MaxEncoders).
		prev := opt.ListingDone
		opt.ListingDone = func() {
			enc.Admit()
			if prev != nil {
				prev()
			}
		}
	}
	st, err := order.Run(ctx, p, sink, b.s, opt)
	return st, enc, err
}

// segSink is the Parquet router as the order stage's sink: rows through
// Write, and once listing is done the rest as parallel segments
// (order.Segmenter), both counted in n when set (progress).
type segSink[P any] struct {
	r *parquetout.Router[P]
	n *atomic.Int64
}

func (s segSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	s.count(segs)
	return s.r.Write(ctx, segs)
}

func (s segSink[P]) count(segs [][]schema.Row[P]) {
	if s.n != nil {
		for _, seg := range segs {
			s.n.Add(int64(len(seg)))
		}
	}
}

func (s segSink[P]) Plan() (dataset.SegmentPlan, bool) { return s.r.Plan() }

func (s segSink[P]) BeginSegments() (dataset.SegmentPlan, bool) { return s.r.BeginSegments() }

func (s segSink[P]) AbandonSegments() { s.r.AbandonSegments() }

func (s segSink[P]) OpenSegment(ctx context.Context, continueOpen bool) (order.SegmentSink[P], error) {
	seg, err := s.r.OpenSegment(ctx, continueOpen)
	if err != nil {
		return nil, err
	}
	return segPart[P]{seg, s}, nil
}

type segPart[P any] struct {
	*parquetout.Segment[P]
	s segSink[P]
}

func (p segPart[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	p.s.count(segs)
	return p.Segment.Write(ctx, segs)
}

type countSink[P any] struct {
	order.Sink[P]
	n *atomic.Int64
}

func (c countSink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	for _, s := range segs {
		c.n.Add(int64(len(s)))
	}
	return c.Sink.Write(ctx, segs)
}

type discard[P any] struct{}

func (discard[P]) Write(context.Context, [][]schema.Row[P]) error { return nil }

var (
	regMu     sync.Mutex
	factories = map[string]Factory{}
)

// Register adds a factory; registering a scheme twice panics.
func Register(f Factory) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := factories[f.Scheme]; ok {
		panic("registry: scheme registered twice: " + f.Scheme)
	}
	factories[f.Scheme] = f
}

// Redact is root as it may be shown or recorded (Factory.Redact): with its
// provider's credential redacted, or as it is. A root whose authority holds
// user information, ? or # is SCHEME://[redacted] (source.RedactRoot), as is
// a URL of a scheme no provider takes, which none can tell a literal prefix
// from a credential in, when it has a query or a fragment or does not parse.
func Redact(root string) string { return RedactPlace(Canonical(root)) }

// RedactPlace is a URL-valued flag's value (source.ShownURL) as it may be
// shown or recorded: redacted as Redact redacts a root, in the form given.
func RedactPlace(root string) string {
	if rootCredentials(root) {
		return source.RedactRoot(root)
	}
	f, err := Lookup(root)
	switch {
	case err == nil && f.Redact != nil:
		return f.Redact(root)
	case err != nil && urlScheme(root):
		if _, perr := url.Parse(root); perr != nil || strings.ContainsAny(root, "?#") {
			return root[:strings.Index(root, "://")+3] + redactedValue
		}
	}
	return root
}

// redactedValue replaces a credential in a recorded or shown argument.
const redactedValue = "[redacted]"

// urlScheme says s starts with a URL scheme and "://".
func urlScheme(s string) bool {
	i := strings.Index(s, "://")
	if i <= 0 || !('a' <= s[0] && s[0] <= 'z' || 'A' <= s[0] && s[0] <= 'Z') {
		return false
	}
	for _, c := range s[1:i] {
		if !('a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '+' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

// rootCredentials checks the authority only: a listing prefix is a literal key.
func rootCredentials(root string) bool {
	return source.RedactRoot(root) != root
}

// RedactArgv copies a canonical command line (each flag --name=value or
// --name, then the operands) for reports and journals: registered secret
// flags, credential-bearing URL flags (source.FlagGroup.URLs) and
// credentials in the operands (source roots) are redacted; any other
// flag's value is data (a filter), kept as given. The command redacts by
// each flag's table entry (source.Flag.Shown) as it records the line;
// this is the second pass of a caller that hands one on.
func RedactArgv(argv []string) []string {
	secrets, urls := map[string]bool{}, map[string]bool{}
	for _, u := range OutputURLFlags {
		urls[u] = true
	}
	for _, s := range OutputSecretFlags {
		secrets[s] = true
	}
	for _, f := range Factories() {
		for _, s := range f.Secrets {
			secrets[s.Flag] = true
		}
	}
	for _, g := range Groups() {
		for _, u := range g.URLs {
			urls[u] = true
		}
	}
	out := make([]string, 0, len(argv))
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		name, value, eq := strings.Cut(strings.TrimLeft(a, "-"), "=")
		if a == "--" || !strings.HasPrefix(a, "-") {
			out = append(out, Redact(a))
			continue
		}
		hide := secrets[name]
		if urls[name] {
			if !eq && i+1 < len(argv) {
				value = argv[i+1]
			}
			hide = RedactURL(value) != value
		}
		if !hide {
			out = append(out, a)
			continue
		}
		dashes := a[:len(a)-len(strings.TrimLeft(a, "-"))]
		out = append(out, dashes+name+"="+redactedValue)
		if !eq && i+1 < len(argv) {
			i++
		}
	}
	return out
}

// OutputURLFlags are the output's flags whose value is a URL that may
// carry a credential (a destination's endpoint, a dataset URI): RedactArgv
// hides each as it hides a provider's (source.FlagGroup.URLs), whichever
// caller records the command line.
var OutputURLFlags = []string{"output", "output-endpoint-url"}

// OutputSecretFlags are the output's flags whose value is a secret (an
// Azure destination's SAS): RedactArgv hides each as it hides a provider's
// (Factory.Secrets), and a resume must be given it again.
var OutputSecretFlags = []string{"output-azure-sas-token"}

// RedactURL omits URL values that may carry credentials, including malformed
// ones whose parse errors would quote the input.
func RedactURL(value string) string {
	if source.UnsafeURL(value) {
		return redactedValue
	}
	return value
}

// Factories returns the registered factories, by scheme.
func Factories() []Factory {
	regMu.Lock()
	defer regMu.Unlock()
	out := make([]Factory, 0, len(factories))
	for _, f := range factories {
		out = append(out, f)
	}
	slices.SortFunc(out, func(a, b Factory) int { return strings.Compare(a.Scheme, b.Scheme) })
	return out
}

// Canonical is root as its provider takes it: a scheme in lower case
// (S3://b is s3://b), or a provider's root for a source given in another
// form (Factory.FromURL); any other root as given.
func Canonical(root string) string {
	if i := strings.Index(root, "://"); i > 0 {
		root = strings.ToLower(root[:i]) + root[i:]
	}
	for _, f := range Factories() {
		if f.FromURL != nil {
			if r, ok := f.FromURL(root); ok {
				return r
			}
		}
	}
	return root
}

// Lookup returns the factory for root's scheme, in any case.
func Lookup(root string) (Factory, error) {
	if rootCredentials(root) {
		return Factory{}, fmt.Errorf("root %q: user information, a query or a fragment in the authority is refused; use the provider's credential settings", source.RedactRoot(root))
	}
	scheme := ""
	if i := strings.Index(root, "://"); i > 0 {
		scheme = strings.ToLower(root[:i])
	}
	regMu.Lock()
	defer regMu.Unlock()
	f, ok := factories[scheme]
	if !ok {
		// The root is not quoted: no provider can redact it (Redact).
		if scheme == "" {
			return f, errors.New("no provider for local paths is registered")
		}
		return f, fmt.Errorf("unsupported source: no provider for %s://", scheme)
	}
	return f, nil
}
