// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/varveio/swath/go/internal/schema"
)

// A Factory opens one kind of source. Providers register a factory from
// an init function; the command imports them only for that. Nothing in the
// pipeline (listing, order, parquet, dataset, schema, durable) imports a
// provider: it sees only what this package declares.
type Factory struct {
	// Scheme is the root's URL scheme ("s3" for s3://bucket/prefix). The
	// factory with the empty scheme takes roots without one (local paths).
	Scheme string
	// Flags registers the provider's flags on fs and returns its options
	// value, which the flags fill when fs is parsed. The value is opaque to
	// everything but the provider: it travels in Request.Options.
	Flags func(fs *flag.FlagSet) any
	// Defaults returns the options value used when none was given.
	Defaults func() any
	// Open validates the root and returns a source ready to list. It must
	// not touch the output directory.
	Open func(ctx context.Context, req Request) (Source, error)
}

// Request is what the pipeline tells a factory.
type Request struct {
	Root    string       // as given, scheme included
	Options any          // the provider's options value (Flags or Defaults)
	Workers int          // the order stage's worker count
	Warn    func(string) // one-time warnings for the user; never nil
}

// Source is an opened provider with its range type erased (see Erase).
type Source interface {
	Provider[any]
	Schema() *schema.Schema
	Locator() Locator
	// Listed is the number of rows emitted so far; after a listing it
	// must equal the rows the order stage handed on.
	Listed() int64
}

// Locator says what a source lists, for the dataset's metadata.
type Locator struct {
	Kind      string // the dataset's source kind ("s3", "local-fs")
	Root      string // the root as recorded (an absolute path, or the URL)
	Container string // the bucket or account container; "" if none
	// Path is the root's local filesystem path, "" for a remote source:
	// the output must not be the root or inside it, unless excluded.
	Path string
}

// Identified sources name what must not change between a run and its
// resume, beyond what the pipeline itself freezes (schema, writer, part
// size, key cap).
type Identified interface {
	Identity() Identity
}

// Identity is a provider's part of the resume identity.
type Identity struct {
	Provider string
	Fields   []IdentityField // in the order a mismatch is reported
}

// IdentityField is one frozen setting. Two values are the same setting if
// their Canonical forms are equal (Value when Canonical is empty); Value is
// what the user is shown.
type IdentityField struct {
	Name, Value, Canonical string
}

// Resumable sources restart after a journaled run's last published key.
// The pipeline journals a checkpoint with every part; the descriptors are
// opaque to it, and versioned by the provider.
type Resumable interface {
	// Checkpoint returns what the provider learned since the previous
	// call that a restart can use (for S3, the range boundaries), or nil.
	Checkpoint() []byte
	// Restart is called before Root: every row the source emits must sort
	// after key after. checkpoints are the journaled descriptors, oldest
	// first.
	Restart(after string, checkpoints [][]byte) error
}

// Capable sources say which optional behaviours this instance supports;
// an instance may decline what its type implements.
type Capable interface {
	Capabilities() Capabilities
}

// Capabilities of an opened source.
type Capabilities struct {
	Resume bool // journal the run so that -resume can continue it (needs Resumable)
}

// Excluder sources can contain the output directory (a local tree); after
// the output is prepared, the pipeline asks them to leave it out.
type Excluder interface {
	Exclude(out string) error
}

// Reporter sources describe the listing for the run summary.
type Reporter interface {
	Report() Report
}

// Report is a provider's account of a listing.
type Report struct {
	Name   string // the summary key its Stats go under ("s3", "listing")
	Stats  any    // marshalled as JSON
	Brief  string // one line of text for the plain-text summary
	Errors int64  // per-entry problems counted (not fatal)
	Fields string // the metadata tier collected, if the provider has tiers
	// Coverage is what the dataset covers; it goes into the manifest,
	// where readers accept or refuse the dataset.
	Coverage Coverage
	// Activity, if set, makes the dataset carry swath's run summary (a
	// remote listing's engine activity).
	Activity *Activity
}

// Coverage is what a dataset claims to cover.
type Coverage struct {
	Target     string   // the canonical listed target (s3://bucket[/prefix], or the root path)
	Prefix     string   // the scope: "" for the whole source, else the listed prefix
	Filters    []string // none today
	Exclusions []string // left out by design (an output directory inside a listed tree)
	Skipped    int64    // entries not listed (unreadable, invalid names)
	Complete   bool     // false if the listing stopped early (a key limit)
	StopReason string   // "completed", or why it stopped
}

// Activity is a remote listing's engine activity, for swath's run summary.
type Activity struct {
	Items           int64 // objects listed
	Requests, Pages int64
	PeakInFlight    int
	AvgInFlight     float64
	Splits          int64
	Region          string
	Unsigned        bool
	Flags           map[string]bool  // engine flags in effect
	RecoveredErrors map[string]int64 // by summary name
	SeedMode        string           // "shallow" or "none"
	SeedCuts        int64
	SeedRanges      int64
}

var (
	regMu     sync.Mutex
	factories = map[string]Factory{}
)

// Register adds a factory; registering a scheme twice panics.
func Register(f Factory) {
	regMu.Lock()
	defer regMu.Unlock()
	if _, ok := factories[f.Scheme]; ok {
		panic("source: scheme registered twice: " + f.Scheme)
	}
	factories[f.Scheme] = f
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

// Lookup returns the factory for root's scheme.
func Lookup(root string) (Factory, error) {
	scheme := ""
	if i := strings.Index(root, "://"); i > 0 {
		scheme = root[:i]
	}
	regMu.Lock()
	defer regMu.Unlock()
	f, ok := factories[scheme]
	if !ok {
		if scheme == "" {
			return f, fmt.Errorf("no provider for local paths is registered (root %q)", root)
		}
		return f, fmt.Errorf("unsupported root %q: no provider for %s://", root, scheme)
	}
	return f, nil
}
