// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import "time"

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
// The journal keeps committed output only, no scheduling history: the
// source starts up as on a fresh run, over the keys after.
type Resumable interface {
	// Restart is called before Root: every row the source emits must sort
	// after key after.
	Restart(after string) error
}

// Capable sources say which optional behaviours this instance supports;
// an instance may decline what its type implements.
type Capable interface {
	Capabilities() Capabilities
}

// Capabilities of an opened source.
type Capabilities struct {
	Resume bool // journal the run so that swath resume can continue it (needs Resumable)
	// Seek is how a listing can start at a key: the parallel range engine
	// applies only to a source that has no rank oracle and a fabricable
	// seek (see docs/internals/providers.md, "Does the range engine apply").
	Seek Seek
	// RankOracle: the source can say how many keys precede a key (an
	// index); it can be partitioned exactly, so dynamic splitting is not
	// the right tool.
	RankOracle bool
	// Order is the order the native listing returns keys in.
	Order Order
}

// Order is the order of a source's native listing.
type Order string

const (
	// OrderBytes: every page ascends in unsigned byte order and each page
	// continues after the last (S3).
	OrderBytes Order = "bytes"
	// OrderOther: sorted, but not in byte order (UTF-16 code-unit order
	// puts U+10000 before U+E000, the reverse of byte order), so the
	// disorder can cross pages; the provider normalizes.
	OrderOther Order = "other"
	// OrderNone: unsorted (a directory read, Drive); the provider sorts.
	OrderNone Order = "none"
)

// Seek is how a listing can start at a key.
type Seek string

const (
	// SeekFabricable: a listing can start after any key the caller makes
	// up (S3 start-after), so a range can be split at a synthesized pivot.
	SeekFabricable Seek = "fabricable"
	// SeekTokenOnly: only an opaque continuation token resumes a listing;
	// the source is listed as one sequential chain per root.
	SeekTokenOnly Seek = "token_only"
	// SeekNone: no seek at all; a range (a directory) is read whole and
	// sorted inside the provider.
	SeekNone Seek = "none"
)

// RangeEngine reports whether dynamic range splitting applies: no rank
// oracle, a fabricable seek and byte-ordered pages, all three. A provider
// that sorts its output itself (OrderOther, OrderNone) does not qualify.
func (c Capabilities) RangeEngine() bool {
	return !c.RankOracle && c.Seek == SeekFabricable && c.Order == OrderBytes
}

// Excluder sources can contain the output directory (a local tree); after
// the output is prepared, the pipeline asks them to leave it out.
type Excluder interface {
	Exclude(out string) error
}

// Live sources count the requests their service answered usably, pages
// without rows included: the watchdog's evidence that a run is alive while
// no row moves (admission waits at a low limit, empty continuing pages). It
// is liveness, not progress; rows stay the progress measure.
type Live interface {
	Answered() int64
}

// Faulted sources count the throttle answers and the attempts past the
// request timeout their service gave (retried or not): the watchdog names
// a stuck run's cause from what rose while nothing progressed.
type Faulted interface {
	Faults() (throttles, timeouts int64)
}

// Contended sources time their engine's decision mutex: the goroutines'
// total wait for it and its longest hold (progress.json; the scale gate).
type Contended interface {
	Contention() (wait, maxHold time.Duration)
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
