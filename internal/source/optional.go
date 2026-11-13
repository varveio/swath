// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"time"

	"github.com/varveio/swath/internal/runtrace"
)

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

// Traced sources write a run trace (swath diag list --trace,
// docs/internals/trace.md): the range engine's.
type Traced interface {
	// TraceTo is called before Root, after Restart: the source writes its
	// run_start and every later event to w. The caller closes w after the
	// run.
	TraceTo(w *runtrace.Writer)
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

// Excluder sources can contain the run's own output (a local tree): after
// the output is prepared, and before listing starts, the pipeline asks
// them to leave out each path it writes there, one call each: the dataset
// directory, or the text file, its temporary and its scratch directory.
// A directory is left out with everything below it; a path that does not
// exist yet is left out by its name.
type Excluder interface {
	Exclude(path string) error
}

// Live sources count the requests their service answered usably, pages
// without rows included: the watchdog's evidence that a run is alive while
// no row moves (admission waits at a low limit, empty continuing pages). It
// is liveness, not progress; rows stay the progress measure.
type Live interface {
	Answered() int64
}

// Advancing sources count their key-space progress, which the watchdog
// takes in place of Answered: a page that held an entry or ended a chain,
// and an empty continuing page within a bounded run of them
// (keyrange.EmptyChainLive); emptyChained counts the empty pages past
// that bound, which the service answered but which are not progress.
type Advancing interface {
	Frontier() (advanced, emptyChained int64)
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

// Requested sources count their HTTP attempts live: the progress file, the
// terminal display and the report's cost read them (atomics, and the
// limiter's current limit once per sample).
type Requested interface {
	Requests() Requests
}

// Requests is a source's request account at one moment.
type Requests struct {
	Attempts  int64 // HTTP attempts sent: pages, probes, client retries, engine requeues
	Retries   int64 // attempts repeated after a retryable failure
	Throttles int64 // attempts answered with a throttle
	Inflight  int64 // attempts in flight now
	// InflightMax is the most attempts in flight at once; InflightNs sums
	// their time in flight (over a wall time: the mean in flight).
	InflightMax, InflightNs int64
	Limit                   int64 // the adaptive limiter's current limit; 0 without a limiter
	Timeouts                int64 // attempts past the per-request timeout, retried or not
}

// SeedTimed sources time their seed, the delimiter listings that tile the
// scope before any range lists (the report's timings.seed_ms); ok is false
// where none ran.
type SeedTimed interface {
	SeedTime() (d time.Duration, ok bool)
}

// Priced sources name the reference tariff of their list requests, so the
// report can label an estimate; a source on an unknown endpoint (an
// S3-compatible store) is not Priced, and the report gives no figure.
type Priced interface {
	Tariff() (usdPer1k float64, basis string)
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
	// where applications read it.
	Coverage Coverage
	// Filesystem is the space of the filesystem that holds a local root,
	// as the operating system reported it; nil when not measured (a remote
	// source, a platform without a measurement, a failed call). It goes
	// into the manifest.
	Filesystem *FilesystemSpace
}

// FilesystemSpace is a filesystem's space as the operating system reports
// it: what the filesystem says of itself, not a physical disk's capacity
// or a promise of writable bytes.
type FilesystemSpace struct {
	TotalBytes     int64 // the filesystem's size
	FreeBytes      int64 // free, including space reserved for the superuser
	AvailableBytes int64 // free to an unprivileged writer (on Windows, within the caller's quota)
	MeasuredAt     time.Time
}

// Coverage is what a dataset claims to cover.
type Coverage struct {
	Target     string   // the canonical listed target (s3://bucket[/prefix], or the root path)
	Prefix     string   // the scope: "" for the whole source, else the listed prefix
	Filters    []string // the row filters applied (listing.Config.Filter), as descriptions
	Exclusions []string // left out by design (an output directory inside a listed tree)
	Skipped    int64    // entries not listed (unreadable, invalid names)
	Complete   bool     // false if the listing stopped early (a key limit)
	StopReason string   // "completed", or why it stopped
}
