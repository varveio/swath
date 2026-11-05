// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package source

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
