// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"encoding/json"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Extension is the manifest's extensions namespace swath writes.
const Extension = "swath"

// Metadata is a dataset's manifest: the module's view of it, and what
// swath records under extensions.swath. Every member of the extension is
// a pointer (a slice: nil) that is nil when the manifest does not have it,
// so a missing member is never read as false, zero or empty; deciding
// whether a dataset is acceptable is the caller's.
type Metadata struct {
	// Manifest is the manifest as the SeekParquet module read it.
	Manifest seekparquet.Metadata
	// SchemaID is Manifest.SchemaID.
	SchemaID SchemaID
	// Swath reports whether the manifest has extensions.swath. Without it
	// every member below is nil.
	Swath       bool
	PublishedAt *time.Time
	Source      *SourceInfo
	Scope       *Scope
	Coverage    *Coverage
}

// SourceInfo is what was listed.
type SourceInfo struct {
	Kind      *string `json:"kind"`      // "s3", "gcs", "local-fs"
	URI       *string `json:"uri"`       // s3://bucket/prefix, gs://…, or the absolute root path
	Container *string `json:"container"` // the bucket; absent for a filesystem
}

// Scope is the part of the source the listing was asked for.
type Scope struct {
	Kind   *string `json:"kind"`   // "whole" or "prefix"
	Prefix *string `json:"prefix"` // with "prefix"
}

// Coverage is how much of the scope the dataset holds: all of it when
// Complete is true, StopReason "completed", nothing skipped and no
// filters. Exclusions name what the listing left out by design (an output
// directory inside a listed tree). A listing is not a point-in-time
// snapshot: Snapshot is false, and entries that changed while it ran may
// or may not be in it.
type Coverage struct {
	Complete          *bool      `json:"complete"`
	StopReason        *string    `json:"stop_reason"`
	SkippedEntries    *int64     `json:"skipped_entries"`
	Exclusions        []string   `json:"exclusions"`
	Filters           []string   `json:"filters"`
	Snapshot          *bool      `json:"snapshot"`
	ListingStartedAt  *time.Time `json:"listing_started_at"`
	ListingFinishedAt *time.Time `json:"listing_finished_at"`
}

// ParseMetadata reads m's extensions.swath. A member of another type than
// swath writes (a malformed extension) is a SemanticError of KindCoverage;
// members swath does not define are ignored.
func ParseMetadata(m seekparquet.Metadata) (Metadata, error) {
	out := Metadata{Manifest: m, SchemaID: SchemaID(m.SchemaID)}
	raw, ok := m.Extension(Extension)
	if !ok {
		return out, nil
	}
	var x struct {
		PublishedAt *time.Time  `json:"published_at"`
		Source      *SourceInfo `json:"source"`
		Scope       *Scope      `json:"scope"`
		Coverage    *Coverage   `json:"coverage"`
	}
	if err := json.Unmarshal(raw, &x); err != nil {
		return Metadata{}, &SemanticError{Kind: KindCoverage, Err: err}
	}
	out.Swath, out.PublishedAt, out.Source, out.Scope, out.Coverage = true, x.PublishedAt, x.Source, x.Scope, x.Coverage
	return out, nil
}

// AcceptWholeSource accepts the dataset as a listing of the whole of one
// source: the container of the given kind ("s3", "gcs" or "local-fs"; a
// filesystem has no container, so container is ""). It requires every
// member below to be present, and fails on the first that is absent or
// does not hold, with a SemanticError of KindCoverage whose Column names
// the member: extensions.swath itself, source.kind and source.container
// equal to kind and container, scope.kind "whole", coverage.complete
// true, coverage.stop_reason "completed", coverage.skipped_entries 0 and
// coverage.filters empty. Exclusions are what a listing leaves out by
// design (an output directory inside a listed tree), and a listing is
// never a point-in-time snapshot (coverage.snapshot false), so neither
// refuses a dataset.
func (d *Dataset) AcceptWholeSource(kind, container string) error {
	if err := d.checkSwath(); err != nil {
		return err
	}
	m := d.meta
	var src SourceInfo
	if m.Source != nil {
		src = *m.Source
	}
	var scope Scope
	if m.Scope != nil {
		scope = *m.Scope
	}
	var c Coverage
	if m.Coverage != nil {
		c = *m.Coverage
	}
	switch {
	case src.Kind == nil:
		return missing("source.kind")
	case *src.Kind != kind:
		return semantic(KindCoverage, "source.kind", nil, "%q, not %q", *src.Kind, kind)
	case src.Container == nil && container != "":
		return missing("source.container")
	case src.Container != nil && *src.Container != container:
		return semantic(KindCoverage, "source.container", nil, "%q, not %q", *src.Container, container)
	case scope.Kind == nil:
		return missing("scope.kind")
	case *scope.Kind != "whole":
		return semantic(KindCoverage, "scope.kind", nil, "%q: the listing covers part of the source", *scope.Kind)
	case c.Complete == nil:
		return missing("coverage.complete")
	case !*c.Complete:
		return semantic(KindCoverage, "coverage.complete", nil, "false: the listing did not reach the end of its scope")
	case c.StopReason == nil:
		return missing("coverage.stop_reason")
	case *c.StopReason != "completed":
		return semantic(KindCoverage, "coverage.stop_reason", nil, "%q", *c.StopReason)
	case c.SkippedEntries == nil:
		return missing("coverage.skipped_entries")
	case *c.SkippedEntries != 0:
		return semantic(KindCoverage, "coverage.skipped_entries", nil, "%d entries skipped", *c.SkippedEntries)
	case c.Filters == nil:
		return missing("coverage.filters")
	case len(c.Filters) > 0:
		return semantic(KindCoverage, "coverage.filters", nil, "filtered by %q", c.Filters)
	}
	return nil
}

func missing(member string) error { return semantic(KindCoverage, member, nil, "absent") }
