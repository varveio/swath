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
