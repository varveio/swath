// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"encoding/json"
	"strconv"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"
)

// Extension is the manifest's extensions namespace swath writes.
const Extension = "swath"

// MetadataVersion is the version of extensions.swath's own contract that
// swath writes (its "version" member) and ParseMetadata reads. A change to
// a schema's rows takes a new schema id instead.
const MetadataVersion = 1

// Metadata is a dataset's manifest: the module's view of it, and what
// swath records under extensions.swath. Every member of the extension is
// a pointer (a slice: nil) that is nil when the manifest does not have it,
// so a missing member is never read as false, zero or empty. The members
// record how the listing ran; what an application does with a partial,
// filtered or unpublished listing is its own decision.
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
	// CommittedObjectBytes is extensions.swath.committed_object_bytes: the
	// dataset's object sizes summed (every row's size column, a local
	// tree's files only), as its writers counted them. Nil when the
	// manifest does not state it (a schema without a size column, a dataset
	// written before the member existed or resumed from parts that predate
	// it): never read as 0, which is a real total.
	CommittedObjectBytes *int64
}

// SourceInfo is what was listed.
type SourceInfo struct {
	Kind      *string `json:"kind"`      // "s3", "gcs", "azblob", "local-fs"
	URI       *string `json:"uri"`       // s3://bucket/prefix, gs://…, az://account/container/…, or the absolute root path
	Container *string `json:"container"` // the bucket or container name; absent for a filesystem
}

// Scope is the part of the source the listing was asked for.
type Scope struct {
	Kind   *string `json:"kind"`   // "whole" or "prefix"
	Prefix *string `json:"prefix"` // with "prefix"
}

// Coverage records how much of the scope the listing reached: whether it
// completed, why it stopped, how many entries it skipped, and the filters
// that limited it (a filesystem listing kept to one filesystem records
// "one_filesystem"). Exclusions name what the listing left out by design
// (an output directory inside a listed tree). A listing is not a
// point-in-time snapshot: Snapshot is false, and entries that changed
// while it ran may or may not be in it.
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

// ParseMetadata reads m's extensions.swath. Its version must be the
// integer MetadataVersion, else (absent included) a SemanticError of
// KindSchema: "unsupported swath metadata version". A member of another
// type than swath writes (a malformed extension) is a SemanticError of
// KindMetadata; members swath does not define are ignored.
func ParseMetadata(m seekparquet.Metadata) (Metadata, error) {
	out := Metadata{Manifest: m, SchemaID: SchemaID(m.SchemaID)}
	raw, ok := m.Extension(Extension)
	if !ok {
		return out, nil
	}
	var v struct {
		Version json.RawMessage `json:"version"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return Metadata{}, &SemanticError{Kind: KindMetadata, Err: err}
	}
	if string(v.Version) != strconv.Itoa(MetadataVersion) {
		got := string(v.Version)
		if got == "" {
			got = "absent"
		}
		return Metadata{}, semantic(KindSchema, "", nil, "unsupported swath metadata version: extensions.swath.version %s, this reader reads %d", got, MetadataVersion)
	}
	var x struct {
		PublishedAt          *time.Time  `json:"published_at"`
		Source               *SourceInfo `json:"source"`
		Scope                *Scope      `json:"scope"`
		Coverage             *Coverage   `json:"coverage"`
		CommittedObjectBytes *int64      `json:"committed_object_bytes"`
	}
	if err := json.Unmarshal(raw, &x); err != nil {
		return Metadata{}, &SemanticError{Kind: KindMetadata, Err: err}
	}
	out.Swath, out.PublishedAt, out.Source, out.Scope, out.Coverage, out.CommittedObjectBytes = true, x.PublishedAt, x.Source, x.Scope, x.Coverage, x.CommittedObjectBytes
	return out, nil
}
