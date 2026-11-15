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
	// Collected is extensions.swath.collected.
	Collected *Collected
	// CommittedObjectBytes is extensions.swath.committed_object_bytes: the
	// dataset's object sizes summed (every row's size column, a local
	// tree's files only), as its writers counted them. Nil when the
	// manifest does not state it (a schema without a size column, a
	// listing that did not read every object's size: --metadata names):
	// never read as 0, which is a real total.
	CommittedObjectBytes *int64
	// Filesystem is extensions.swath.filesystem: the space of the
	// filesystem that holds a local source's root, as its operating system
	// reported it when the listing ended. Nil for a remote source, and when
	// the listing did not measure it (a platform without a measurement, a
	// failed call, a filesystem that reports no size).
	Filesystem *FilesystemSpace
}

// FilesystemSpace is a filesystem's space as its operating system reported
// it: what the filesystem says of itself, not a physical disk's capacity
// or a promise of writable bytes. Volumes that share a pool (an APFS
// container, a ZFS pool) each report the pool's free space, so it is not
// summed across datasets; nor is the dataset's size total minus free (the
// listing's scope, hard links, shared blocks, snapshots and the
// filesystem's own overhead all differ).
type FilesystemSpace struct {
	// TotalBytes is the filesystem's size; on Windows, within the caller's
	// quota.
	TotalBytes *int64 `json:"total_bytes"`
	// FreeBytes is its free space, including space reserved for the
	// superuser; on Windows, the volume's, which can exceed a quota's
	// TotalBytes.
	FreeBytes *int64 `json:"free_bytes"`
	// AvailableBytes is the free space an unprivileged writer may use; on
	// Windows, within the caller's quota.
	AvailableBytes *int64     `json:"available_bytes"`
	MeasuredAt     *time.Time `json:"measured_at"`
}

// SourceInfo is what was listed.
type SourceInfo struct {
	Kind      *string `json:"kind"`      // "s3", "gcs", "azure", "local"
	URI       *string `json:"uri"`       // s3://bucket/prefix, gs://…, az://account/container/…, or the absolute root path
	Container *string `json:"container"` // the bucket or container name; absent for a filesystem
}

// Scope is the part of the source the listing was asked for.
type Scope struct {
	Kind   *string `json:"kind"`   // "whole" or "prefix"
	Prefix *string `json:"prefix"` // with "prefix"
}

// Coverage records how much of the scope the listing reached: whether it is
// complete (nothing missing: the end of the scope reached and no entry
// unreadable), why it stopped, how many entries it could not read, and the
// filters that limited it (a filesystem listing kept to one filesystem records
// one_file_system). Exclusions name what the listing left out by design
// (an output directory inside a listed tree). A listing is not a
// point-in-time snapshot: entries that changed while it ran may or may
// not be in it.
type Coverage struct {
	Complete          *bool      `json:"complete"`
	StopReason        *string    `json:"stop_reason"`
	Unreadable        *int64     `json:"unreadable"`
	Exclusions        []string   `json:"exclusions"`
	Filters           []Filter   `json:"filters"`
	ListingStartedAt  *time.Time `json:"listing_started_at"`
	ListingFinishedAt *time.Time `json:"listing_finished_at"`
}

// Filter is one filter that limited a listing: the option's name in snake
// case (modified_since for --modified-since) and its value, nil for an
// option that takes none (one_file_system, no_dirs). A time is RFC 3339
// in UTC with six fraction digits; a pattern is one filter each.
type Filter struct {
	Filter string  `json:"filter"`
	Value  *string `json:"value"`
}

// String is the filter as name=value, or its name without a value.
func (f Filter) String() string {
	if f.Value == nil {
		return f.Filter
	}
	return f.Filter + "=" + *f.Value
}

// Collected is what a listing read beyond the columns every row of its
// schema carries: its metadata tier (names, stat or statx; nil for a
// source without tiers) and the optional columns it asked for (an S3
// listing's owner_id, a local tree's link_target). A nullable column not
// named here and outside the tier is null because the source did not
// report it, not because the listing did not ask.
type Collected struct {
	Metadata *string  `json:"metadata"`
	Columns  []string `json:"columns"`
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
		PublishedAt          *time.Time       `json:"published_at"`
		Source               *SourceInfo      `json:"source"`
		Scope                *Scope           `json:"scope"`
		Coverage             *Coverage        `json:"coverage"`
		Collected            *Collected       `json:"collected"`
		CommittedObjectBytes *int64           `json:"committed_object_bytes"`
		Filesystem           *FilesystemSpace `json:"filesystem"`
	}
	if err := json.Unmarshal(raw, &x); err != nil {
		return Metadata{}, &SemanticError{Kind: KindMetadata, Err: err}
	}
	out.Swath, out.PublishedAt, out.Source, out.Scope, out.Coverage, out.CommittedObjectBytes = true, x.PublishedAt, x.Source, x.Scope, x.Coverage, x.CommittedObjectBytes
	out.Collected, out.Filesystem = x.Collected, x.Filesystem
	return out, nil
}
