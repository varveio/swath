// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package store is the replay server's serving seam: an ordered,
// byte-range reader over a sorted listing fixture.
//
// A Store speaks ranges, never provider pagination: Rows returns the first
// limit object rows whose key falls in [from, toExclusive) in ascending
// unsigned byte order. Max-keys accounting, truncation, delimiter rollup,
// common-prefix resume and continuation tokens live in the route's pager
// above this seam, so every route plugs into the same store.
//
// DelimitedRollup is the one opt-in exception: a native skip-scan for the
// "/" delimiter that answers a whole rollup without one store round-trip
// per common prefix. It must return entries byte-identical to the pager's
// range walk over Rows; a store may decline (ok=false) and the pager walks
// ranges instead.
package store

import (
	"context"
	"runtime"
	"time"
)

// Store is an ordered reader over one immutable fixture. Implementations
// are safe for concurrent use.
type Store interface {
	// Rows returns up to limit rows with key in [from, toExclusive) in
	// ascending unsigned byte order. A nil from means an open lower bound
	// (fromInclusive is then ignored); a nil toExclusive means an open
	// upper bound. Rows applies no prefix, delimiter or start-after
	// semantics: only the bounds, the limit and the projection.
	Rows(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, limit int, proj Projection) ([]Object, error)

	// DelimitedRollup is the fast path for a delimiter listing over
	// [from, toExclusive) scoped to prefix: the rolled-up common prefixes
	// and the objects directly under prefix, interleaved in ascending key
	// order, up to limit+1 entries so the caller can detect truncation
	// from the extra one. A nil or empty prefix is a root rollup.
	//
	// suppressPrefixAtOrBeforeFloor selects the floor rule: true (S3)
	// suppresses a common prefix that sorts at or before from, because the
	// client has already been handed it; false (GCS, whose inclusive start
	// offset may land inside a prefix) still emits it.
	//
	// ok=false means the store declines (for example a delimiter other
	// than "/"); the caller then walks ranges through Rows.
	DelimitedRollup(ctx context.Context, from []byte, fromInclusive bool, toExclusive []byte, prefix, delimiter []byte, limit int, proj Projection, suppressPrefixAtOrBeforeFloor bool) ([]DelimitedEntry, bool, error)

	// Close releases the store's file handles. Calls after Close fail.
	Close() error
}

// Object is one listed object row. The nullable fields are nil when the
// fixture stores a null or lacks the column; rendering policy (a null size
// as 0, a null time as the epoch) belongs to the routes.
type Object struct {
	Key               []byte
	Size              *int64
	LastModified      *time.Time
	ETag              *string
	StorageClass      *string
	OwnerID           *string
	OwnerDisplayName  *string
	ChecksumAlgorithm *string
	ChecksumType      *string
	// ContentType and MD5 are a GCS recording's content type and MD5
	// (base64, as stored), read only with Projection.Content.
	ContentType *string
	MD5         *string
}

// Projection selects the columns a read materialises. The owner columns are
// only needed for fetch-owner listings, so a read without Owner skips
// decoding them and leaves OwnerID and OwnerDisplayName nil.
type Projection struct {
	Owner bool
	// Content reads ContentType and MD5 (the Azure route's Content-Type
	// and Content-MD5). A schema without those columns ignores it, so its
	// reads share the window cache with reads without it.
	Content bool
	// Created reads LastModified as the object's creation time where the
	// schema stores one: a GCS recording's time_created, which Cloud
	// Storage's S3-compatible XML API lists as LastModified. A row whose
	// creation time is null (a part written before the column was listed)
	// keeps its modification time. A schema without the column ignores it,
	// as Content.
	Created bool
}

// DelimitedEntry is one entry of a DelimitedRollup: exactly one of
// CommonPrefix (bytes ending in the delimiter) and Object is set.
type DelimitedEntry struct {
	CommonPrefix []byte
	Object       *Object
}

// Kind names the on-disk layout a fixture was opened as.
type Kind string

const (
	// KindJavaEra is a directory of footer-stamped sorted parts (or one
	// such file) written by the Java sort-fixture command.
	KindJavaEra Kind = "java-era"
	// KindFormat2 is a manifest-described sorted dataset (format 2),
	// written by earlier builds.
	KindFormat2 Kind = "format-2"
	// KindSeekParquet is a SeekParquet dataset, the format this build
	// writes, read through the seekparquet module.
	KindSeekParquet Kind = "seekparquet"
)

// OrderingProfile is the key order every Store serves.
const OrderingProfile = "unsigned-utf8-byte-order"

// Options configures Open.
type Options struct {
	// Readers bounds concurrent page decodes across all requests. Zero
	// selects DefaultReaders().
	Readers int
	// MaxOpenFiles bounds the descriptors the fixture's parts hold open,
	// whatever their number: an evicted part is reopened when next read.
	// Zero selects legacy.DefaultMaxOpenFiles(), half the soft
	// descriptor limit, at most 4,096.
	MaxOpenFiles int
	// Prefetch configures the window cache in front of Rows; the zero
	// value is on with the defaults.
	Prefetch Prefetch
}

// Metadata describes an opened fixture for the startup line and metrics.
type Metadata struct {
	Kind      Kind
	Parts     int
	RowGroups int
	Rows      int64
	// Identity is a stable hex SHA-256 restart identity over the resolved
	// part names, sizes, modification times and stamps (not a content
	// digest; fixtures stay immutable while served).
	Identity string
	// Ordering is always OrderingProfile.
	Ordering string
	// SchemaID is the declared schema id: the manifest's for a format-2
	// dataset, "swath/canonical" for a Java-era fixture. Projections are
	// chosen by it, never by the columns present.
	SchemaID string
}

// DefaultReaders is the default concurrent page-decode bound:
// max(8, min(32, GOMAXPROCS)). Eight keeps a small host able to overlap a
// handful of requests; 32 caps memory held by in-flight page buffers.
func DefaultReaders() int {
	return max(8, min(32, runtime.GOMAXPROCS(0)))
}

// Open opens the fixture at path (a format-2 dataset directory, a directory
// of Java-era sorted parts, or one such part) and builds its routing index.
// Fixtures that cannot be served sorted are refused with an error that
// names the reason.
func Open(ctx context.Context, path string, opts Options) (Store, Metadata, error) {
	return open(ctx, path, opts)
}
