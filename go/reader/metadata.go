// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
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
	Kind      *string `json:"kind"`      // "s3", "gcs", "azblob", "local-fs"
	URI       *string `json:"uri"`       // s3://bucket/prefix, gs://…, az://account/container/…, or the absolute root path
	Container *string `json:"container"` // the bucket or container name; absent for a filesystem
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

// ParseMetadata reads m's extensions.swath. Its version must be the
// integer MetadataVersion, else (absent included) a SemanticError of
// KindSchema: "unsupported swath metadata version". A member of another
// type than swath writes (a malformed extension) is a SemanticError of
// KindCoverage; members swath does not define are ignored.
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
		return Metadata{}, &SemanticError{Kind: KindCoverage, Err: err}
	}
	if string(v.Version) != strconv.Itoa(MetadataVersion) {
		got := string(v.Version)
		if got == "" {
			got = "absent"
		}
		return Metadata{}, semantic(KindSchema, "", nil, "unsupported swath metadata version: extensions.swath.version %s, this reader reads %d", got, MetadataVersion)
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

// WholeSource is the source a whole-source listing must be of: its kind
// ("s3", "gcs", "azblob" or "local-fs"), its container (the bucket, or
// the Azure container's name; "" for a filesystem) and its URI as swath
// records it in source.uri (an Azure container's az://account/container,
// a filesystem's absolute root path).
type WholeSource struct {
	Kind, Container, URI string
}

// sourceKinds is the source kind of each schema id swath writes.
var sourceKinds = map[SchemaID]string{
	S3ObjectsV1: "s3", S3VersionsV1: "s3", GCSObjectsV1: "gcs", GCSGenerationsV1: "gcs",
	AzblobObjectsV1: "azblob", LocalV1: "local-fs",
}

// AcceptWholeSource accepts the dataset as a listing of the whole of the
// source want names. It requires every member below to be present, and
// fails on the first that is absent or does not hold, with a
// SemanticError of KindCoverage whose Column names the member:
// extensions.swath itself; source.kind equal to want.Kind, and the kind
// of the dataset's schema id; source.container and source.uri as the
// kind identifies a source; scope.kind "whole"; coverage.complete true;
// coverage.stop_reason "completed"; coverage.skipped_entries 0; and
// coverage.filters empty.
//
// A bucket ("s3", "gcs") is its container, equal to want.Container (a
// container absent from the manifest matches only ""); its URI is compared
// only when want.URI is not "". An Azure container's name is unique only
// within its account, which source.uri holds: "azblob" requires both
// source.container and source.uri equal to want's, and want.URI "" is
// refused. A filesystem ("local-fs") has no container, so want.Container
// must be "" (and the manifest's absent or ""), and is its root: source.uri
// equal to want.URI, neither of them "", lexically, except that a
// trailing separator on either is ignored. No path is resolved or
// case-folded.
//
// Exclusions are what a listing leaves out by design (an output directory
// inside a listed tree), and a listing is never a point-in-time snapshot
// (coverage.snapshot false), so neither refuses a dataset.
func (d *Dataset) AcceptWholeSource(want WholeSource) error {
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
	if err := acceptSource(src, want, m.SchemaID); err != nil {
		return err
	}
	switch {
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

// acceptSource checks that src is the source want names, by the members
// that identify a source of its kind.
func acceptSource(src SourceInfo, want WholeSource, id SchemaID) error {
	switch {
	case src.Kind == nil:
		return missing("source.kind")
	case *src.Kind != want.Kind:
		return semantic(KindCoverage, "source.kind", nil, "%q, not %q", *src.Kind, want.Kind)
	case sourceKinds[id] != want.Kind:
		return semantic(KindCoverage, "source.kind", nil, "%q, but %s is a listing of %q", want.Kind, id, sourceKinds[id])
	case want.Kind == "local-fs" && want.Container != "":
		return semantic(KindCoverage, "source.container", nil, "%q: a filesystem has no container", shownName(want.Container))
	case src.Container == nil && want.Container != "":
		return missing("source.container")
	case src.Container != nil && *src.Container != want.Container:
		return semantic(KindCoverage, "source.container", nil, "%q, not %q", shownName(*src.Container), shownName(want.Container))
	}
	switch want.Kind {
	case "s3", "gcs":
		if want.URI == "" {
			return nil
		}
	case "azblob", "local-fs":
		if want.URI == "" {
			return semantic(KindCoverage, "source.uri", nil, "a %s source is identified by its URI, and none was given", want.Kind)
		}
	}
	switch {
	case src.URI == nil:
		return missing("source.uri")
	case *src.URI == "":
		return semantic(KindCoverage, "source.uri", nil, "empty: the manifest does not identify its source")
	case want.Kind == "local-fs" && trimSeparator(*src.URI) == trimSeparator(want.URI):
	case want.Kind != "local-fs" && *src.URI == want.URI:
	case want.Kind == "local-fs": // a path: @, ? and # are name characters
		return semantic(KindCoverage, "source.uri", nil, "%q, not %q", *src.URI, want.URI)
	default:
		return semantic(KindCoverage, "source.uri", nil, "%q, not %q", shown(*src.URI), shown(want.URI))
	}
	return nil
}

// trimSeparator removes one trailing path separator from a root path.
func trimSeparator(root string) string {
	if n := len(root); n > 0 && (root[n-1] == '/' || root[n-1] == filepath.Separator) {
		return root[:n-1]
	}
	return root
}

func missing(member string) error { return semantic(KindCoverage, member, nil, "absent") }

// shown keeps a clean container or object-store URI in a diagnostic and
// hides one that holds user information, a query or a fragment where the
// bucket, account or container belongs: a mistyped root can carry a
// credential there. A URI's prefix is a literal key and is shown as it is.
func shown(v string) string {
	if scheme, rest, ok := strings.Cut(v, "://"); ok {
		if authority, _, _ := strings.Cut(rest, "/"); strings.ContainsAny(authority, "@?#") {
			return scheme + "://[redacted]"
		}
		// An S3 or GCS root's '?' is key data; any other scheme's query (an
		// Azure root's is where its shared access signature goes) is hidden.
		if i := strings.IndexAny(v, "?#"); i >= 0 && scheme != "s3" && scheme != "gs" {
			return v[:i+1] + "[redacted]"
		}
		return v
	}
	if strings.ContainsAny(v, "@?#") {
		return "[redacted]"
	}
	return v
}

// shownName keeps a clean bucket, account or container name and hides one
// holding user information, a query or a fragment anywhere: a name is never
// a URI, so nothing in it is key data.
func shownName(v string) string {
	if strings.ContainsAny(v, "@?#") {
		return "[redacted]"
	}
	return v
}
