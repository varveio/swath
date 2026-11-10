// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/reader"
)

// Key is a key's exact bytes, held in a Go string and compared as unsigned
// bytes. JSON spells it in standard base64, so a key that is not UTF-8 (a
// filesystem name) survives a round trip; readers compare decoded bytes.
type Key string

// MarshalJSON writes k as a base64 JSON string.
func (k Key) MarshalJSON() ([]byte, error) {
	return json.Marshal(base64.StdEncoding.EncodeToString([]byte(k)))
}

// UnmarshalJSON reads a base64 JSON string.
func (k *Key) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("key: %w", err)
	}
	*k = Key(v)
	return nil
}

// PartInfo is what is durable about a part, shared by the manifest and the
// journal's recovery records: its rows, size, SHA-256 (lowercase hex, as
// the module's part writer computes it while writing) and exact key
// bounds.
type PartInfo struct {
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	MinKey Key    `json:"min_key_base64"`
	MaxKey Key    `json:"max_key_base64"`
	SHA256 string `json:"sha256"`
}

// Part is a written part at run time: its file and its PartInfo.
type Part struct {
	Path string // temporary path while unpublished, the final path once published
	PartInfo
}

// SegmentPlan is a sink's cut rule for the rows still to come, as the
// order stage cuts the rest into segments with it (order.Segmenter; the
// Parquet router's Plan): a part closes after the row whose logical bytes
// (schema.LogicalBytes) bring it to Limit, or else after MaxRows rows (0:
// no row cap). Logical and Rows are the open part's so far (Open); Full:
// the open part is closed before the next row. Encoders is the encoders
// admitted now; Queue the batches a segment queues to its part.
type SegmentPlan struct {
	Limit, MaxRows int64
	Logical, Rows  int64
	Open, Full     bool
	Encoders       int
	Queue          int
}

// InvariantError is a violated dataset invariant found by the publication
// gate: nothing is published.
type InvariantError struct{ Reason string }

func (e *InvariantError) Error() string { return "dataset invariant violated: " + e.Reason }

// Check is the publication gate: dense ordinals (part i at TempPath i, or
// at its final name once published early), strictly ascending disjoint key
// ranges, and rows written == rows routed.
func Check(parts []Part, rows int64) error {
	var n int64
	for i, p := range parts {
		if filepath.Base(p.Path) != PartName(i) {
			return &InvariantError{fmt.Sprintf("part ordinals not dense: index %d holds %s", i, p.Path)}
		}
		if p.Rows == 0 || p.MinKey > p.MaxKey {
			return &InvariantError{fmt.Sprintf("part %d: empty or inverted key range", i)}
		}
		if i > 0 && !(parts[i-1].MaxKey < p.MinKey) {
			return &InvariantError{fmt.Sprintf("parts %d and %d overlap: %q !< %q", i-1, i, parts[i-1].MaxKey, p.MinKey)}
		}
		n += p.Rows
	}
	if n != rows {
		return &InvariantError{fmt.Sprintf("row count mismatch: routed %d, written %d", rows, n)}
	}
	return nil
}

// Manifest is manifest.json, the dataset's commit record and the only file
// a reader needs to open it: a SeekParquet manifest (the version, the
// schema id, the key declaration and the ordered parts with their exact
// bounds) whose swath extension says what was listed (source, scope), how
// much of it (coverage) and when it was published. Each part carries its
// SHA-256 (the manifest's standard sha256 member).
//
// The fields are flat in Go; the JSON is the module's
// (seekparquet.MarshalManifest, ParseManifest), with swath's fields under
// extensions.swath, whose version is reader.MetadataVersion. UnmarshalJSON
// reads that form.
type Manifest struct {
	Format        string // seekparquet.Format
	FormatVersion int    // seekparquet.FormatVersion
	DraftRevision int    // seekparquet.DraftRevision
	SchemaID      string
	Key           seekparquet.Key // KeyColumn
	// MetadataVersion is extensions.swath.version as read (0 for a
	// manifest without the extension); MarshalJSON always writes
	// reader.MetadataVersion.
	MetadataVersion int
	PublishedAt     time.Time
	Source          Source
	Scope           Scope
	Coverage        Coverage
	Parts           []ManifestPart // in key order; part i is data/part-%05d.parquet
}

// swathExtension is extensions.swath.
type swathExtension struct {
	Version     int       `json:"version"`
	PublishedAt time.Time `json:"published_at"`
	Source      Source    `json:"source"`
	Scope       Scope     `json:"scope"`
	Coverage    Coverage  `json:"coverage"`
}

// MarshalJSON writes m in the SeekParquet form.
func (m Manifest) MarshalJSON() ([]byte, error) {
	x := swathExtension{Version: reader.MetadataVersion, PublishedAt: m.PublishedAt, Source: m.Source, Scope: m.Scope, Coverage: m.Coverage}
	sm := seekparquet.Manifest{Key: m.Key, SchemaID: m.SchemaID, Parts: make([]seekparquet.PartInfo, len(m.Parts)),
		Version: seekparquet.Version{FormatVersion: int64(m.FormatVersion), DraftRevision: int64(m.DraftRevision)}}
	for i, p := range m.Parts {
		sm.Parts[i] = seekparquet.PartInfo{Path: p.Path, Rows: p.Rows, Bytes: p.Bytes, MinKey: []byte(p.MinKey), MaxKey: []byte(p.MaxKey), SHA256: p.SHA256}
	}
	ext, err := json.Marshal(map[string]swathExtension{Extension: x})
	if err != nil {
		return nil, err
	}
	sm.Extensions = ext
	return seekparquet.MarshalManifest(sm)
}

// UnmarshalJSON reads a SeekParquet manifest as the module parses it
// (strictly, its version one this build reads, within ManifestLimits),
// with extensions.swath, when present, at version reader.MetadataVersion.
// Any other manifest, the swath-dataset format 2 of builds from 2026-11-05
// to 2026-11-08 included, is refused.
func (m *Manifest) UnmarshalJSON(b []byte) error {
	sm, err := seekparquet.ParseManifest(b, ManifestLimits)
	if err != nil {
		return err
	}
	var x swathExtension
	if raw, ok := sm.Extension(Extension); ok {
		if err := json.Unmarshal(raw, &x); err != nil {
			return fmt.Errorf("manifest: extensions.%s: %w", Extension, err)
		}
		if x.Version != reader.MetadataVersion {
			return fmt.Errorf("manifest: unsupported swath metadata version %d (this build reads %d)", x.Version, reader.MetadataVersion)
		}
	}
	*m = Manifest{Format: seekparquet.Format, FormatVersion: int(sm.Version.FormatVersion), DraftRevision: int(sm.Version.DraftRevision), SchemaID: sm.SchemaID,
		Key: sm.Key, MetadataVersion: x.Version, PublishedAt: x.PublishedAt, Source: x.Source, Scope: x.Scope, Coverage: x.Coverage, Parts: make([]ManifestPart, len(sm.Parts))}
	for i, p := range sm.Parts {
		m.Parts[i] = ManifestPart{Path: p.Path, PartInfo: PartInfo{Rows: p.Rows, Bytes: p.Bytes, MinKey: Key(p.MinKey), MaxKey: Key(p.MaxKey), SHA256: p.SHA256}}
	}
	return nil
}

// Current reports whether m is in the format this build writes.
func (m *Manifest) Current() bool {
	return m.Format == seekparquet.Format && m.FormatVersion == int(seekparquet.FormatVersion) && m.DraftRevision == int(seekparquet.DraftRevision)
}

// Source is what was listed.
type Source struct {
	Kind      string `json:"kind"`                // "s3", "local-fs"
	URI       string `json:"uri"`                 // s3://bucket/prefix, or the absolute root path
	Container string `json:"container,omitempty"` // the bucket
}

// Scope is the part of the source the listing was asked for.
type Scope struct {
	Kind   string `json:"kind"`             // "whole" (the bucket, the tree) or "prefix"
	Prefix string `json:"prefix,omitempty"` // with "prefix"
}

// Coverage is how much of the scope the dataset holds. A dataset covers its
// scope fully when complete is true, nothing was skipped and there are no
// filters; exclusions name what the listing left out by design (an output
// directory inside a listed tree). A listing is not a point-in-time
// snapshot: entries that changed while it ran may or may not be in it.
type Coverage struct {
	Complete          bool      `json:"complete"`    // the listing reached the end of the scope
	StopReason        string    `json:"stop_reason"` // "completed", or why it stopped early
	SkippedEntries    int64     `json:"skipped_entries"`
	Exclusions        []string  `json:"exclusions"`
	Filters           []string  `json:"filters"`
	Snapshot          bool      `json:"snapshot"` // always false
	ListingStartedAt  time.Time `json:"listing_started_at"`
	ListingFinishedAt time.Time `json:"listing_finished_at"`
}

// ManifestPart is one part in the manifest: its path, relative to the
// dataset root, and its PartInfo (keys exact, never truncated, base64).
type ManifestPart struct {
	Path string `json:"path"`
	PartInfo
}

// Meta is what Publish needs beyond the parts.
type Meta struct {
	SchemaID string
	// Groups: the schema's keys repeat (schema.Info.Groups); the manifest
	// declares them not unique, as the footers do.
	Groups   bool
	Source   Source
	Scope    Scope
	Coverage Coverage
}

// ReportError is a run report that could not be written after the dataset
// was committed: the dataset stands.
type ReportError struct{ Err error }

func (e *ReportError) Error() string { return "dataset committed; run report: " + e.Err.Error() }
func (e *ReportError) Unwrap() error { return e.Err }

// WriteReport writes the run report (ReportName) beside a committed
// dataset. A failure is a *ReportError: the dataset stands.
func WriteReport(out string, v any) error {
	if err := faults.At("run-report"); err != nil { // tests: a run blocked while it reports
		return &ReportError{err}
	}
	if err := writeJSON(out, ReportName, v); err != nil {
		return &ReportError{err}
	}
	return nil
}

// Publish commits parts (already gated): each part not yet published is
// renamed into data/, data/ is synced, and manifest.json is installed by an
// atomic rename and a directory sync. That installation is the commit; the
// run report follows (WriteReport). commit, if set, runs the installation
// (install) as the caller's commit step: the caller accounts the commit in
// the same step, or refuses it (an error without calling install).
func Publish(out string, meta Meta, parts []Part, commit func(install func() error) error) error {
	data := filepath.Join(out, DataDir)
	m := Manifest{Format: seekparquet.Format, FormatVersion: int(seekparquet.FormatVersion), DraftRevision: int(seekparquet.DraftRevision), SchemaID: meta.SchemaID,
		Key: seekparquet.Key{Column: KeyColumn, Unique: !meta.Groups}, Source: meta.Source, Scope: meta.Scope, Coverage: meta.Coverage, Parts: []ManifestPart{}}
	if m.Coverage.Exclusions == nil {
		m.Coverage.Exclusions = []string{}
	}
	if m.Coverage.Filters == nil {
		m.Coverage.Filters = []string{}
	}
	for i, p := range parts {
		if final := filepath.Join(data, PartName(i)); p.Path != final {
			if err := os.Rename(p.Path, final); err != nil {
				return fmt.Errorf("publish part %d: %w", i, err)
			}
		}
		m.Parts = append(m.Parts, ManifestPart{Path: DataDir + "/" + PartName(i), PartInfo: p.PartInfo})
	}
	if err := syncDir(data); err != nil {
		return fmt.Errorf("sync data directory: %w", err)
	}
	if err := faults.At("published"); err != nil {
		return err
	}
	m.PublishedAt = time.Now().UTC()
	install := func() error { return writeJSON(out, ManifestName, m) }
	if commit == nil {
		commit = func(install func() error) error { return install() }
	}
	if err := commit(install); err != nil {
		return err
	}
	return faults.At("manifest")
}

// Committed reports whether out holds a committed dataset (its manifest).
func Committed(out string) bool {
	fi, err := os.Stat(filepath.Join(out, ManifestName))
	return err == nil && fi.Mode().IsRegular()
}

func writeJSON(dir, name string, v any) error {
	return ReplaceJSON(filepath.Join(dir, name), v, Durable)
}

// SyncDir fsyncs a directory, making the entries created or renamed in it
// durable where DirSyncEnforced (a no-op on Windows).
func SyncDir(dir string) error { return syncDir(dir) }

// PublishPart renames one closed part to its final name and syncs the data
// directory: the early publication of a journaled run, after the part's
// journal record is durable. It returns the part with its final path.
func PublishPart(out string, ordinal int, p Part) (Part, error) {
	return PublishPartWith(os.Rename, out, ordinal, p)
}

// PublishPartWith is PublishPart renaming with rename (one relative to the
// directory the caller locked, so a replaced pathname is never renamed
// into).
func PublishPartWith(rename func(old, new string) error, out string, ordinal int, p Part) (Part, error) {
	return publishPart(rename, func() error { return syncDir(filepath.Join(out, DataDir)) }, out, ordinal, p)
}

// PublishPartIn is PublishPart through r, the root of out: the rename and
// the data directory's sync are relative to it (resume recovery).
func PublishPartIn(r *os.Root, out string, ordinal int, p Part) (Part, error) {
	rename := func(old, new string) error {
		o, err1 := filepath.Rel(out, old)
		n, err2 := filepath.Rel(out, new)
		if err := errors.Join(err1, err2); err != nil {
			return err
		}
		return r.Rename(o, n)
	}
	return publishPart(rename, func() error { return SyncDirIn(r, DataDir) }, out, ordinal, p)
}

// publishPart renames p to part ordinal's final name and syncs the data
// directory (sync), with the fault points between.
func publishPart(rename func(old, new string) error, sync func() error, out string, ordinal int, p Part) (Part, error) {
	final := filepath.Join(out, DataDir, PartName(ordinal))
	if err := faults.At("rename"); err != nil {
		return p, err
	}
	if err := rename(p.Path, final); err != nil {
		return p, fmt.Errorf("publish part %d: %w", ordinal, err)
	}
	if err := faults.At("renamed-unsynced"); err != nil { // between the rename and its directory sync
		return p, err
	}
	if err := sync(); err != nil {
		return p, fmt.Errorf("sync data directory: %w", err)
	}
	p.Path = final
	return p, nil
}
