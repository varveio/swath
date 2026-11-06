// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"github.com/varveio/swath/go/internal/faults"

	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
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
// journal's recovery records: its rows, size, digest and exact key bounds.
type PartInfo struct {
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	MinKey Key    `json:"min_key_base64"`
	MaxKey Key    `json:"max_key_base64"`
	MD5    string `json:"md5"`
}

// Part is a written part at run time: its file and its PartInfo.
type Part struct {
	Path string // temporary path while unpublished, the final path once published
	PartInfo
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
// a reader needs to accept it: what was listed (source, scope), how much of
// it (coverage), and the ordered parts with their exact bounds.
type Manifest struct {
	Format        string         `json:"format"`         // Format
	FormatVersion int            `json:"format_version"` // FormatVersion
	SchemaID      string         `json:"schema_id"`
	PublishedAt   time.Time      `json:"published_at"`
	Source        Source         `json:"source"`
	Scope         Scope          `json:"scope"`
	Coverage      Coverage       `json:"coverage"`
	Parts         []ManifestPart `json:"parts"` // in key order; part i is data/part-%05d.parquet
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
	m := Manifest{Format: Format, FormatVersion: FormatVersion, SchemaID: meta.SchemaID,
		Source: meta.Source, Scope: meta.Scope, Coverage: meta.Coverage, Parts: []ManifestPart{}}
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
	if err := syncDir(filepath.Join(out, DataDir)); err != nil {
		return p, fmt.Errorf("sync data directory: %w", err)
	}
	p.Path = final
	return p, nil
}
