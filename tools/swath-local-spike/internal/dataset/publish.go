// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Part is the close-gated metadata of one written part.
type Part struct {
	Path   string // temporary path while unpublished
	Rows   int64
	Bytes  int64
	MD5    string
	MinKey string
	MaxKey string
}

// Check is the publication gate: dense ordinals (part i at TempPath i),
// strictly ascending disjoint key ranges, and rows written == rows routed.
func Check(parts []Part, rows int64) error {
	var n int64
	for i, p := range parts {
		if filepath.Base(p.Path) != PartName(i)+".tmp" {
			return fmt.Errorf("part ordinals not dense: index %d holds %s", i, p.Path)
		}
		if p.Rows == 0 || p.MinKey > p.MaxKey {
			return fmt.Errorf("part %d: empty or inverted key range", i)
		}
		if i > 0 && !(parts[i-1].MaxKey < p.MinKey) {
			return fmt.Errorf("parts %d and %d overlap: %q !< %q", i-1, i, parts[i-1].MaxKey, p.MinKey)
		}
		n += p.Rows
	}
	if n != rows {
		return fmt.Errorf("row count mismatch: routed %d, written %d", rows, n)
	}
	return nil
}

// Meta describes the dataset in the manifest.
type Meta struct {
	SchemaID     string
	FileSchema   string // parquet-mr message string
	SortKey      string
	SourceKind   string // "local-fs" or "s3"
	SourceBucket string // object-store bucket; empty for a filesystem
	SourceRoot   string // absolute path or s3://bucket/prefix
	// SwathSummary, if set, is written as _swath_summary.json: the subset of
	// Swath's run summary (schema_version 2) that dataset readers check.
	SwathSummary any
}

// SwathSummaryName is Swath's run-summary file.
const SwathSummaryName = "_swath_summary.json"

// RunSummary is the subset of Swath's schema_version 2 run summary that
// readers check before trusting a dataset to cover its target.
type RunSummary struct {
	SchemaVersion int  `json:"schema_version"`
	Completed     bool `json:"completed"`
	Config        struct {
		Target  string   `json:"target"`
		Filters []string `json:"filters"`
	} `json:"config"`
}

type manifestFile struct {
	Key         string `json:"key"`
	Size        int64  `json:"size"`
	MD5checksum string `json:"MD5checksum"`
	RowCount    int64  `json:"rowCount"`
	MinKey      string `json:"minKey"`
	MaxKey      string `json:"maxKey"`
}

// manifest mirrors io.varve.swath.output.parquet.Manifest (field order kept),
// with additive local fields after files.
type manifest struct {
	SourceBucket      string         `json:"sourceBucket"`
	Version           string         `json:"version"`
	CreationTimestamp int64          `json:"creationTimestamp"`
	FileFormat        string         `json:"fileFormat"`
	FileSchema        string         `json:"fileSchema"`
	Sorted            bool           `json:"sorted"`
	SortKey           string         `json:"sortKey"`
	Files             []manifestFile `json:"files"`
	SchemaID          string         `json:"schemaId"`
	SourceKind        string         `json:"sourceKind"`
	SourceRoot        string         `json:"sourceRoot"`
	TotalRows         int64          `json:"totalRows"`
}

// Publish commits parts (already gated) in order: rename each part, fsync
// the data directory, then manifest.json, symlink.txt and the summary, each
// atomically, and _SUCCESS last.
func Publish(out string, meta Meta, parts []Part, summary any) error {
	data := filepath.Join(out, DataDir)
	m := manifest{
		Version: "1", CreationTimestamp: time.Now().UnixMilli(), FileFormat: "Parquet",
		FileSchema: meta.FileSchema, Sorted: true, SortKey: meta.SortKey, Files: []manifestFile{},
		SourceBucket: meta.SourceBucket, SchemaID: meta.SchemaID, SourceKind: meta.SourceKind, SourceRoot: meta.SourceRoot,
	}
	var links strings.Builder
	for i, p := range parts {
		if err := os.Rename(p.Path, filepath.Join(data, PartName(i))); err != nil {
			return fmt.Errorf("publish part %d: %w", i, err)
		}
		key := DataDir + "/" + PartName(i)
		m.Files = append(m.Files, manifestFile{Key: key, Size: p.Bytes, MD5checksum: p.MD5, RowCount: p.Rows, MinKey: p.MinKey, MaxKey: p.MaxKey})
		m.TotalRows += p.Rows
		links.WriteString(key + "\n")
	}
	if err := syncDir(data); err != nil {
		return fmt.Errorf("sync data directory: %w", err)
	}
	if err := writeJSON(out, ManifestName, m); err != nil {
		return err
	}
	if err := writeAtomic(out, SymlinkName, []byte(links.String())); err != nil {
		return err
	}
	if err := writeJSON(out, SummaryName, summary); err != nil {
		return err
	}
	if meta.SwathSummary != nil {
		if err := writeJSON(out, SwathSummaryName, meta.SwathSummary); err != nil {
			return err
		}
	}
	return writeAtomic(out, SuccessName, nil)
}

func writeJSON(dir, name string, v any) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode %s: %w", name, err)
	}
	return writeAtomic(dir, name, b.Bytes())
}

// writeAtomic writes name via name.tmp, fsync, rename and a directory fsync.
func writeAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync after %s: %w", name, err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if err != nil && dirSyncUnsupported(err) {
		err = nil
	}
	return errors.Join(err, d.Close())
}
