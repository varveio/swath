// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package dataset

import (
	"github.com/varveio/swath/go/internal/faults"

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
	Path   string `json:"path"` // temporary path while unpublished, the final path once published
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	MD5    string `json:"md5"`
	MinKey string `json:"min_key"`
	MaxKey string `json:"max_key"`
}

// Check is the publication gate: dense ordinals (part i at TempPath i, or
// at its final name once published early), strictly ascending disjoint key
// ranges, and rows written == rows routed.
func Check(parts []Part, rows int64) error {
	var n int64
	for i, p := range parts {
		if b := filepath.Base(p.Path); b != PartName(i)+".tmp" && b != PartName(i) {
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
}

// SwathSummaryName is Swath's run-summary file.
const SwathSummaryName = "_swath_summary.json"

// File is a JSON document published beside the manifest.
type File struct {
	Name  string
	Value any
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
// the data directory, then manifest.json, symlink.txt and the run reports,
// each atomically, and _SUCCESS last.
func Publish(out string, meta Meta, parts []Part, reports []File) error {
	data := filepath.Join(out, DataDir)
	m := manifest{
		Version: "1", CreationTimestamp: time.Now().UnixMilli(), FileFormat: "Parquet",
		FileSchema: meta.FileSchema, Sorted: true, SortKey: meta.SortKey, Files: []manifestFile{},
		SourceBucket: meta.SourceBucket, SchemaID: meta.SchemaID, SourceKind: meta.SourceKind, SourceRoot: meta.SourceRoot,
	}
	var links strings.Builder
	for i, p := range parts {
		if final := filepath.Join(data, PartName(i)); p.Path != final {
			if err := os.Rename(p.Path, final); err != nil {
				return fmt.Errorf("publish part %d: %w", i, err)
			}
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
	if err := faults.At("manifest"); err != nil {
		return err
	}
	if err := writeAtomic(out, SymlinkName, []byte(links.String())); err != nil {
		return err
	}
	for _, r := range reports {
		if err := writeJSON(out, r.Name, r.Value); err != nil {
			return err
		}
	}
	if err := writeAtomic(out, SuccessName, nil); err != nil {
		return err
	}
	return faults.At("success")
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

// PublishPart renames one closed part to its final name and syncs the data
// directory: the early publication of a journaled run, after the part's
// journal record is durable. It returns the part with its final path.
func PublishPart(out string, ordinal int, p Part) (Part, error) {
	final := filepath.Join(out, DataDir, PartName(ordinal))
	if err := faults.At("rename"); err != nil {
		return p, err
	}
	if err := os.Rename(p.Path, final); err != nil {
		return p, fmt.Errorf("publish part %d: %w", ordinal, err)
	}
	if err := syncDir(filepath.Join(out, DataDir)); err != nil {
		return p, fmt.Errorf("sync data directory: %w", err)
	}
	p.Path = final
	return p, nil
}
