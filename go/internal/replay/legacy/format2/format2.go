// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package format2 is the sorted-Parquet profile of manifest datasets
// (format 2): a directory whose manifest.json is the commit and lists the
// parts in key order with their exact key bounds. It decides whether such a
// dataset can be served sorted and opens it with legacy.
package format2

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

// Names and stamps of the format.
const (
	ManifestName = "manifest.json"
	// Format and Version are the manifest's format and format_version and
	// the footers' profile stamps.
	Format  = "swath-dataset"
	Version = 2

	StampProfile        = "swath.profile"
	StampProfileVersion = "swath.profile_version"
	StampSchemaID       = "swath.schema_id"
	// StampIndexBound is the writer's ColumnIndex truncation length: bounds
	// this long or longer are conservative.
	StampIndexBound = "swath.index_bound_bytes"
)

// Manifest is the part of manifest.json the reader uses.
type Manifest struct {
	Format        string `json:"format"`
	FormatVersion int    `json:"format_version"`
	SchemaID      string `json:"schema_id"`
	Coverage      struct {
		Complete bool `json:"complete"`
	} `json:"coverage"`
	Parts []Part `json:"parts"`
}

// Part is one manifest part: its path relative to the dataset root, its
// rows and bytes, and its exact first and last keys.
type Part struct {
	Path   string `json:"path"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	MinKey Key    `json:"min_key_base64"`
	MaxKey Key    `json:"max_key_base64"`
	MD5    string `json:"md5"`
}

// Key is a key's bytes, spelled in standard base64 in JSON.
type Key []byte

// UnmarshalJSON decodes a base64 JSON string.
func (k *Key) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("key: %w", err)
	}
	*k = v
	return nil
}

// IsDataset reports whether dir holds a manifest.
func IsDataset(dir string) bool {
	info, err := os.Stat(dir + string(os.PathSeparator) + ManifestName)
	return err == nil && info.Mode().IsRegular()
}

// Dataset is an opened format-2 dataset.
type Dataset struct {
	Reader   *legacy.Reader
	Manifest Manifest
	// ManifestBytes are the manifest exactly as read.
	ManifestBytes []byte
}

// Open reads dir's manifest and opens the parts it lists, with the column
// type expectations expect. Any schema ID is accepted as long as the parts have a required
// key column. It refuses, with the first failing reason:
//
//   - invalid_manifest: no readable manifest of this format, a part with
//     rows but without both exact key bounds, or a part path outside the
//     dataset;
//   - unknown_format_version: a manifest or footer of another version;
//   - incomplete_multifile: a listed part that is missing or whose size or
//     row count differs from its entry;
//   - no_stamp: a part without the profile stamps of its manifest's schema,
//     or a row group not stamped as sorted by key ascending;
//   - unsupported_schema, no_page_index, sanity_failed: as legacy.Open,
//     where sanity_failed also covers decoded bounds that differ from the
//     manifest's and parts whose bounds do not ascend.
//
// It does not check the parts' MD5 digests or the manifest's coverage: a
// dataset that stopped early is served as what it holds.
//
// The parts are read through files, which bounds the descriptors they hold
// open whatever their number; nil uses a cache of its own with
// legacy.DefaultMaxOpenFiles.
func Open(ctx context.Context, dir string, expect []legacy.Column, files *legacy.FileCache) (*Dataset, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	raw, err := root.ReadFile(ManifestName)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, legacy.Refuse(legacy.ReasonInvalidManifest, "%s has no %s", dir, ManifestName)
		}
		return nil, err
	}
	var manifest Manifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, legacy.Refuse(legacy.ReasonInvalidManifest, "%s: %v", ManifestName, err)
	}
	if manifest.Format != Format {
		return nil, legacy.Refuse(legacy.ReasonInvalidManifest, "%s: format %q is not %q", ManifestName, manifest.Format, Format)
	}
	if manifest.FormatVersion != Version {
		return nil, legacy.Refuse(legacy.ReasonUnknownFormatVersion, "%s: format_version %d", ManifestName, manifest.FormatVersion)
	}
	if manifest.SchemaID == "" {
		return nil, legacy.Refuse(legacy.ReasonInvalidManifest, "%s names no schema_id", ManifestName)
	}
	for i, part := range manifest.Parts {
		// The bounds route reads between parts and are checked against the
		// decoded keys: a part without them could overlap its neighbours
		// unseen, and a read routed past it would skip its rows.
		if part.Rows > 0 && (len(part.MinKey) == 0 || len(part.MaxKey) == 0) {
			return nil, legacy.Refuse(legacy.ReasonInvalidManifest, "%s: part %d has rows but no exact key bounds", ManifestName, i)
		}
		if part.Rows < 0 || part.Rows > 0 && bytes.Compare(part.MinKey, part.MaxKey) > 0 {
			return nil, legacy.Refuse(legacy.ReasonInvalidManifest, "%s: part %d has invalid rows or bounds", ManifestName, i)
		}
		if i > 0 && part.Rows > 0 && manifest.Parts[i-1].Rows > 0 && bytes.Compare(manifest.Parts[i-1].MaxKey, part.MinKey) >= 0 {
			return nil, legacy.Refuse(legacy.ReasonSanityFailed, "%s: part %d does not start after part %d", ManifestName, i, i-1)
		}
	}

	if files == nil {
		files = legacy.NewFileCache(0)
	}
	var sources []legacy.PartSource
	ok := false
	defer func() {
		if !ok {
			for _, source := range sources {
				_ = source.File.(io.Closer).Close()
			}
		}
	}()
	for _, part := range manifest.Parts {
		source, err := openPart(ctx, root, dir, files, manifest, part)
		if source.File != nil {
			sources = append(sources, source)
		}
		if err != nil {
			return nil, err
		}
	}
	reader, err := legacy.Open(ctx, sources, legacy.Options{
		Expect:     expect,
		CheckGroup: checkSorted,
	})
	if err != nil {
		return nil, err
	}
	ok = true
	return &Dataset{Reader: reader, Manifest: manifest, ManifestBytes: raw}, nil
}

// openPart opens a part through root, which refuses a path escaping the
// dataset, into files under its path in dir: the cache reopens it by that
// path and requires the same file.
func openPart(ctx context.Context, root *os.Root, dir string, files *legacy.FileCache, manifest Manifest, part Part) (legacy.PartSource, error) {
	file, err := files.Open(ctx, filepath.Join(dir, filepath.FromSlash(part.Path)), func() (*os.File, error) {
		return root.OpenFile(part.Path, legacy.OpenFlags, 0)
	})
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return legacy.PartSource{}, legacy.Refuse(legacy.ReasonIncompleteMultifile, "%s is missing", part.Path)
	case errors.Is(err, legacy.ErrNotRegular):
		return legacy.PartSource{}, legacy.Refuse(legacy.ReasonIncompleteMultifile, "%s is not a regular file", part.Path)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return legacy.PartSource{}, err
	case err != nil:
		// os.Root refuses a path that escapes the dataset.
		return legacy.PartSource{}, legacy.Refuse(legacy.ReasonInvalidManifest, "%s: %v", part.Path, err)
	}
	if file.Size() != part.Bytes {
		_ = file.Close()
		return legacy.PartSource{}, legacy.Refuse(legacy.ReasonIncompleteMultifile, "%s is %d bytes, its manifest entry %d", part.Path, file.Size(), part.Bytes)
	}
	source := legacy.PartSource{Name: part.Path, File: file, Size: file.Size()}
	footer, err := legacy.OpenFooter(file, file.Size())
	if err != nil {
		return source, fmt.Errorf("%s: %w", part.Path, err)
	}
	profile, _ := footer.Lookup(StampProfile)
	version, _ := footer.Lookup(StampProfileVersion)
	schemaID, _ := footer.Lookup(StampSchemaID)
	bound, boundErr := strconv.Atoi(lookup(footer, StampIndexBound))
	switch {
	case profile != Format:
		return source, legacy.Refuse(legacy.ReasonNoStamp, "%s lacks the %s stamp", part.Path, StampProfile)
	case version != strconv.Itoa(Version):
		return source, legacy.Refuse(legacy.ReasonUnknownFormatVersion, "%s is stamped %s=%q", part.Path, StampProfileVersion, version)
	case schemaID != manifest.SchemaID:
		return source, legacy.Refuse(legacy.ReasonNoStamp, "%s is stamped schema %q, its manifest %q", part.Path, schemaID, manifest.SchemaID)
	case boundErr != nil || bound <= 0:
		return source, legacy.Refuse(legacy.ReasonNoStamp, "%s has no valid %s stamp", part.Path, StampIndexBound)
	}
	if footer.NumRows() != part.Rows {
		return source, legacy.Refuse(legacy.ReasonIncompleteMultifile, "%s holds %d rows, its manifest entry %d", part.Path, footer.NumRows(), part.Rows)
	}
	source.Footer, source.BoundBytes = footer, bound
	if part.Rows > 0 {
		source.MinKey, source.MaxKey = part.MinKey, part.MaxKey
	}
	return source, nil
}

func lookup(file *parquet.File, key string) string {
	value, _ := file.Lookup(key)
	return value
}

// checkSorted requires a row group to record the key column ascending as
// its first sorting column.
func checkSorted(part string, footer *parquet.File, physical int, group *format.RowGroup) error {
	key, _ := footer.Schema().Lookup(legacy.KeyColumn)
	sorting := group.SortingColumns
	if len(sorting) == 0 || int(sorting[0].ColumnIdx) != key.ColumnIndex || sorting[0].Descending {
		return legacy.Refuse(legacy.ReasonNoStamp, "%s row group %d is not recorded as sorted by key ascending", part, physical)
	}
	return nil
}
