// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package javaera is a LEGACY profile (deleted with its parent package
// legacy, #353): the sorted-Parquet profile of the Java tool's sorted
// output: footer-stamped parts of the 13-column canonical schema, in a
// directory or as one file, with no manifest. It decides whether such a set
// can be served sorted and opens it with legacy.
package javaera

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/swath/go/internal/replay/legacy"
)

// Footer stamps and the values this profile accepts.
const (
	StampOrder         = "swath.sort.order"
	StampMode          = "swath.sort.mode"
	StampFormatVersion = "swath.sort.format_version"
	StampFileIndex     = "swath.sort.file_index"
	StampFileFinal     = "swath.sort.file_final"

	// Order is the only recognised sort order: a future writer's order
	// must not be trusted by this reader.
	Order = "key_bytes_unsigned,version_id_null_first,row_type_rank"
	// ModeObjects is the current-objects mode; ModeVersions is recognised
	// but not servable.
	ModeObjects  = "objects"
	ModeVersions = "versions"
	// FormatVersion is the one stamp format version this reader knows.
	FormatVersion = 1
	// FileFinalValue is the only value the final-file stamp is written
	// with; its absence is the negative case.
	FileFinalValue = "true"
)

// BoundBytes is the length from which this profile treats a key ColumnIndex
// bound as possibly truncated. The Java writer does not record its
// truncation length: served fixtures keep 1,024 bytes, older ones the
// library default of 64, cut on a code point boundary, so a truncated bound
// can be as short as 61 bytes. Treating every bound from 61 bytes on as
// conservative is correct for both and costs at most a page read.
const BoundBytes = 64 - 3

// SchemaID is the schema id this profile declares for its parts: the Java
// tool's 13-column canonical schema.
const SchemaID = "swath/canonical"

// RowTypeColumn is the column whose footer statistics must prove that a
// row group holds only current objects.
const RowTypeColumn = "row_type"

var objectRowType = []byte("OBJECT")

// Stamp is a part's sortedness stamp.
type Stamp struct {
	Order         string
	Mode          string
	FormatVersion int
	// FileIndex is the part's 1-based position in the output, 1 when the
	// stamp is absent or unparseable (a stamp that predates it comes from
	// a single-file output).
	FileIndex int
	FileFinal bool
}

// String spells the stamp as the Java tool's record does, which the fixture
// identity hashes.
func (s Stamp) String() string {
	return fmt.Sprintf("SortedParquetStamp[order=%s, mode=%s, formatVersion=%d, fileIndex=%d, fileFinal=%t]",
		s.Order, strings.ToUpper(s.Mode), s.FormatVersion, s.FileIndex, s.FileFinal)
}

// ReadStamp returns a part's stamp, or false unless the footer carries a
// recognised order, a known mode and a parseable format version.
func ReadStamp(file *parquet.File) (Stamp, bool) {
	order, _ := file.Lookup(StampOrder)
	if order != Order {
		return Stamp{}, false
	}
	mode, _ := file.Lookup(StampMode)
	if mode != ModeObjects && mode != ModeVersions {
		return Stamp{}, false
	}
	version, ok := parseInt(file.Lookup(StampFormatVersion))
	if !ok {
		return Stamp{}, false
	}
	index, ok := parseInt(file.Lookup(StampFileIndex))
	if !ok {
		index = 1
	}
	final, _ := file.Lookup(StampFileFinal)
	return Stamp{Order: order, Mode: mode, FormatVersion: version, FileIndex: index, FileFinal: final == FileFinalValue}, true
}

func parseInt(value string, present bool) (int, bool) {
	if !present {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(value), 10, 32)
	return int(n), err == nil
}

// Resolve returns path itself when it is a regular file, otherwise its
// direct children named *.parquet in byte order of their names, without
// recursion. A directory without parts resolves to none.
func Resolve(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode().IsRegular() {
		return []string{path}, nil
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s: not a file or directory", path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".parquet") && !entry.IsDir() {
			files = append(files, filepath.Join(path, entry.Name()))
		}
	}
	slices.Sort(files)
	return files, nil
}

// CompletenessViolation returns why the stamps of the n parts present do
// not prove a complete output, or "" when they do: their file indexes must
// be exactly 1..n, and exactly one, the one at n, must be stamped final. A
// publish interrupted between renaming parts leaves a contiguous prefix
// with no final part, which the second rule catches.
func CompletenessViolation(stamps []Stamp) string {
	n := len(stamps)
	seen := make([]bool, n+1)
	finals, finalIndex := 0, -1
	for i, stamp := range stamps {
		if stamp.FileIndex < 1 || stamp.FileIndex > n || seen[stamp.FileIndex] {
			return fmt.Sprintf("part %d reports file_index=%d, not a free position among 1..%d", i+1, stamp.FileIndex, n)
		}
		seen[stamp.FileIndex] = true
		if stamp.FileFinal {
			finals++
			finalIndex = stamp.FileIndex
		}
	}
	if finals != 1 {
		return fmt.Sprintf("expected exactly one part stamped final among %d, found %d", n, finals)
	}
	if finalIndex != n {
		return fmt.Sprintf("the final stamp is on file_index=%d, expected %d", finalIndex, n)
	}
	return ""
}

// PureObjects reports whether a row group's row_type statistics prove every
// row is an OBJECT row: the one decision this profile takes from
// statistics, sound because a minimum and maximum that are both OBJECT
// leave no room for another value.
func PureObjects(file *parquet.File, group *format.RowGroup) bool {
	leaf, ok := file.Schema().Lookup(RowTypeColumn)
	if !ok || leaf.ColumnIndex >= len(group.Columns) {
		return false
	}
	statistics := group.Columns[leaf.ColumnIndex].MetaData.Statistics
	lo, hi := statistics.MinValue, statistics.MaxValue
	if lo == nil && hi == nil {
		lo, hi = statistics.Min, statistics.Max
	}
	return bytes.Equal(lo, objectRowType) && bytes.Equal(hi, objectRowType)
}

// Fixture is an opened Java-era fixture.
type Fixture struct {
	Reader *legacy.Reader
	// Files are the resolved parts in key order; Stamps their stamps.
	Files  []string
	Stamps []Stamp
}

// Open resolves path (one part, or a directory's direct *.parquet
// children), checks in order that every part carries a recognised stamp of
// the objects mode and known format version, that the parts are complete,
// and that every non-empty row group holds only OBJECT rows with first keys
// strictly ascending across the set, and opens it with the column type
// expectations expect. A set that fails is refused with the first failing
// reason: no_stamp, unsupported_mode, unknown_format_version,
// incomplete_multifile, mixed_row_types or sanity_failed.
//
// The parts are read through files, which bounds the descriptors they hold
// open whatever their number; nil uses a cache of its own with
// legacy.DefaultMaxOpenFiles.
func Open(ctx context.Context, path string, expect []legacy.Column, files *legacy.FileCache) (*Fixture, error) {
	names, err := Resolve(path)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, legacy.Refuse(legacy.ReasonNoStamp, "%s holds no *.parquet parts", path)
	}
	if files == nil {
		files = legacy.NewFileCache(0)
	}
	fixture := &Fixture{Files: names}
	var sources []legacy.PartSource
	ok := false
	defer func() {
		if !ok {
			for _, source := range sources {
				_ = source.File.(io.Closer).Close()
			}
		}
	}()
	for _, name := range names {
		file, err := files.Open(ctx, name, nil)
		if err != nil {
			return nil, err
		}
		source := legacy.PartSource{Name: name, File: file, Size: file.Size(), BoundBytes: BoundBytes}
		sources = append(sources, source)
		// An unreadable or foreign footer is not stamped.
		footer, err := legacy.OpenFooter(file, file.Size())
		if err != nil {
			return nil, legacy.Refuse(legacy.ReasonNoStamp, "%s: %v", name, err)
		}
		stamp, stamped := ReadStamp(footer)
		switch {
		case !stamped:
			return nil, legacy.Refuse(legacy.ReasonNoStamp, "%s carries no recognised sortedness stamp%s", name, format2Hint(footer))
		case stamp.Mode != ModeObjects:
			return nil, legacy.Refuse(legacy.ReasonUnsupportedMode, "%s is stamped mode=%s, not objects", name, stamp.Mode)
		case stamp.FormatVersion != FormatVersion:
			return nil, legacy.Refuse(legacy.ReasonUnknownFormatVersion, "%s is stamped format_version=%d", name, stamp.FormatVersion)
		}
		sources[len(sources)-1].Footer = footer
		fixture.Stamps = append(fixture.Stamps, stamp)
	}
	if violation := CompletenessViolation(fixture.Stamps); violation != "" {
		return nil, legacy.Refuse(legacy.ReasonIncompleteMultifile, "%s", violation)
	}
	reader, err := legacy.Open(ctx, sources, legacy.Options{
		Expect: expect,
		CheckGroup: func(part string, footer *parquet.File, physical int, group *format.RowGroup) error {
			if !PureObjects(footer, group) {
				return legacy.Refuse(legacy.ReasonMixedRowTypes,
					"row group %d of %s is not provably OBJECT-only (row_type statistics absent or not OBJECT..OBJECT)", physical, part)
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	ok = true
	fixture.Reader = reader
	return fixture, nil
}

// format2Hint names the likely mistake when a part of a manifest dataset is
// opened without its manifest.
func format2Hint(footer *parquet.File) string {
	if _, ok := footer.Lookup("swath.profile"); ok {
		return " (it is a part of a manifest dataset: open the dataset directory)"
	}
	return ""
}
