// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package sortfixture turns an unsorted Java-era capture into a sorted
// format-2 dataset the replay server can serve sorted.
//
// The command line is
//
//	swath-replay sort-fixture --capture PATH --output DIR [--key-limit N]
//
// --capture is one capture part or a directory of *.parquet parts (not
// recursed) in the 13-column canonical schema; --output is a new or empty
// directory the dataset is published into; --key-limit, at most 4,096,
// raises the key cap from 1,024 bytes (see Options.KeyLimit).
//
// The dataset's schema id is always SchemaID: the nine columns of the S3
// current-object schema under an id of its own, since a converted capture
// is not a listing by the Go lister.
//
// The capture is read whole into memory and sorted there: about 150 bytes
// per row plus its key and value bytes, so a few million rows is the
// intended scope. The Go lister writes sorted datasets directly; this
// command exists for captures recorded before it.
package sortfixture

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/parquetout"
	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/sortedparquet/javaera"
)

// SchemaID is the schema id of every dataset Run writes: key, size,
// last_modified, etag, storage_class, owner_id, owner_display_name,
// checksum_algorithm and checksum_type, as the S3 current-object schema,
// with a key cap of DefaultKeyLimit or Options.KeyLimit. Azure blob names
// reach 1,024 characters, up to about 2,000 UTF-8 bytes, which a 1,024-byte
// cap cannot hold.
const SchemaID = "swath-replay/objects/v1"

// DefaultKeyLimit is the key cap without Options.KeyLimit: the S3 limit.
const DefaultKeyLimit = 1024

// MaxKeyLimit is the largest Options.KeyLimit.
const MaxKeyLimit = 4096

// partSize is the encoded target per part.
const partSize = 128 << 20

// Options configures Run.
type Options struct {
	// Capture is one capture part or a directory of *.parquet parts.
	Capture string
	// Output is the dataset directory; it must not exist or be empty.
	Output string
	// KeyLimit, when positive (at most MaxKeyLimit), is the key cap in
	// bytes; zero is DefaultKeyLimit.
	KeyLimit int
}

// Stats summarises a run.
type Stats struct {
	CaptureRows int64 // rows read
	Objects     int64 // OBJECT rows written
	Skipped     int64 // rows of other types left out
	Parts       int
	Bytes       int64
	SchemaID    string
	Duration    time.Duration
}

// ErrDuplicateKey reports two capture rows with the same key.
var ErrDuplicateKey = errors.New("sort-fixture: duplicate key")

// ErrVersioned reports a capture row with a version id: only current
// listings can be served sorted.
var ErrVersioned = errors.New("sort-fixture: versioned capture")

// ErrKeyTooLong reports an object key over the allowed length.
var ErrKeyTooLong = errors.New("sort-fixture: key too long")

// row is one capture row kept for sorting: its key, whether it is a
// current object, and the object's facts.
type row struct {
	object bool
	schema.Row[Object]
}

// Run reads the capture, keeps its OBJECT rows, refuses a capture with a
// versioned row (ErrVersioned) or two rows of any type with one key
// (ErrDuplicateKey), sorts the objects by unsigned key bytes and writes
// them as a format-2 dataset of SchemaID, nulls intact. It writes one
// summary line to log.
func Run(ctx context.Context, opts Options, log io.Writer) (Stats, error) {
	start := time.Now()
	stats := Stats{}
	if opts.KeyLimit < 0 || opts.KeyLimit > MaxKeyLimit {
		return stats, fmt.Errorf("sort-fixture: key limit %d outside 0..%d", opts.KeyLimit, MaxKeyLimit)
	}
	if opts.Capture == "" || opts.Output == "" {
		return stats, errors.New("sort-fixture: a capture and an output are required")
	}
	files, err := javaera.Resolve(opts.Capture)
	if err != nil {
		return stats, fmt.Errorf("sort-fixture: capture: %w", err)
	}
	var rows []row
	for _, file := range files {
		if rows, err = readCapture(ctx, file, rows); err != nil {
			return stats, fmt.Errorf("sort-fixture: %s: %w", file, err)
		}
	}
	stats.CaptureRows = int64(len(rows))
	slices.SortFunc(rows, func(a, b row) int { return strings.Compare(a.Key, b.Key) })
	objects := make([]schema.Row[Object], 0, len(rows))
	longest := 0
	for i := range rows {
		if i > 0 && rows[i].Key == rows[i-1].Key {
			return stats, fmt.Errorf("%w '%s'", ErrDuplicateKey, rows[i].Key)
		}
		if !rows[i].object {
			stats.Skipped++
			continue
		}
		longest = max(longest, len(rows[i].Key))
		objects = append(objects, rows[i].Row)
	}
	rows = nil
	stats.Objects = int64(len(objects))

	limit := cmp.Or(opts.KeyLimit, DefaultKeyLimit)
	if longest > limit {
		return stats, fmt.Errorf("%w: an object key is %d bytes (limit %d; --key-limit raises it up to %d)", ErrKeyTooLong, longest, limit, MaxKeyLimit)
	}
	sch := newSchema(limit)
	stats.SchemaID = sch.ID
	parts, err := write(ctx, opts, sch, objects, start)
	if err != nil {
		return stats, err
	}
	stats.Parts = len(parts)
	for _, p := range parts {
		stats.Bytes += p.Bytes
	}
	stats.Duration = time.Since(start)
	if log != nil {
		abs, _ := filepath.Abs(opts.Output)
		fmt.Fprintf(log, "sort_fixture rows=%d objects=%d skipped=%d parts=%d bytes=%d schema=%s wall_ms=%d output=%s\n",
			stats.CaptureRows, stats.Objects, stats.Skipped, stats.Parts, stats.Bytes, stats.SchemaID, stats.Duration.Milliseconds(), abs)
	}
	return stats, nil
}

// write publishes objects, sorted, through the dataset writer.
func write(ctx context.Context, opts Options, sch *schema.Schema[Object], objects []schema.Row[Object], start time.Time) ([]dataset.Part, error) {
	if err := dataset.Prepare(opts.Output, "", sch.ID, false); err != nil {
		return nil, fmt.Errorf("sort-fixture: output: %w", err)
	}
	router, err := parquetout.NewRouter(parquetout.Options{Out: opts.Output, PartSize: partSize, Encoders: 1}, sch)
	if err != nil {
		return nil, fmt.Errorf("sort-fixture: %w", err)
	}
	const batch = 4096
	for i := 0; i < len(objects); i += batch {
		if err := router.Write(ctx, [][]schema.Row[Object]{objects[i:min(i+batch, len(objects))]}); err != nil {
			router.Abort()
			dataset.Cleanup(opts.Output)
			return nil, fmt.Errorf("sort-fixture: write: %w", err)
		}
	}
	parts, err := router.Close()
	if err != nil {
		dataset.Cleanup(opts.Output)
		return nil, fmt.Errorf("sort-fixture: write: %w", err)
	}
	capture, _ := filepath.Abs(opts.Capture)
	meta := dataset.Meta{
		SchemaID: sch.ID,
		Source:   dataset.Source{Kind: "capture", URI: filepath.ToSlash(capture)},
		Scope:    dataset.Scope{Kind: "whole"},
		Coverage: dataset.Coverage{Complete: true, StopReason: "completed", ListingStartedAt: start.UTC(), ListingFinishedAt: time.Now().UTC()},
	}
	if err := dataset.Publish(opts.Output, meta, parts, nil); err != nil {
		dataset.Cleanup(opts.Output)
		return nil, fmt.Errorf("sort-fixture: publish: %w", err)
	}
	_ = os.RemoveAll(filepath.Join(opts.Output, dataset.Workspace))
	return parts, nil
}

// captureColumns are the canonical columns a capture row is read from;
// only key and row_type are required.
var captureColumns = []string{"key", "size", "last_modified", "etag", "storage_class", "version_id",
	"owner_id", "owner_display_name", "checksum_algorithm", "checksum_type", "row_type"}

const (
	cKey = iota
	cSize
	cModified
	cETag
	cStorageClass
	cVersion
	cOwnerID
	cOwnerDisplayName
	cChecksumAlgorithm
	cChecksumType
	cRowType
)

// readCapture appends the rows of one capture part.
func readCapture(ctx context.Context, name string, rows []row) ([]row, error) {
	f, err := os.Open(name)
	if err != nil {
		return rows, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return rows, err
	}
	file, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return rows, err
	}
	index := make([]int, len(captureColumns))
	for i, column := range captureColumns {
		index[i] = -1
		if leaf, ok := file.Schema().Lookup(column); ok {
			index[i] = leaf.ColumnIndex
		}
	}
	if index[cKey] < 0 || index[cRowType] < 0 {
		return rows, errors.New("not a capture part: key and row_type columns are required")
	}
	byColumn := make(map[int]int, len(index))
	for i, column := range index {
		if column >= 0 {
			byColumn[column] = i
		}
	}
	reader := parquet.NewReader(file)
	defer func() { _ = reader.Close() }()
	buffer := make([]parquet.Row, 256)
	values := make([]parquet.Value, len(captureColumns))
	for {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		n, readErr := reader.ReadRows(buffer)
		for _, r := range buffer[:n] {
			for i := range values {
				values[i] = parquet.NullValue()
			}
			for _, v := range r {
				if i, ok := byColumn[v.Column()]; ok {
					values[i] = v
				}
			}
			key := values[cKey].ByteArray()
			if !values[cVersion].IsNull() {
				return rows, fmt.Errorf("%w: key '%s' carries version_id '%s'", ErrVersioned, key, values[cVersion].ByteArray())
			}
			kept := row{object: bytes.Equal(values[cRowType].ByteArray(), []byte("OBJECT"))}
			kept.Key = string(key)
			if kept.object {
				kept.Data = Object{
					Size:              number(values[cSize]),
					Mtime:             number(values[cModified]),
					ETag:              text(values[cETag]),
					StorageClass:      text(values[cStorageClass]),
					OwnerID:           text(values[cOwnerID]),
					OwnerDisplayName:  text(values[cOwnerDisplayName]),
					ChecksumAlgorithm: text(values[cChecksumAlgorithm]),
					ChecksumType:      text(values[cChecksumType]),
				}
			}
			rows = append(rows, kept)
		}
		if errors.Is(readErr, io.EOF) || n == 0 && readErr == nil {
			return rows, nil
		}
		if readErr != nil {
			return rows, readErr
		}
	}
}

// number returns an integer column's value, nil when null.
func number(v parquet.Value) *int64 {
	if v.IsNull() {
		return nil
	}
	n := v.Int64()
	return &n
}

// text returns a string column's value, "" (null in the schema) when null.
func text(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	return string(v.ByteArray())
}
