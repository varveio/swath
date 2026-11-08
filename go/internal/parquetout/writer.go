// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"errors"
	"fmt"
	"os"
	"strings"

	pq "github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/write"

	"github.com/varveio/swath/go/internal/buildinfo"
	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// Served-layout geometry (Swath's sorted output), passed to the module's
// part writer; pageBufferBytes is its fixed page buffer, priced below.
const (
	pageRows        = 1024    // rows per page; the byte-target check runs at page boundaries
	groupBytes      = 8 << 20 // row-group target, on the writer's size estimate
	groupRows       = 1_000_000
	pageBufferBytes = 1 << 20 // parquet-go page buffer
)

// EncoderBytes is the working set admitted per encoder of schema s: one
// writer's (writerBytes) and one of the router's codec states (stateBytes:
// the codec is shared by the writers, a state per encoder admitted). It is
// priced at the longest values the schema allows (its key cap, and each
// column's schema.Spec.MaxBytes), so it holds whatever the listing.
func EncoderBytes[P any](s *schema.Schema[P]) int64 {
	wide := 0
	for i := range s.Columns {
		wide += s.Columns[i].MaxBytes
	}
	return encoderBytes(s.MaxKeyBytes, wide, len(s.Columns)+1)
}

// DefaultEncoderBytes prices an encoder of a schema not known yet: keys
// without a cap, one more wide column of unboundedKeyBytes and 16
// columns, beyond every provider's schema.
var DefaultEncoderBytes = encoderBytes(0, unboundedKeyBytes, 16)

// unboundedKeyBytes prices a key without a cap (schema.Info.MaxKeyBytes
// 0: a local path, at most PATH_MAX on Linux).
const unboundedKeyBytes = 4096

// The writer's and its codec state's working sets are measurements, not
// values derived from the libraries: parquet-go v0.32.0 and
// klauspost/compress v1.19.1 (2026-11-08, live heap after GC after every
// page of 24 pages a writer, random values at each length). Remeasure on
// an upgrade of either (TestWriterAdmissionCoversHeap, and the probes in
// docs/internals/parquet-output.md). v1.20.3, which the seekparquet module
// requires, passed TestWriterAdmissionCoversHeap and wrote the same pages
// (TestFooterOnlyChange); it was not remeasured.
//   - a codec state that has compressed a page larger than one zstd block
//     (128 KiB) keeps a history of one 8 MiB window and a block (zstd's
//     lower encoder memory: twice the window without it), plus its match
//     tables: about 10.6 MiB measured, 12 priced (bigStateBytes); one that
//     only saw smaller pages, about 2 MiB (smallStateBytes). A key page is
//     1,024 keys, so keys over 128 bytes make every state a big one.
//   - a writer holds its row group's encoded pages (groupBytes, overshot:
//     the size is checked between pages), a page buffer per column
//     (pageBufferBytes, grown to the page) and its wide values: the key
//     about two and a half times over (the page being filled, its delta
//     encoding), a column with Spec.MaxBytes four times (its dictionary
//     attempt besides). The module's part writer copies the page being
//     filled (write.PartWriter.WriteRows: rows may be reused), one more
//     time each, priced and not yet remeasured. One S3 writer at 1,024-byte keys measured 13 MiB,
//     four 15 each (19.5 priced); four filesystem writers at binary
//     4,096-byte keys and link targets, 24 pages each, 170 MiB in all, and
//     216 to 218 with 800 MB of rows live beside them (244 priced, 61
//     each: the margin, 12 %, is over that worst case).
const (
	bigStateBytes   = 12 << 20
	smallStateBytes = 2 << 20
	zstdBlockBytes  = 128 << 10
)

// stateBytes is one codec state's working set for pages of at most page
// bytes; writerBytes one writer's, of columns columns, keys of key bytes
// and Spec.MaxBytes columns whose widths sum to wide.
func stateBytes(page int64) int64 {
	if page > zstdBlockBytes {
		return bigStateBytes
	}
	return smallStateBytes
}

func writerBytes(key, wide, columns int) int64 {
	return groupBytes + int64(columns)*pageBufferBytes + 7*int64(pageRows)*int64(key)/2 + 5*int64(pageRows)*int64(wide)
}

func encoderBytes(maxKey, wide, columns int) int64 {
	if maxKey <= 0 {
		maxKey = unboundedKeyBytes
	}
	page := int64(pageRows) * int64(max(maxKey, wide))
	return writerBytes(maxKey, wide, columns) + stateBytes(page)
}

// Recipe identifies everything about the writer that decides a part's
// bytes apart from the build: the seekparquet module's part writer
// (write.NewPart, at the version go.mod pins) on parquet-go v0.32.0, with
// this schema binding and codec: v2 pages of 1,024 rows,
// 8 MiB row groups, 8 KiB dictionaries, exact column index bounds, the
// SeekParquet footer stamps, delta keys
// and etags, delta integers with standard INT annotations, zstd
// SpeedDefault with entropy coding, and parts cut at size/0.1 for part 0,
// then at the frozen ratio. Bump the revision with any change to these.
// It is the resume identity's writer field: a resume refuses a journal
// written under another recipe.
//
// Versions are separate: the dataset format (seekparquet.FormatVersion and
// DraftRevision), the logical schema (schema IDs), the private journal
// (journal.Version) and this recipe. created_by names the real build, so bytes are identical
// within a build; across builds only the recipe is promised.
const Recipe = "swath-go/parquet-r3"

// createdApp, createdVersion and createdBuild make the footer's
// created_by: the tool, its module version and its VCS revision, from the
// build itself (buildinfo, the linker's revision included), the revision
// shortened to 12 hex digits.
var createdApp, createdVersion, createdBuild = footerBuild(buildinfo.Read())

func footerBuild(b buildinfo.Build) (app, version, rev string) {
	rev, dirty := strings.CutSuffix(b.Revision, "-dirty")
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "-dirty"
	}
	return b.Tool, b.Version, rev
}

// writer writes one sorted part through the module's part writer, which
// checks the key order (strictly ascending, or with schema.Info.Groups
// nondecreasing) and the profile.
type writer[P any] struct {
	schema *schema.Schema[P]
	path   string
	part   *write.PartWriter
	capped []int    // the columns with a Spec.MaxBytes, enforced as the key's cap is
	rows   []pq.Row // a batch handed to the part writer, reused
	closed bool
}

// batchRows is the rows handed to the part writer at a time.
const batchRows = 64

// partFile is a part's file as the module writes it: its writes and its
// sync go through faults.
type partFile struct{ *os.File }

func (f partFile) Write(b []byte) (int, error) { return faults.Write(f.File, b) }
func (f partFile) Sync() error                 { return faults.Sync(f.File) }

// create opens path for a part, compressing with c (the router's, shared
// by its writers; the writer does not close it). createdBy, if not empty,
// is the footer's created_by verbatim instead of this build's (a
// reproduced claimed part's own, Cut.CreatedBy).
func create[P any](open func(string) (*os.File, error), path string, s *schema.Schema[P], c *codec, columns func() int, createdBy string) (*writer[P], error) {
	if open == nil {
		open = func(path string) (*os.File, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		}
	}
	f, err := open(path)
	if err != nil {
		return nil, fmt.Errorf("create part: %w", err)
	}
	if createdBy == "" {
		var cfg pq.WriterConfig
		pq.CreatedBy(createdApp, createdVersion, createdBuild).ConfigureWriter(&cfg)
		createdBy = cfg.CreatedBy
	}
	part, err := write.NewPart(partFile{f}, nodeOf(s), write.PartOptions{
		Key:      seekparquet.Key{Column: dataset.KeyColumn, Unique: !s.Info.Groups},
		SchemaID: s.ID, Codec: c, CreatedBy: createdBy, ColumnWorkers: columns,
		PageRows: pageRows, RowGroupBytes: groupBytes, RowGroupRows: groupRows,
	})
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("create part: %w", err)
	}
	width := len(s.Columns) + 1
	values := make([]pq.Value, batchRows*width)
	w := &writer[P]{schema: s, path: path, part: part, rows: make([]pq.Row, batchRows)}
	for i := range w.rows {
		w.rows[i] = values[i*width : (i+1)*width : (i+1)*width]
	}
	for i := range s.Columns {
		if s.Columns[i].MaxBytes > 0 && s.Columns[i].Str != nil {
			w.capped = append(w.capped, i)
		}
	}
	return w, nil
}

// write appends rows, after checking each against the schema's value
// caps.
func (w *writer[P]) write(rows []schema.Row[P]) error {
	for len(rows) > 0 {
		n := min(len(rows), batchRows)
		for i := range rows[:n] {
			if err := w.check(&rows[i]); err != nil {
				return err
			}
		}
		fill(w.schema, w.rows[:n], rows[:n])
		if _, err := w.part.WriteRows(w.rows[:n]); err != nil {
			return err
		}
		rows = rows[n:]
	}
	return nil
}

// check enforces the key's cap and each capped column's.
func (w *writer[P]) check(r *schema.Row[P]) error {
	if n := w.schema.MaxKeyBytes; n > 0 && len(r.Key) > n {
		return fmt.Errorf("%w: %d bytes (limit %d): %.64q…", schema.ErrKeyTooLong, len(r.Key), n, r.Key)
	}
	for _, c := range w.capped {
		col := &w.schema.Columns[c]
		if v, ok := col.Str(&r.Data); ok && len(v) > col.MaxBytes {
			return fmt.Errorf("%w: %s of %.64q: %d bytes (limit %d)", schema.ErrValueTooLong, col.Name, r.Key, len(v), col.MaxBytes)
		}
	}
	return nil
}

// close seals the part: the last page and row group, the footer, the
// file's sync and close. Metadata is returned only if all of that
// succeeded.
func (w *writer[P]) close() (dataset.Part, error) {
	if w.closed {
		return dataset.Part{}, errors.New("writer already closed")
	}
	w.closed = true
	// The directory entry is made durable by the committer that claims
	// the part for its record (listing), not here.
	info, err := w.part.Seal()
	if err != nil {
		return dataset.Part{}, fmt.Errorf("close %s: %w", w.path, err)
	}
	return dataset.Part{Path: w.path, PartInfo: dataset.PartInfo{Rows: info.Rows, Bytes: info.Bytes, SHA256: info.SHA256,
		MinKey: dataset.Key(info.MinKey), MaxKey: dataset.Key(info.MaxKey)}}, nil
}

// abort releases the writer without producing a part.
func (w *writer[P]) abort() {
	if !w.closed {
		w.closed = true
		_ = w.part.Abort()
	}
}
