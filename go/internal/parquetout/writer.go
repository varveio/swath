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
//     keeps a history of one window and a block (zstd's lower encoder
//     memory: twice the window without it), plus its match tables. Measured
//     at SpeedDefault (8 MiB window, 128 KiB blocks): about 10.6 MiB, 12
//     priced (bigStateBytes). SpeedFastest, the page level since r4, uses a
//     4 MiB window and 64 KiB blocks, so the price stays conservative; one that
//     only saw smaller pages, about 2 MiB (smallStateBytes). A key page is
//     1,024 keys, so keys over 128 bytes make every state a big one.
//   - a writer holds its row group's encoded pages (groupBytes, overshot:
//     the size is checked between pages), a page buffer per column
//     (pageBufferBytes, grown to the page) and its wide values: the key
//     about two and a half times over (the page being filled, its delta
//     encoding), a column with Spec.MaxBytes four times (its dictionary
//     attempt besides). The module's page intake (WritePage) borrows the
//     page's values from the rows and copies nothing. One S3 writer at 1,024-byte keys measured 13 MiB,
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
	return groupBytes + int64(columns)*pageBufferBytes + 5*int64(pageRows)*int64(key)/2 + 4*int64(pageRows)*int64(wide)
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
// SpeedFastest with entropy coding, and parts cut at size/0.1 for part 0,
// then at the frozen ratio. Bump the revision with any change to these.
// It is the resume identity's writer field: a resume refuses a journal
// written under another recipe.
//
// Versions are separate: the dataset format (seekparquet.FormatVersion and
// DraftRevision), the logical schema (schema IDs), the private journal
// (journal.Version) and this recipe. created_by names the real build, so bytes are identical
// within a build; across builds only the recipe is promised.
const Recipe = "swath/parquet-r4"

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

// writer writes one sorted part through the module's part writer, a page
// at a time (write.PartWriter.WritePage), which checks the key order
// (strictly ascending, or with schema.Info.Groups nondecreasing).
type writer[P any] struct {
	schema *schema.Schema[P]
	file   dataset.Artifact
	cols   []string // the footer's columns, the key first
	by     string   // the footer's created_by
	part   *write.PartWriter
	fill   func(column int, values []pq.Value) // fillPage, bound once
	capped []int                               // the columns with a Spec.MaxBytes, enforced as the key's cap is
	page   []schema.Row[P]                     // the page being filled; the module borrows it during WritePage
	closed bool
}

// partFile is a local part's file as the module writes it: its writes and
// its sync go through faults.
type partFile struct{ *os.File }

func (f partFile) Write(b []byte) (int, error) { return faults.Write(f.File, b) }
func (f partFile) Sync() error                 { return faults.Sync(f.File) }
func (f partFile) Identity() string            { return "" }

// openLocal creates a local part's file at path with open (nil: by
// pathname).
func openLocal(open func(string) (*os.File, error), path string) (dataset.Artifact, error) {
	if open == nil {
		open = func(path string) (*os.File, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		}
	}
	f, err := open(path)
	if err != nil {
		return nil, fmt.Errorf("create part: %w", err)
	}
	return partFile{f}, nil
}

// create starts a part written to f, compressing with c (the router's,
// shared by its writers; the writer does not close it). createdBy, if not
// empty, is the footer's created_by verbatim instead of this build's (a
// reproduced claimed part's own, Cut.CreatedBy). The module's part writer
// gets the artifact, never an SDK writer: its Seal syncs then closes, and
// its Abort closes, which the artifact maps onto completing or aborting.
func create[P any](f dataset.Artifact, s *schema.Schema[P], c *codec, columns func() int, createdBy string) (*writer[P], error) {
	if createdBy == "" {
		var cfg pq.WriterConfig
		pq.CreatedBy(createdApp, createdVersion, createdBuild).ConfigureWriter(&cfg)
		createdBy = cfg.CreatedBy
	}
	part, err := write.NewPart(f, nodeOf(s), write.PartOptions{
		Key:      seekparquet.Key{Column: dataset.KeyColumn, Unique: !s.Info.Groups},
		SchemaID: s.ID, Codec: c, CreatedBy: createdBy, ColumnWorkers: columns,
		PageRows: pageRows, RowGroupBytes: groupBytes, RowGroupRows: groupRows,
	})
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("create part: %w", err)
	}
	w := &writer[P]{schema: s, file: f, by: createdBy, part: part, page: make([]schema.Row[P], 0, pageRows)}
	w.cols = append(w.cols, dataset.KeyColumn)
	for i := range s.Columns {
		w.cols = append(w.cols, s.Columns[i].Name)
	}
	w.fill = func(column int, values []pq.Value) { fillPage(s, w.page, column, values) }
	for i := range s.Columns {
		if s.Columns[i].MaxBytes > 0 && s.Columns[i].Str != nil {
			w.capped = append(w.capped, i)
		}
	}
	return w, nil
}

// fillPage writes column of page into values (WritePage's fill): the key,
// or schema column column-1.
func fillPage[P any](s *schema.Schema[P], page []schema.Row[P], column int, values []pq.Value) {
	if column == 0 {
		fillKey(values, page)
	} else {
		fill(&s.Columns[column-1], column, values, page)
	}
}

// write appends rows, after checking each against the schema's value
// caps, writing each page as it fills.
func (w *writer[P]) write(rows []schema.Row[P]) error {
	for i := range rows {
		if err := w.check(&rows[i]); err != nil {
			return err
		}
		w.page = append(w.page, rows[i])
		if len(w.page) == pageRows {
			if err := w.flushPage(); err != nil {
				return err
			}
		}
	}
	return nil
}

// flushPage hands the buffered page to the module, which encodes it
// before it returns; the page is reused after.
func (w *writer[P]) flushPage() error {
	if len(w.page) == 0 {
		return nil
	}
	err := w.part.WritePage(len(w.page), w.fill)
	clear(w.page) // drop the rows' strings: the provider's arenas
	w.page = w.page[:0]
	return err
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
	err := w.flushPage()
	var info seekparquet.PartInfo
	if err == nil {
		info, err = w.part.Seal()
	} else {
		_ = w.part.Abort()
	}
	if err != nil {
		return dataset.Part{}, fmt.Errorf("close %s: %w", w.file.Name(), err)
	}
	return dataset.Part{Path: w.file.Name(), Identity: w.file.Identity(), Columns: w.cols, CreatedBy: w.by,
		PartInfo: dataset.PartInfo{Rows: info.Rows, Bytes: info.Bytes, SHA256: info.SHA256,
			MinKey: dataset.Key(info.MinKey), MaxKey: dataset.Key(info.MaxKey)}}, nil
}

// abort releases the writer without producing a part.
func (w *writer[P]) abort() {
	if !w.closed {
		w.closed = true
		_ = w.part.Abort()
	}
}
