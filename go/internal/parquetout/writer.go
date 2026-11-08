// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquetout

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/go/internal/dataset"
	"github.com/varveio/swath/go/internal/faults"
	"github.com/varveio/swath/go/internal/schema"
)

// Served-layout geometry (Swath's sorted output).
const (
	pageRows        = 1024    // rows per page; the byte-target check runs at page boundaries
	groupBytes      = 8 << 20 // row-group target, on the writer's size estimate
	groupRows       = 1_000_000
	dictionaryBytes = 8 << 10 // dictionary limit per column chunk
	indexBoundBytes = 1024    // column-index min/max truncation
	pageBufferBytes = 1 << 20 // parquet-go page buffer
	writeBuffer     = 4096    // parquet-go output buffer
	writeQuantum    = 64      // parquet-go's WriteRows batching, kept for identical pages
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
// docs/internals/parquet-output.md).
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
//     attempt besides). One S3 writer at 1,024-byte keys measured 13 MiB,
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
// bytes apart from the build: parquet-go v0.32.0, v2 pages of 1,024 rows,
// 8 MiB row groups, 8 KiB dictionaries, 1,024-byte index bounds, delta keys
// and etags, delta integers with standard INT annotations, zstd
// SpeedDefault with entropy coding, and parts cut at size/0.1 for part 0,
// then at the frozen ratio. Bump the revision with any change to these.
// A resume refuses a journal written under another recipe.
//
// Versions are separate: the dataset format (dataset.FormatVersion), the
// logical schema (schema IDs), the private journal (journal.Version) and
// this recipe. created_by names the real build, so bytes are identical
// within a build; across builds only the recipe is promised.
const Recipe = "swath-go/parquet-r2"

// WriterID is the resume identity's writer field.
const WriterID = Recipe

// createdBy is the footer's created_by: the tool, its module version and
// its VCS revision, from the build itself.
var createdBy, createdVersion, createdBuild = build()

func build() (app, version, rev string) {
	app, version = "swath-go", "devel"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	if v := info.Main.Version; v != "" && v != "(devel)" {
		version = v
	}
	dirty := false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "-dirty"
	}
	return
}

// writer writes one sorted part in strictly ascending key order.
type writer[P any] struct {
	schema    *schema.Schema[P]
	columns   func() int // goroutines encoding one page's columns (>= 1), read per page
	file      *os.File
	out       *tracking
	w         *pq.GenericWriter[any]
	codec     *codec
	ownCodec  bool  // created for this writer (else the router's, shared)
	capped    []int // the columns with a Spec.MaxBytes, enforced as the key's cap is
	values    [][]pq.Value
	page      []schema.Row[P]
	rows      int64
	groupRows int64
	baseline  int64
	minKey    string
	maxKey    string
	closed    bool
}

// create opens path for part ordinal (0-based), compressing with c (the
// router's, shared by its writers), or, nil, with a codec of its own for
// maxColumns goroutines.
func create[P any](open func(string) (*os.File, error), path string, s *schema.Schema[P], ordinal int, c *codec, maxColumns int, columns func() int) (*writer[P], error) {
	own := c == nil
	if own {
		var err error
		if c, err = newCodec(max(1, maxColumns)); err != nil {
			return nil, err
		}
	}
	closeCodec := func() {
		if own {
			c.Close()
		}
	}
	if open == nil {
		open = func(path string) (*os.File, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		}
	}
	f, err := open(path)
	if err != nil {
		closeCodec()
		return nil, fmt.Errorf("create part: %w", err)
	}
	opts := []pq.WriterOption{
		nodeOf(s),
		pq.Compression(c),
		pq.DataPageVersion(2),
		pq.DataPageStatistics(false),
		pq.PageBufferSize(pageBufferBytes),
		pq.WriteBufferSize(writeBuffer),
		pq.MaxRowsPerRowGroup(groupRows),
		pq.DictionaryMaxBytes(dictionaryBytes),
		pq.ColumnIndexSizeLimit(func([]string) int { return indexBoundBytes }),
		pq.DefaultEncodingFor(pq.ByteArray, &pq.RLEDictionary),
		pq.DefaultEncodingFor(pq.Int64, &pq.DeltaBinaryPacked),
		pq.DefaultEncodingFor(pq.Int32, &pq.DeltaBinaryPacked),
		pq.DefaultEncodingFor(pq.Boolean, &pq.Plain),
		pq.CreatedBy(createdBy, createdVersion, createdBuild),
		pq.KeyValueMetadata(dataset.StampProfile, dataset.Format),
		pq.KeyValueMetadata(dataset.StampProfileVersion, strconv.Itoa(dataset.FormatVersion)),
		pq.KeyValueMetadata(dataset.StampSchema, s.ID),
		pq.KeyValueMetadata(dataset.StampIndexBound, strconv.Itoa(indexBoundBytes)),
		// Standard Parquet sorting_columns on every row group (metadata only:
		// rows arrive sorted; GenericWriter does not reorder them).
		pq.SortingWriterConfig(pq.SortingColumns(pq.Ascending("key"))),
	}
	out := &tracking{w: faults.Writer(f), digest: md5.New()}
	w := &writer[P]{
		schema: s, columns: columns, file: f, out: out, codec: c, ownCodec: own,
		w:    pq.NewGenericWriter[any](out, opts...),
		page: make([]schema.Row[P], 0, pageRows),
	}
	for i := range s.Columns {
		if s.Columns[i].MaxBytes > 0 && s.Columns[i].Str != nil {
			w.capped = append(w.capped, i)
		}
	}
	w.values = make([][]pq.Value, len(s.Columns)+1)
	for i := range w.values {
		w.values[i] = make([]pq.Value, pageRows)
	}
	w.baseline = w.w.Size()
	return w, nil
}

// write appends one row; keys must be strictly ascending.
func (w *writer[P]) write(r *schema.Row[P]) error {
	if n := w.schema.MaxKeyBytes; n > 0 && len(r.Key) > n {
		return fmt.Errorf("%w: %d bytes (limit %d): %.64q…", schema.ErrKeyTooLong, len(r.Key), n, r.Key)
	}
	for _, c := range w.capped {
		col := &w.schema.Columns[c]
		if v, ok := col.Str(&r.Data); ok && len(v) > col.MaxBytes {
			return fmt.Errorf("%w: %s of %.64q: %d bytes (limit %d)", schema.ErrValueTooLong, col.Name, r.Key, len(v), col.MaxBytes)
		}
	}
	if w.rows > 0 && r.Key <= w.maxKey {
		return fmt.Errorf("keys not strictly ascending: %q after %q", r.Key, w.maxKey)
	}
	if w.rows == 0 {
		w.minKey = strings.Clone(r.Key) // outlives the part: do not pin the provider's arena
	}
	w.maxKey = r.Key
	w.rows++
	w.page = append(w.page, *r)
	if len(w.page) == pageRows {
		return w.flushPage()
	}
	return nil
}

func (w *writer[P]) flushPage() error {
	if len(w.page) == 0 {
		return nil
	}
	if err := w.encodePage(); err != nil {
		return err
	}
	w.groupRows += int64(len(w.page))
	w.page = w.page[:0]
	if w.w.Size()-w.baseline >= groupBytes || w.groupRows >= groupRows {
		return w.flushGroup()
	}
	return nil
}

// encodePage fills, writes and flushes every column of the buffered page.
// parquet-go column writers are independent (own buffers and page state;
// the shared pools and the codec are concurrency-safe), so several
// goroutines may take columns; the bytes equal the serial result.
func (w *writer[P]) encodePage() error {
	cols := w.w.ColumnWriters()
	if len(cols) != len(w.schema.Columns)+1 {
		return fmt.Errorf("column count %d, schema has %d", len(cols), len(w.schema.Columns)+1)
	}
	one := func(c int) error {
		vals := w.values[c][:len(w.page)]
		if c == 0 {
			fillKey(vals, w.page)
		} else {
			fill(&w.schema.Columns[c-1], c, vals, w.page)
		}
		return writeColumn(cols[c], vals)
	}
	g := min(w.columns(), len(cols))
	if g <= 1 {
		for c := range cols {
			if err := one(c); err != nil {
				return err
			}
		}
		return nil
	}
	var next atomic.Int32
	errs := make([]error, len(cols))
	var wg sync.WaitGroup
	for range g {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := int(next.Add(1) - 1); c < len(cols); c = int(next.Add(1) - 1) {
				errs[c] = one(c)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func writeColumn(cw *pq.ColumnWriter, values []pq.Value) error {
	for written := 0; written < len(values); {
		end := written + min(writeQuantum, len(values)-written)
		n, err := cw.WriteRowValues(values[written:end])
		if err != nil {
			return err
		}
		if n != end-written {
			return io.ErrShortWrite
		}
		written = end
	}
	return cw.Flush()
}

func (w *writer[P]) flushGroup() error {
	if w.groupRows == 0 {
		return nil
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.groupRows = 0
	w.baseline = w.w.Size()
	return nil
}

// close flushes, writes the footer, fsyncs and closes the file. Metadata is
// returned only if all of that succeeded.
func (w *writer[P]) close() (dataset.Part, error) {
	if w.closed {
		return dataset.Part{}, errors.New("writer already closed")
	}
	w.closed = true
	err := w.flushPage()
	if err == nil {
		err = w.flushGroup()
	}
	if err == nil {
		err = w.w.Close()
	}
	if w.ownCodec {
		err = errors.Join(err, w.codec.Close())
	}
	if err == nil {
		err = faults.Sync(w.file)
	}
	// The directory entry is made durable by the committer that claims
	// the part for its record (listing), not here.
	err = errors.Join(err, w.file.Close())
	if err != nil {
		return dataset.Part{}, fmt.Errorf("close %s: %w", w.file.Name(), err)
	}
	return dataset.Part{
		Path: w.file.Name(), PartInfo: dataset.PartInfo{Rows: w.rows, Bytes: w.out.n,
			MD5: hex.EncodeToString(w.out.digest.Sum(nil)), MinKey: dataset.Key(w.minKey), MaxKey: dataset.Key(strings.Clone(w.maxKey))},
	}, nil
}

// abort releases the writer without producing a part.
func (w *writer[P]) abort() {
	if !w.closed {
		w.closed = true
		if w.ownCodec {
			_ = w.codec.Close()
		}
		_ = w.file.Close()
	}
}

// tracking counts and hashes the bytes written.
type tracking struct {
	w      io.Writer
	digest hash.Hash
	n      int64
}

func (t *tracking) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 {
		_, _ = t.digest.Write(p[:n])
		t.n += int64(n)
	}
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}

// PartRows is the row count in a part file's footer. A resume reads it from
// a claimed part to cut the reproduced part at the same boundary
// (Options.Cut); the byte comparison that follows decides, so a damaged
// footer can only make that comparison fail. f is the part, opened by the
// caller (recovery opens it through the output root).
func PartRows(f *os.File) (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := pq.OpenFile(f, st.Size(), pq.SkipPageIndex(true), pq.SkipBloomFilters(true))
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}
