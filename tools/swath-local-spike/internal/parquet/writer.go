// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package parquet

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	pq "github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/tools/swath-local-spike/internal/dataset"
	"github.com/varveio/swath/tools/swath-local-spike/internal/schema"
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
	createdBy       = "swath-local"
	createdVersion  = "0.0.0-spike"
)

// writer writes one sorted part in strictly ascending key order.
type writer struct {
	schema    *schema.Schema
	columns   func() int // goroutines encoding one page's columns (>= 1), read per page
	file      *os.File
	out       *tracking
	w         *pq.GenericWriter[any]
	codec     *codec
	values    [][]pq.Value
	page      []schema.Row
	rows      int64
	groupRows int64
	baseline  int64
	minKey    string
	maxKey    string
	closed    bool
}

// create opens path for part ordinal (0-based); file_final is decided at close.
func create(path string, s *schema.Schema, ordinal, maxColumns int, columns func() int) (*writer, error) {
	c, err := newCodec(max(1, maxColumns))
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		c.Close()
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
		pq.CreatedBy(createdBy, createdVersion, ""),
		pq.KeyValueMetadata(dataset.StampOrder, s.SortOrder),
		pq.KeyValueMetadata(dataset.StampMode, "objects"),
		pq.KeyValueMetadata(dataset.StampFormat, "1"),
		pq.KeyValueMetadata(dataset.StampFileIndex, strconv.Itoa(ordinal+1)),
	}
	out := &tracking{w: f, digest: md5.New()}
	w := &writer{
		schema: s, columns: columns, file: f, out: out, codec: c,
		w:    pq.NewGenericWriter[any](out, opts...),
		page: make([]schema.Row, 0, pageRows),
	}
	w.values = make([][]pq.Value, len(s.Columns))
	for i := range w.values {
		w.values[i] = make([]pq.Value, pageRows)
	}
	w.baseline = w.w.Size()
	return w, nil
}

// write appends one row; keys must be strictly ascending.
func (w *writer) write(r *schema.Row) error {
	if w.rows > 0 && r.Key <= w.maxKey {
		return fmt.Errorf("keys not strictly ascending: %q after %q", r.Key, w.maxKey)
	}
	if w.rows == 0 {
		w.minKey = r.Key
	}
	w.maxKey = r.Key
	w.rows++
	w.page = append(w.page, *r)
	if len(w.page) == pageRows {
		return w.flushPage()
	}
	return nil
}

func (w *writer) flushPage() error {
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
func (w *writer) encodePage() error {
	cols := w.w.ColumnWriters()
	if len(cols) != len(w.schema.Columns) {
		return fmt.Errorf("column count %d, schema has %d", len(cols), len(w.schema.Columns))
	}
	one := func(c int) error {
		vals := w.values[c][:len(w.page)]
		fill(&w.schema.Columns[c], c, vals, w.page)
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

func (w *writer) flushGroup() error {
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

// close flushes, stamps file_final when final, writes the footer, fsyncs and
// closes the file. Metadata is returned only if all of that succeeded.
func (w *writer) close(final bool) (dataset.Part, error) {
	if w.closed {
		return dataset.Part{}, errors.New("writer already closed")
	}
	w.closed = true
	err := w.flushPage()
	if err == nil {
		err = w.flushGroup()
	}
	if err == nil && final {
		w.w.SetKeyValueMetadata(dataset.StampFileFinal, "true")
	}
	if err == nil {
		err = w.w.Close()
	}
	err = errors.Join(err, w.codec.Close())
	if err == nil {
		err = w.file.Sync()
	}
	err = errors.Join(err, w.file.Close())
	if err != nil {
		return dataset.Part{}, fmt.Errorf("close %s: %w", w.file.Name(), err)
	}
	return dataset.Part{
		Path: w.file.Name(), Rows: w.rows, Bytes: w.out.n,
		MD5: hex.EncodeToString(w.out.digest.Sum(nil)), MinKey: w.minKey, MaxKey: w.maxKey,
	}, nil
}

// abort releases the writer without producing a part.
func (w *writer) abort() {
	if !w.closed {
		w.closed = true
		_ = w.codec.Close()
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
