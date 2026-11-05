// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"sync/atomic"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/swath/tools/swath-local-spike/record"
)

// Footer stamp keys and values, identical to Swath's SortedParquetWriter.
const (
	SortOrderKey     = "swath.sort.order"
	SortOrderValue   = "key_bytes_unsigned,version_id_null_first,row_type_rank"
	SortModeKey      = "swath.sort.mode"
	SortFormatKey    = "swath.sort.format_version"
	SortFileIndexKey = "swath.sort.file_index" // 1-based roll position
	SortFileFinalKey = "swath.sort.file_final" // "true" only on the last file
)

// Config is the served-layout geometry from the Oct spike.
type Config struct {
	PageRows   int    // rows per key page (1024)
	GroupBytes int64  // row-group byte target checked at page boundaries (8 MiB)
	GroupRows  int64  // row-group row ceiling
	Codec      string // go | go-entropy
	Fsync      bool   // fsync file before Close returns
	Encoders   int    // goroutines encoding the columns of each page (<=1 = serial)
}

func DefaultConfig() Config {
	return Config{PageRows: 1024, GroupBytes: 8 << 20, GroupRows: 1_000_000, Codec: "go-entropy", Fsync: true, Encoders: 1}
}

// PartInfo is close-gated metadata of one written file.
type PartInfo struct {
	Path   string
	Rows   int64
	Bytes  int64
	MD5    string
	MinKey string
	MaxKey string
	Groups int
}

// Writer writes one Parquet file from rows in strictly ascending key order.
type Writer struct {
	cfg       Config
	file      *os.File
	tracked   *trackingWriter
	w         *parquet.GenericWriter[schemaRow]
	cleanup   func() error
	cols      *columnValues
	page      []record.Row
	rows      int64
	groupRows int64
	baseline  int64
	groups    int
	minKey    string
	maxKey    string
	closed    bool
}

// Create opens path and a writer. sorted adds the Swath footer stamps;
// fileIndex is the 1-based roll position (file_final is decided at Close).
func Create(path string, cfg Config, sorted bool, fileIndex int) (*Writer, error) {
	codec, cleanup, err := newCodec(cfg.Codec, max(1, cfg.Encoders))
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		cleanup()
		return nil, err
	}
	opts := []parquet.WriterOption{
		Schema(),
		parquet.Compression(codec),
		parquet.DataPageVersion(2),
		parquet.DataPageStatistics(false),
		parquet.PageBufferSize(1 << 20),
		parquet.WriteBufferSize(4096),
		parquet.MaxRowsPerRowGroup(cfg.GroupRows),
		parquet.DictionaryMaxBytes(8 << 10),
		parquet.ColumnIndexSizeLimit(func([]string) int { return 1024 }),
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
		parquet.DefaultEncodingFor(parquet.Int64, &parquet.DeltaBinaryPacked),
		parquet.DefaultEncodingFor(parquet.Int32, &parquet.DeltaBinaryPacked),
		parquet.CreatedBy("swath-local", "0.0.0-spike", ""),
	}
	if sorted {
		opts = append(opts,
			parquet.KeyValueMetadata(SortOrderKey, SortOrderValue),
			parquet.KeyValueMetadata(SortModeKey, "objects"),
			parquet.KeyValueMetadata(SortFormatKey, "1"),
			parquet.KeyValueMetadata(SortFileIndexKey, fmt.Sprint(fileIndex)),
		)
	}
	tw := &trackingWriter{output: f, digest: md5.New()}
	w := &Writer{
		cfg: cfg, file: f, tracked: tw, cleanup: cleanup,
		w:    parquet.NewGenericWriter[schemaRow](tw, opts...),
		cols: newColumnValues(cfg.PageRows),
		page: make([]record.Row, 0, cfg.PageRows),
	}
	w.baseline = w.w.Size()
	return w, nil
}

// Write appends one row; keys must be strictly ascending.
func (w *Writer) Write(r *record.Row) error {
	if w.rows > 0 && r.Key <= w.maxKey {
		return fmt.Errorf("keys not strictly ascending: %q after %q", r.Key, w.maxKey)
	}
	if w.rows == 0 {
		w.minKey = r.Key
	}
	w.maxKey = r.Key
	w.rows++
	w.page = append(w.page, *r)
	if len(w.page) == w.cfg.PageRows {
		return w.flushPage()
	}
	return nil
}

func (w *Writer) flushPage() error {
	n := len(w.page)
	if n == 0 {
		return nil
	}
	if err := w.encodePage(); err != nil {
		return err
	}
	w.page = w.page[:0]
	w.groupRows += int64(n)
	estimate := w.w.Size() - w.baseline
	if (w.cfg.GroupBytes > 0 && estimate >= w.cfg.GroupBytes) || w.groupRows >= w.cfg.GroupRows {
		return w.flushGroup()
	}
	return nil
}

// encodePage fills, writes and flushes every column of the buffered page.
// Columns are independent in parquet-go (own buffers and page state; shared
// pools and the codec are concurrency-safe), so with Encoders > 1 goroutines
// take columns dynamically. The bytes produced are identical to the serial path.
func (w *Writer) encodePage() error {
	writers := w.w.ColumnWriters()
	if len(writers) != columnCount {
		return fmt.Errorf("column count differs: %d", len(writers))
	}
	n := len(w.page)
	one := func(c int) error {
		vals := w.cols.columns[c][:n]
		fillColumn(c, vals, w.page)
		return writeColumn(writers[c], vals)
	}
	if w.cfg.Encoders <= 1 {
		for c := 0; c < columnCount; c++ {
			if err := one(c); err != nil {
				return err
			}
		}
		return nil
	}
	var next atomic.Int32
	errs := make([]error, columnCount)
	var wg sync.WaitGroup
	for g := 0; g < min(w.cfg.Encoders, columnCount); g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := int(next.Add(1) - 1); c < columnCount; c = int(next.Add(1) - 1) {
				errs[c] = one(c)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (w *Writer) flushGroup() error {
	if w.groupRows == 0 {
		return nil
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	w.groups++
	w.groupRows = 0
	w.baseline = w.w.Size()
	return nil
}

// Rows written so far.
func (w *Writer) Rows() int64 { return w.rows }

// EstimatedBytes: bytes emitted plus the current group's buffered estimate.
func (w *Writer) EstimatedBytes() int64 { return w.w.Size() }

// MaxKey is the last key written.
func (w *Writer) MaxKey() string { return w.maxKey }

// Close flushes, stamps file_final when final, closes the footer, fsyncs and
// closes the file. Metadata is returned only after all of that succeeded.
func (w *Writer) Close(final bool) (PartInfo, error) {
	if w.closed {
		return PartInfo{}, errors.New("writer already closed")
	}
	w.closed = true
	err := w.flushPage()
	if err == nil {
		err = w.flushGroup()
	}
	if err == nil && final {
		w.w.SetKeyValueMetadata(SortFileFinalKey, "true")
	}
	if err == nil {
		err = w.w.Close()
	}
	err = errors.Join(err, w.cleanup())
	if err == nil && w.cfg.Fsync {
		err = w.file.Sync()
	}
	err = errors.Join(err, w.file.Close())
	if err != nil {
		return PartInfo{}, err
	}
	return PartInfo{
		Path: w.file.Name(), Rows: w.rows, Bytes: w.tracked.bytes,
		MD5: hex.EncodeToString(w.tracked.digest.Sum(nil)), MinKey: w.minKey, MaxKey: w.maxKey, Groups: w.groups,
	}, nil
}

// Abort closes without producing valid metadata (error paths).
func (w *Writer) Abort() {
	if !w.closed {
		w.closed = true
		_ = w.cleanup()
		_ = w.file.Close()
	}
}

type trackingWriter struct {
	output io.Writer
	digest hash.Hash
	bytes  int64
}

func (t *trackingWriter) Write(p []byte) (int, error) {
	n, err := t.output.Write(p)
	if n > 0 {
		_, _ = t.digest.Write(p[:n])
		t.bytes += int64(n)
	}
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
