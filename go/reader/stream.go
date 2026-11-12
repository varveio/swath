// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	seekparquet "github.com/varveio/seekparquet/go"
)

// StreamStats is the work of the Streams on a Dataset, summed over every
// request they made: the requests (the seekparquet.Dataset.Scan calls: one
// for each Stream that reaches the read, whether it succeeds, fails or
// finds its context ended, none for one refused before it), the objects
// handed to fn, and the module's counts (seekparquet.ReadStats, including
// a failed read's), which are diagnostics, not a contract.
type StreamStats struct {
	Requests int64
	Objects  int64
	ReadStats
}

// StreamStats returns the work of every Stream on the Dataset so far,
// including those that failed, up to where they stopped. A Stream's work,
// every field of it, is added once, when it returns: nothing of a Stream
// still running is in it.
func (d *Dataset) StreamStats() StreamStats {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return d.stats
}

// Stream hands fn every current object of the dataset (an S3, Cloud
// Storage or Azure Blob object) in dataset order: ascending unsigned byte
// order of the keys. It reads the dataset in one ordered pass of borrowed
// batches (seekparquet.Dataset.Scan, with no plan and no limit), projecting
// only the columns Object holds, and opens each part, row group and
// projected column chunk as the pass reaches it. Each Object owns its
// memory (its Key and strings are copies), so fn may keep or change it.
//
// A versions schema (swath/s3-versions/v1, swath/gcs-generations/v1) holds
// history, not current objects, and a filesystem's entries are
// StreamLocal's: Stream refuses both with a SemanticError of KindSchema
// before it reads. Each object must have a size, at least 0, and a
// modification time, when it has one, in the years 1 to 9999: a row that
// breaks either ends the stream with a SemanticError of KindRow naming the
// column and the row, so Size is never nil. A row the schema's decoder
// refuses ends it with the decoder's error.
//
// Stream stops at the first error: fn's, returned unchanged; the module's
// (a *seekparquet.RefusalError for data that breaks the specification, an
// error of the storage, ErrClosed), returned unchanged; or the context's,
// checked before every object. Objects already handed to fn stay handed:
// every object before a row that fails, and those of the pages before a
// part, page or read that fails. The module checks every key page it
// decodes; the rows' other values are checked as decoded, and the parts'
// digests are not checked (seekparquet.Validate at LevelDigest checks
// them).
//
// fn runs inside the read, which holds the Dataset open: Close from
// another goroutine waits for fn to return; if fn returns nil, the rest of
// its batch is still handed on and the stream then ends with ErrClosed.
// fn must not call Close, nor wait for a goroutine that does: that would
// deadlock.
func (d *Dataset) Stream(ctx context.Context, fn func(Object) error) error {
	columns, err := d.currentColumns()
	if err != nil {
		return err
	}
	values := make([]seekparquet.Value, len(columns))
	return streamRows(ctx, d, columns, func(b *seekparquet.Batch, i int) (Object, error) {
		return d.batchObject(b, i, columns, values)
	}, fn)
}

// batchObject decodes and checks row i of b as an owning Object, values
// its scratch. It decodes a borrowed view of the row: the decoder copies
// every value it keeps but the key, cloned here. An error's Position would
// borrow the key too, so a failing row is decoded again from an owning
// copy, giving the same error with its own key. values holds no borrowed
// value once it returns.
func (d *Dataset) batchObject(b *seekparquet.Batch, i int, columns []string, values []seekparquet.Value) (Object, error) {
	o, err := decodeObject(d.spec.ID, batchRow(b, i, columns, values))
	clear(values)
	if err != nil {
		_, err = decodeObject(d.spec.ID, b.Row(i))
		return Object{}, err
	}
	o.Key = bytes.Clone(o.Key)
	return o, nil
}

// batchRow returns row i of b as a seekparquet.Row borrowing the batch's
// key and values (b.Row(i) without the copies), its Values in values.
func batchRow(b *seekparquet.Batch, i int, columns []string, values []seekparquet.Value) seekparquet.Row {
	for c := range values {
		values[c] = b.Column(c).Value(i)
	}
	return seekparquet.Row{Key: b.Key(i), Offset: b.Offset(i), Columns: columns, Values: values}
}

// StreamLocal hands fn every entry of a swath/local-entries/v1 dataset in
// dataset order, as Stream hands objects: each entry with every column of
// the schema (LocalEntry), owning its memory, so fn may keep or change it.
// Any other schema id is refused with a SemanticError of KindSchema before
// any read. Nulls are kept: an entry listed without its stat (--metadata
// names) has a nil Size and LastModified. Its kind must be one of the
// schema's (UNKNOWN included), a size, when present, at least 0 and a
// modification time, when present, in the years 1 to 9999, else the
// stream ends with a SemanticError of KindRow. Its errors,
// statistics and checks are otherwise Stream's.
func (d *Dataset) StreamLocal(ctx context.Context, fn func(LocalEntry) error) error {
	if d.spec.ID != LocalV1 {
		return semantic(KindSchema, "", nil, "%s holds no filesystem entries", d.spec.ID)
	}
	return streamRows(ctx, d, d.spec.ColumnNames(), func(b *seekparquet.Batch, i int) (LocalEntry, error) {
		return decodeLocal(b.Row(i)) // an owning row: the entry owns its key and link target
	}, fn)
}

// streamRows reads the dataset in one ordered pass of batches projecting
// columns, one unlimited request, and hands fn what decode returns for
// each row. An error of the callback (the context's, decode's or fn's) is
// returned as it is, never wrapped by the module's ErrClosed. The stream's
// statistics are added to the Dataset's once, on return.
func streamRows[T any](ctx context.Context, d *Dataset, columns []string, decode func(*seekparquet.Batch, int) (T, error), fn func(T) error) error {
	var stats StreamStats
	defer func() {
		d.statsMu.Lock()
		defer d.statsMu.Unlock()
		d.stats.Requests += stats.Requests
		d.stats.Objects += stats.Objects
		d.stats.ReadStats.Add(stats.ReadStats)
	}()
	var stop error
	each := func(b *seekparquet.Batch, i int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		v, err := decode(b, i)
		if err != nil {
			return err
		}
		stats.Objects++
		return fn(v)
	}
	err := d.sp.Scan(ctx, seekparquet.Query{Columns: columns}, &stats.ReadStats, func(b *seekparquet.Batch) error {
		for i := range b.Len() {
			if stop = each(b, i); stop != nil {
				return stop
			}
		}
		return nil
	})
	stats.Requests++
	if stop != nil {
		return stop
	}
	return err
}

// currentColumns returns the columns Stream and Scan project, refusing a
// schema that holds versions or filesystem entries.
func (d *Dataset) currentColumns() ([]string, error) {
	if d.spec.ID == LocalV1 {
		return nil, semantic(KindSchema, "", nil, "%s holds filesystem entries, not objects: read them with StreamLocal", d.spec.ID)
	}
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return nil, notCurrent(d.spec.ID)
	}
	return columns, nil
}

// decodeObject decodes and checks a current object, its Key the row's.
func decodeObject(id SchemaID, r seekparquet.Row) (Object, error) {
	return decodeObjectFields(id, r, AllObjectFields)
}

// decodeObjectFields is decodeObject of the fields of fields alone, the
// others left nil (decodeFields).
func decodeObjectFields(id SchemaID, r seekparquet.Row, fields ObjectFields) (Object, error) {
	columns := objectColumns(id)
	if columns == nil {
		return DecodeObject(id, r)
	}
	o, err := decodeFields(id, r, columns, fields)
	if err == nil {
		err = checkObject(&o, false)
	}
	return o, err
}

// localKinds are the values of swath/local-entries/v1's kind column.
var localKinds = []string{"FILE", "DIR", "SYMLINK", "FIFO", "SOCKET", "CHAR_DEVICE", "BLOCK_DEVICE", "UNKNOWN"}

// decodeLocal decodes and checks a filesystem entry, its Key the row's: a
// kind of the schema's, and a size and time as checkObject checks them.
func decodeLocal(r seekparquet.Row) (LocalEntry, error) {
	e, err := DecodeLocalEntry(r)
	if err == nil && !slices.Contains(localKinds, e.Kind) {
		err = semantic(KindRow, "kind", &e.Position, "%q is not a kind of %s", e.Kind, LocalV1)
	}
	if err == nil {
		err = checkObject(&Object{Position: e.Position, Size: e.Size, LastModified: e.LastModified}, true)
	}
	return e, err
}

// checkObject applies the checks varve's reader applied to every object it
// streamed: a size, not negative, and a modification time in years 1 to
// 9999. A filesystem entry (unsized) may lack its size.
func checkObject(o *Object, unsized bool) error {
	var size int64
	if o.Size != nil {
		size = *o.Size
	}
	if err := checkSize(size, o.Size != nil, unsized); err != nil {
		return &SemanticError{Kind: KindRow, Column: "size", Row: &o.Position, Err: err}
	}
	if o.LastModified != nil && !objectMicrosInRange(o.LastModified.UnixMicro()) {
		return semantic(KindRow, "last_modified", &o.Position, "%d µs is outside the years 1 to 9999", o.LastModified.UnixMicro())
	}
	return nil
}

// checkSize is shared by the owning and borrowed readers. Filesystem
// entries (unsized) may lack a size; present sizes must be non-negative.
func checkSize(size int64, present, unsized bool) error {
	if !present && !unsized {
		return fmt.Errorf("null: an object has a size")
	}
	if present && size < 0 {
		return fmt.Errorf("%d is negative", size)
	}
	return nil
}

// UTC microseconds for 0001-01-01 (inclusive) and 10000-01-01 (exclusive).
const minObjectMicros int64 = -62135596800000000
const endObjectMicros int64 = 253402300800000000

// objectMicrosInRange implements the years 1..9999 check without constructing
// a time.Time for each borrowed row. Owning rows use the same bounds.
func objectMicrosInRange(micros int64) bool {
	return micros >= minObjectMicros && micros < endObjectMicros
}

// addStats adds a request's statistics to s, when s is not nil.
func addStats(s *ReadStats, st ReadStats) {
	if s != nil {
		s.Add(st)
	}
}
