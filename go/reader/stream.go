// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"
	"slices"

	seekparquet "github.com/varveio/seekparquet/go"
)

// streamPageRows is the most rows one Range request of Stream returns.
const streamPageRows = 1024

// StreamStats is the work of the Streams on a Dataset, summed over every
// Range request they made: the requests, the objects handed to fn, and the
// module's counts (seekparquet.ReadStats), which are diagnostics, not a
// contract.
type StreamStats struct {
	Requests int64
	Objects  int64
	ReadStats
}

// StreamStats returns the work of every Stream on the Dataset so far,
// including those that failed, up to where they stopped.
func (d *Dataset) StreamStats() StreamStats {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	return d.stats
}

// Stream hands fn every current object of the dataset (an S3, Cloud
// Storage or Azure Blob object) in dataset order: ascending unsigned byte
// order of the keys. It reads the dataset with seekparquet.Range in pages,
// each continuing after the last row of the one before (its Cursor),
// projecting only the columns Object holds. Each Object owns its memory
// (its Key is a copy), so fn may keep or change it.
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
// error of the storage), returned unchanged; or the context's, checked
// before every object. The module checks every key page it decodes; the
// rows' other values are checked as decoded, and the parts' digests are
// not checked (seekparquet.Validate at LevelDigest checks them).
func (d *Dataset) Stream(ctx context.Context, fn func(Object) error) error {
	columns, err := d.currentColumns()
	if err != nil {
		return err
	}
	return streamRows(ctx, d, columns, func(r seekparquet.Row) (Object, error) {
		o, err := decodeObject(d.spec.ID, r)
		o.Key = bytes.Clone(o.Key)
		return o, err
	}, fn)
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
	return streamRows(ctx, d, d.spec.ColumnNames(), func(r seekparquet.Row) (LocalEntry, error) {
		e, err := decodeLocal(r)
		e.Key = bytes.Clone(e.Key)
		return e, err
	}, fn)
}

// streamRows reads the dataset in Range pages projecting columns, and
// hands fn each row decode returns.
func streamRows[T any](ctx context.Context, d *Dataset, columns []string, decode func(seekparquet.Row) (T, error), fn func(T) error) error {
	var objects int64
	defer func() {
		d.statsMu.Lock()
		defer d.statsMu.Unlock()
		d.stats.Objects += objects
	}()
	var cursor *seekparquet.Cursor
	for {
		var st seekparquet.ReadStats
		rows, err := d.sp.Range(ctx, seekparquet.Range{Cursor: cursor, Limit: streamPageRows, Columns: columns, Stats: &st})
		d.addRead(&st)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		cursor = rows[len(rows)-1].Cursor()
		for _, r := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			v, err := decode(r)
			if err != nil {
				return err
			}
			objects++
			if err := fn(v); err != nil {
				return err
			}
		}
	}
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
	o, err := DecodeObject(id, r)
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
	switch {
	case o.Size == nil && !unsized:
		return semantic(KindRow, "size", &o.Position, "null: an object has a size")
	case o.Size != nil && *o.Size < 0:
		return semantic(KindRow, "size", &o.Position, "%d is negative", *o.Size)
	case o.LastModified != nil && (o.LastModified.Year() < 1 || o.LastModified.Year() > 9999):
		return semantic(KindRow, "last_modified", &o.Position, "%d µs is outside the years 1 to 9999", o.LastModified.UnixMicro())
	}
	return nil
}

// addRead adds one request's statistics to the Dataset's.
func (d *Dataset) addRead(st *seekparquet.ReadStats) {
	d.statsMu.Lock()
	defer d.statsMu.Unlock()
	d.stats.Requests++
	addStats(&d.stats.ReadStats, *st)
}

// addStats adds a request's statistics to s, when s is not nil.
func addStats(s *ReadStats, st ReadStats) {
	if s == nil {
		return
	}
	s.PartsOpened += st.PartsOpened
	s.KeyPagesDecoded += st.KeyPagesDecoded
	s.ValuePagesDecoded += st.ValuePagesDecoded
	s.KeysDecoded += st.KeysDecoded
	s.RowGroupsOpened += st.RowGroupsOpened
	s.IndexLoads += st.IndexLoads
	s.PageIndexSeeks += st.PageIndexSeeks
	s.PrefixSkips += st.PrefixSkips
	s.GroupsSkipped += st.GroupsSkipped
	s.SourceReads += st.SourceReads
	s.SourceBytes += st.SourceBytes
}
