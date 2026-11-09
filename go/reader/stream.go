// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"

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

// Stream hands fn every current object of the dataset (an S3 or Cloud
// Storage object, or a filesystem entry) in dataset order: ascending
// unsigned byte order of the keys. It reads the dataset with
// seekparquet.Range in pages, each continuing after the last row of the
// one before (its Cursor), projecting only the columns Object holds. Each
// Object owns its memory (its Key is a copy), so fn may keep or change it.
//
// A versions schema (swath/s3-versions/v1, swath/gcs-generations/v1) holds
// history, not current objects: Stream refuses it with a SemanticError of
// KindSchema before it reads. Each object must have a size, at least 0,
// and a modification time, when it has one, in the years 1 to 9999: a row
// that breaks either ends the stream with a SemanticError of KindRow
// naming the column and the row, so Size is never nil. A row the schema's
// decoder refuses ends it with the decoder's error.
//
// Stream stops at the first error: fn's, returned unchanged; the module's
// (a *seekparquet.RefusalError for data that breaks the specification, an
// error of the storage), returned unchanged; or the context's, checked
// before every object. The module checks every key page it decodes; the
// rows' other values are checked as decoded, and the parts' digests are
// not checked (seekparquet.Validate at LevelDigest checks them).
func (d *Dataset) Stream(ctx context.Context, fn func(Object) error) error {
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return semantic(KindSchema, "", nil, "%s holds versions, not current objects", d.spec.ID)
	}
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
			o, err := DecodeObject(d.spec.ID, r)
			if err == nil {
				err = checkObject(&o)
			}
			if err != nil {
				return err
			}
			o.Key = bytes.Clone(o.Key)
			objects++
			if err := fn(o); err != nil {
				return err
			}
		}
	}
}

// checkObject applies the checks varve's reader applied to every object it
// streamed: a size, not negative, and a modification time in years 1 to
// 9999.
func checkObject(o *Object) error {
	switch {
	case o.Size == nil:
		return semantic(KindRow, "size", &o.Position, "null: an object has a size")
	case *o.Size < 0:
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
	s := &d.stats.ReadStats
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
