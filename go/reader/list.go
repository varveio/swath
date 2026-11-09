// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"context"

	seekparquet "github.com/varveio/seekparquet/go"
)

// ListRequest is a delimited listing of current objects: the objects whose
// keys begin with Prefix and, in place of the keys whose bytes after Prefix
// contain Delimiter, the common prefix up to and including the first
// delimiter, after Cursor when it is set, at most Limit entries.
type ListRequest struct {
	// Prefix scopes the listing; empty lists every key.
	Prefix []byte
	// Delimiter is the single delimiter byte.
	Delimiter byte
	// Cursor, when set, continues the listing after an entry it returned
	// (Entry.Cursor).
	Cursor *Cursor
	// Limit is the most entries returned. It must be positive.
	Limit int
	// Stats, when not nil, receives the request's read statistics, set
	// whether it succeeds or fails.
	Stats *ReadStats
}

// Entry is one listing entry: exactly one of Object and CommonPrefix is
// set. Both own their bytes.
type Entry struct {
	Object       *Object
	CommonPrefix []byte
}

// Cursor returns the cursor after this entry, owning its bytes: after an
// object, its key and offset; after a common prefix, the prefix, so the
// listing continues after every key that rolls up to it.
func (e Entry) Cursor() *Cursor {
	if e.Object != nil {
		return e.Object.Cursor()
	}
	return &Cursor{Prefix: bytes.Clone(e.CommonPrefix)}
}

// List lists the dataset's current objects by delimiter, in ascending
// unsigned byte order of the keys and prefixes: one request of the
// module's delimited listing (seekparquet.Dataset.List), which routes it,
// skips each common prefix's keys and rolls them up, projecting the
// columns Object holds. Repeating the request with the Cursor of each
// page's last entry returns every entry once.
//
// A versions schema is refused with a SemanticError of KindSchema before
// any read, as Stream refuses it. Each object is decoded and checked as
// Stream checks it: a row that breaks a check fails the listing with the
// SemanticError naming the column and the row, and no entries. The
// module's errors are returned unchanged: a *RefusalError, ErrClosed after
// Close, an error wrapping ErrInvalidRequest (a Limit that is not
// positive, a malformed Cursor), the Source's, and the context's.
func (d *Dataset) List(ctx context.Context, req ListRequest) ([]Entry, error) {
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return nil, semantic(KindSchema, "", nil, "%s holds versions, not current objects", d.spec.ID)
	}
	got, err := d.sp.List(ctx, seekparquet.List{Prefix: req.Prefix, Delimiter: req.Delimiter, Cursor: req.Cursor,
		Limit: req.Limit, Columns: columns, Stats: req.Stats})
	if err != nil {
		return nil, err
	}
	out := make([]Entry, len(got))
	for i, e := range got {
		if e.Row == nil {
			out[i].CommonPrefix = bytes.Clone(e.CommonPrefix)
			continue
		}
		o, err := DecodeObject(d.spec.ID, *e.Row)
		if err == nil {
			err = checkObject(&o)
		}
		if err != nil {
			return nil, err
		}
		o.Key = bytes.Clone(o.Key)
		out[i].Object = &o
	}
	return out, nil
}
