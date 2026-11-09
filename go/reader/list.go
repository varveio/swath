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
	// Delimiter is the single delimiter byte. 0 is the NUL byte, a
	// delimiter like any other: there is no listing without rollup.
	Delimiter byte
	// Cursor, when set, continues the listing after an entry it returned
	// (Entry.Cursor). A cursor before Prefix's keys lists from the first
	// of them; one after them lists nothing.
	Cursor *Cursor
	// Limit is the most entries returned. It must be positive.
	Limit int
	// Stats, when not nil, receives the request's read statistics: reset
	// when the request starts, and set whether it succeeds or fails.
	Stats *ReadStats
}

// Entry is one listing entry: an object (Object set, CommonPrefix nil) or
// a common prefix (CommonPrefix set). A common prefix also has Object set
// when the object stored at the prefix's exact key is the first the
// listing reached under it (an S3-style "a/" folder object): the prefix
// and that object, from one request. Both own their bytes.
//
// The pairing is exact whenever the listing reaches the prefix's first
// key: without a cursor, and after any cursor an earlier page returned. A
// prefix cursor resumes after every key under its prefix, so the next
// prefix starts at its first key; a row cursor is an object's key, which
// never rolls up to a prefix of the same listing. Only a cursor made up
// inside a prefix's keys (a row cursor, or a prefix cursor for a deeper
// prefix) skips the object at its exact key, and that prefix is then
// returned without it.
type Entry struct {
	Object       *Object
	CommonPrefix []byte
}

// Cursor returns the cursor after this entry, owning its bytes: after an
// object, its key and offset; after a common prefix, the prefix, so the
// listing continues after every key that rolls up to it, the object at
// its exact key included.
func (e Entry) Cursor() *Cursor {
	if e.CommonPrefix == nil {
		return e.Object.Cursor()
	}
	return &Cursor{Prefix: bytes.Clone(e.CommonPrefix)}
}

// List lists the dataset's current objects by delimiter, in ascending
// unsigned byte order of the keys and prefixes: one request of the
// module's delimited listing (seekparquet.Dataset.List), which routes it,
// skips each common prefix's keys and rolls them up, projecting the
// columns Object holds. Repeating the request with the Cursor of each
// page's last entry returns every entry once. A common prefix carries the
// object at its exact key (Entry) from the module's exact row
// (seekparquet.Entry.Exact), without another request.
//
// A versions schema is refused with a SemanticError of KindSchema before
// any read, as Stream refuses it. Each object, a prefix's included, is
// decoded and checked as Stream checks it: a row that breaks a check fails
// the listing with the SemanticError naming the column and the row, and no
// entries. The module's errors are returned unchanged: a *RefusalError,
// ErrClosed after Close, an error wrapping ErrInvalidRequest (a Limit that
// is not positive, a malformed Cursor), the Source's, and the context's.
func (d *Dataset) List(ctx context.Context, req ListRequest) ([]Entry, error) {
	if req.Stats != nil {
		*req.Stats = ReadStats{}
	}
	columns := objectColumns(d.spec.ID)
	if columns == nil {
		return nil, notCurrent(d.spec.ID)
	}
	got, err := d.sp.List(ctx, seekparquet.List{Prefix: req.Prefix, Delimiter: req.Delimiter, Cursor: req.Cursor,
		Limit: req.Limit, Columns: columns, Stats: req.Stats})
	if err != nil {
		return nil, err
	}
	out := make([]Entry, len(got))
	for i, e := range got {
		row := e.Row
		if row == nil {
			out[i].CommonPrefix = bytes.Clone(e.CommonPrefix)
			if row = e.Exact; row == nil {
				continue
			}
		}
		o, err := DecodeObject(d.spec.ID, *row)
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
