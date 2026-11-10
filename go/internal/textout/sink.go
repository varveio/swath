// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/schema"
)

// KeyColumn is the key's column name, the first of every schema's.
const KeyColumn = "key"

// Base64Suffix names a JSONL member that carries a string or binary
// column's bytes in standard base64, in place of the column's own member,
// when they are not UTF-8 (NAME_base64). No schema column may end in it.
const Base64Suffix = "_base64"

// The columns the table reads by their dataset names, when the schema has
// them.
const (
	sizeColumn  = "size"
	mtimeColumn = "last_modified"
	kindColumn  = "kind"
)

// Sink writes the order stage's ascending segments as text rows of the
// schema: the key, then the schema's columns in its order under the
// dataset's column names (or Config.Columns, in their order), each value as
// the schema stores it. TSV is one header line of the names and a line of
// fields per row (appendTSV); JSONL one object per row, its members in
// column order, a null JSON null, and a string or binary value that is not
// UTF-8 as NAME_base64. The table, for people, is the time in local time,
// the size in binary units, the kind and version columns where the schema
// has them, and the key last.
type Sink[P any] struct {
	o     *Out
	cols  []column[P] // TSV and JSONL
	table *table[P]   // the table
	line  []byte
	pipe  *pipeline[P] // parallel formatters (Config.Formatters > 1), else nil
}

// column is one TSV or JSONL column: its name and its accessor (none for
// the key, which the row holds).
type column[P any] struct {
	typ  schema.Type
	str  func(*P) (string, bool)
	num  func(*P) (int64, bool)
	flag func(*P) (bool, bool)
	// member and member64 are the JSONL member openings: `"NAME":` and
	// `"NAME_base64":"`.
	name, member, member64 string
	key                    bool
}

// ProjectionError refuses a projection (Config.Columns) the schema cannot
// give: the caller's to fix, found before any row is listed.
type ProjectionError struct{ msg string }

func (e *ProjectionError) Error() string { return e.msg }

// NewSink binds o to schema s; a TSV output gets its header line here. It
// refuses a projection (Config.Columns) that names a column s lacks, or
// names one twice, listing s's columns, before any row is written
// (ProjectionError).
func NewSink[P any](o *Out, s *schema.Schema[P]) (*Sink[P], error) {
	vers, err := schema.BindVersions(s)
	if err != nil {
		return nil, fmt.Errorf("text output: %w", err)
	}
	all := make([]column[P], 0, len(s.Columns)+1)
	names := make([]string, 0, len(s.Columns)+1)
	all = append(all, column[P]{typ: s.Key, key: true})
	names = append(names, KeyColumn)
	for _, c := range s.Columns {
		if strings.HasSuffix(c.Name, Base64Suffix) {
			return nil, fmt.Errorf("text output: schema %s: column %s ends in %s, which JSONL reserves", s.ID, c.Name, Base64Suffix)
		}
		all = append(all, column[P]{typ: c.Type, str: c.Str, num: c.Int, flag: c.Bool})
		names = append(names, c.Name)
	}
	for i, n := range names {
		all[i].name, all[i].member, all[i].member64 = n, `"`+n+`":`, `"`+n+Base64Suffix+`":"`
	}
	k := &Sink[P]{o: o}
	switch {
	case o.cfg.Format == Table && len(o.cfg.Columns) > 0:
		return nil, errors.New("text output: the table's columns are fixed; columns are chosen for TSV and JSONL")
	case o.cfg.Format == Table:
		k.table = newTable(s, vers, o.cfg.Loc)
		o.cfg.Formatters = 1 // its widths grow with the rows it formats
		if _, err := o.bw.Write(k.table.header(nil)); err != nil {
			return nil, err
		}
	case len(o.cfg.Columns) == 0:
		k.cols = all
	default:
		for i, n := range o.cfg.Columns {
			j := slices.Index(names, n)
			if j < 0 {
				return nil, &ProjectionError{fmt.Sprintf("no column %q in %s (its columns: %s)", n, s.ID, strings.Join(names, ", "))}
			}
			if slices.Contains(o.cfg.Columns[:i], n) {
				return nil, &ProjectionError{fmt.Sprintf("column %q named twice", n)}
			}
			k.cols = append(k.cols, all[j])
		}
	}
	if o.cfg.Format == TSV {
		var h []byte
		for i := range k.cols {
			if i > 0 {
				h = append(h, '\t')
			}
			h = append(h, k.cols[i].name...)
		}
		if _, err := o.bw.Write(append(h, '\n')); err != nil {
			return nil, err
		}
	}
	if o.cfg.Formatters > 1 {
		k.pipe = startPipeline(k, s)
		o.pipe = k.pipe
	}
	return k, nil
}

// Write implements order.Sink.
func (k *Sink[P]) Write(ctx context.Context, segs [][]schema.Row[P]) error {
	t := time.Now()
	defer func() { k.o.sink += time.Since(t) }()
	if k.pipe != nil {
		return k.pipe.send(ctx, segs)
	}
	for _, seg := range segs {
		for i := range seg {
			k.line = k.appendRow(k.line[:0], &seg[i])
			if _, err := k.o.bw.Write(k.line); err != nil {
				return err
			}
		}
		k.o.rows += int64(len(seg))
	}
	return k.o.endBatch()
}

// appendRow appends r's line to b. For TSV and JSONL it reads only what
// NewSink bound, so formatters call it at once; the table, whose widths
// grow, is formatted by the order stage alone.
func (k *Sink[P]) appendRow(b []byte, r *schema.Row[P]) []byte {
	switch k.o.cfg.Format {
	case TSV:
		for i := range k.cols {
			if i > 0 {
				b = append(b, '\t')
			}
			b = k.cols[i].appendTSV(b, r)
		}
	case JSONL:
		b = append(b, '{')
		for i := range k.cols {
			if i > 0 {
				b = append(b, ',')
			}
			b = k.cols[i].appendJSON(b, r)
		}
		b = append(b, '}')
	case Table:
		b = k.table.row(b, r)
	}
	return append(b, '\n')
}

// nullTSV is a null value's TSV field.
const nullTSV = `\N`

func (c *column[P]) appendTSV(b []byte, r *schema.Row[P]) []byte {
	p := &r.Data
	switch {
	case c.key:
		return appendTSV(b, r.Key)
	case c.str != nil:
		v, ok := c.str(p)
		if !ok {
			return append(b, nullTSV...)
		}
		return appendTSV(b, v)
	case c.flag != nil:
		v, ok := c.flag(p)
		if !ok {
			return append(b, nullTSV...)
		}
		return strconv.AppendBool(b, v)
	default:
		v, ok := c.num(p)
		if !ok {
			return append(b, nullTSV...)
		}
		return appendNumber(b, c.typ, v)
	}
}

func (c *column[P]) appendJSON(b []byte, r *schema.Row[P]) []byte {
	p := &r.Data
	var v string
	switch {
	case c.key:
		v = r.Key
	case c.str != nil:
		var ok bool
		if v, ok = c.str(p); !ok {
			return append(append(b, c.member...), "null"...)
		}
	case c.flag != nil:
		f, ok := c.flag(p)
		if !ok {
			return append(append(b, c.member...), "null"...)
		}
		return strconv.AppendBool(append(b, c.member...), f)
	default:
		n, ok := c.num(p)
		b = append(b, c.member...)
		switch {
		case !ok:
			return append(b, "null"...)
		case c.typ == schema.TimestampMicros:
			return append(appendTime(append(b, '"'), n), '"')
		}
		return appendNumber(b, c.typ, n)
	}
	if !utf8.ValidString(v) {
		return append(base64.StdEncoding.AppendEncode(append(b, c.member64...), []byte(v)), '"')
	}
	return appendJSONString(append(b, c.member...), v)
}

// appendNumber appends an integer or time column's value: an unsigned
// column's bit pattern unsigned, a time as appendTime.
func appendNumber(b []byte, t schema.Type, v int64) []byte {
	switch t {
	case schema.Uint32:
		return strconv.AppendUint(b, uint64(uint32(v)), 10)
	case schema.Uint64:
		return strconv.AppendUint(b, uint64(v), 10)
	case schema.TimestampMicros:
		return appendTime(b, v)
	}
	return strconv.AppendInt(b, v, 10)
}

// table is the table's layout: the time and size, the kind and version
// columns when the schema has them, the key last, two blanks apart. The
// time and size have fixed widths; the kind and version ID columns are as
// wide as their heading or the widest value written so far.
type table[P any] struct {
	mtime, size func(*P) (int64, bool)
	kind        func(*P) (string, bool)
	vers        *schema.Versions[P]
	idName      string
	kindW, idW  int
	loc         *time.Location
}

// The table's headings beyond the schema's column names: the time says it
// is local; a versions schema's latest and delete-marker roles.
const (
	headingLocalTime    = mtimeColumn + " (local)"
	headingLatest       = "latest"
	headingDeleteMarker = "delete_marker"
	// sizeWidth is the widest appendIEC value ("0.98 MiB").
	sizeWidth = 8
)

func newTable[P any](s *schema.Schema[P], vers *schema.Versions[P], loc *time.Location) *table[P] {
	t := &table[P]{vers: vers, loc: loc, kindW: len(kindColumn)}
	if t.loc == nil {
		t.loc = time.Local
	}
	for _, c := range s.Columns {
		switch c.Name {
		case mtimeColumn:
			t.mtime = c.Int
		case sizeColumn:
			t.size = c.Int
		case kindColumn:
			t.kind = c.Str
		}
	}
	if vers != nil {
		t.idName = s.Versions.ID
		t.idW = len(t.idName)
	}
	return t
}

func (t *table[P]) header(b []byte) []byte {
	b = appendLeft(b, headingLocalTime, len(headingLocalTime))
	b = appendPadded(append(b, "  "...), []byte(sizeColumn), sizeWidth)
	if t.kind != nil {
		b = appendLeft(append(b, "  "...), kindColumn, t.kindW)
	}
	if t.vers != nil {
		b = appendLeft(append(b, "  "...), t.idName, t.idW)
		b = appendLeft(append(b, "  "...), headingLatest, len(headingLatest))
		if t.vers.HasDeleteMarkers() {
			b = appendLeft(append(b, "  "...), headingDeleteMarker, len(headingDeleteMarker))
		}
	}
	return append(b, "  "+KeyColumn+"\n"...)
}

func (t *table[P]) row(b []byte, r *schema.Row[P]) []byte {
	p := &r.Data
	start := len(b)
	if v, ok := get(t.mtime, p); ok {
		b = appendLocal(b, v, t.loc)
	}
	b = pad(b, start, len(headingLocalTime))
	var num [24]byte
	n := num[:0]
	if v, ok := get(t.size, p); ok {
		n = appendIEC(n, v)
	}
	b = appendPadded(append(b, "  "...), n, sizeWidth)
	if t.kind != nil {
		v, _ := t.kind(p)
		t.kindW = max(t.kindW, len(v))
		start = len(b) + 2
		b = pad(appendTSV(append(b, "  "...), v), start, t.kindW)
	}
	if t.vers != nil {
		id, _ := t.vers.ID(p)
		start = len(b) + 2
		b = appendTSV(append(b, "  "...), id)
		t.idW = max(t.idW, len(b)-start)
		b = pad(b, start, t.idW)
		start = len(b) + 2
		b = pad(strconv.AppendBool(append(b, "  "...), t.vers.Latest(p)), start, len(headingLatest))
		if t.vers.HasDeleteMarkers() {
			start = len(b) + 2
			b = pad(strconv.AppendBool(append(b, "  "...), t.vers.Marker(p)), start, len(headingDeleteMarker))
		}
	}
	return appendTSV(append(b, "  "...), r.Key)
}

// get is an integer accessor's value; false when the schema lacks the
// column or the value is null.
func get[P any](f func(*P) (int64, bool), p *P) (int64, bool) {
	if f == nil {
		return 0, false
	}
	return f(p)
}

// pad pads b with blanks until what follows start is width bytes.
func pad(b []byte, start, width int) []byte {
	for range width - (len(b) - start) {
		b = append(b, ' ')
	}
	return b
}

// appendLeft appends s left-aligned in width columns.
func appendLeft(b []byte, s string, width int) []byte {
	return pad(append(b, s...), len(b), width)
}
