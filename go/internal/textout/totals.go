// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/totals"
)

// The per-prefix totals' fields (swath du): TSV columns and JSONL members,
// the same for every source of a kind. PrefixBase64 replaces Prefix for a
// prefix that is not UTF-8 (a local name's bytes), which a JSON string
// cannot carry exactly; AllocatedSize replaces Size with --allocated; a
// local tree's rows add Dirs and Unsized.
const (
	FieldPrefix        = "Prefix"
	FieldPrefixBase64  = "PrefixBase64"
	FieldObjects       = "Objects"
	FieldDirs          = "Dirs"
	FieldSize          = "Size"
	FieldAllocatedSize = "AllocatedSize"
	FieldUnsized       = "Unsized"
	FieldLastModified  = "LastModified"
)

// The table's headings that differ from the fields: a local tree's objects
// are files, and the table's times are the reader's local time.
const (
	headingFiles     = "Files"
	headingAllocated = "Allocated"
	headingModified  = "LastModified (local)"
	headingTotal     = "(total)" // the total row's prefix in the table
)

// TotalsLayout is what the totals' rows hold besides the prefix, objects,
// bytes and time, and how the table shows times.
type TotalsLayout struct {
	// Allocated: the bytes are allocated bytes (AllocatedSize), not the
	// apparent size.
	Allocated bool
	// Local: the source is a local tree, whose rows carry their Dirs and
	// their Unsized (files the metadata tier read no size for: the names
	// tier), and whose table calls the objects files.
	Local bool
	// Loc is the table's time zone (nil: time.Local); TSV and JSONL are
	// UTC.
	Loc *time.Location
}

// WriteTotals writes per-prefix totals (swath du), in the order given, the
// total first: TSV under a header line, JSONL, or the table. TSV and JSONL
// carry exact bytes and microsecond UTC times; Size is the bytes of the
// objects that had a size, and a local tree's Unsized counts those that
// had none (as rclone size's sizeless); a time is left out (empty in TSV,
// absent in JSONL) when no object under the prefix had one. Names take the
// listing's escaping (appendEscaped), so TSV round-trips any bytes and
// never sends a terminal a control byte; in JSONL a prefix that is not
// UTF-8 is PrefixBase64, its exact bytes, instead of Prefix. The table is
// for reading: sizes in binary units ("-" when one is unknown), local
// times to the second, the prefix last as du prints it.
func WriteTotals(w io.Writer, format string, l TotalsLayout, rows []totals.Prefix) error {
	bw := bufio.NewWriterSize(w, bufferBytes)
	var b []byte
	var tw tableWidths
	switch format {
	case TSV:
		b = append(b, FieldPrefix+"\t"+FieldObjects...)
		if l.Local {
			b = append(b, "\t"+FieldDirs...)
		}
		b = append(append(b, '\t'), l.sizeField()...)
		if l.Local {
			b = append(b, "\t"+FieldUnsized...)
		}
		b = append(b, "\t"+FieldLastModified+"\n"...)
	case JSONL:
	case Table:
		tw = widths(l, rows)
		b = tw.header(b, l)
	default:
		return fmt.Errorf("text output: per-prefix totals have no %q format", format)
	}
	for i := range rows {
		r := &rows[i]
		switch format {
		case TSV:
			b = appendEscaped(b, r.Prefix, false)
			b = strconv.AppendInt(append(b, '\t'), r.Objects, 10)
			if l.Local {
				b = strconv.AppendInt(append(b, '\t'), r.Dirs, 10)
			}
			b = strconv.AppendInt(append(b, '\t'), r.Bytes, 10)
			if l.Local {
				b = strconv.AppendInt(append(b, '\t'), r.Unsized, 10)
			}
			b = append(b, '\t')
			if r.HasNewest {
				b = appendISO(b, r.Newest)
			}
		case JSONL:
			if utf8.ValidString(r.Prefix) {
				b = appendJSON(append(b, `{"`+FieldPrefix+`":`...), r.Prefix)
			} else {
				b = append(b, `{"`+FieldPrefixBase64+`":"`...)
				b = append(base64.StdEncoding.AppendEncode(b, []byte(r.Prefix)), '"')
			}
			b = strconv.AppendInt(append(b, `,"`+FieldObjects+`":`...), r.Objects, 10)
			if l.Local {
				b = strconv.AppendInt(append(b, `,"`+FieldDirs+`":`...), r.Dirs, 10)
			}
			b = strconv.AppendInt(append(append(append(b, `,"`...), l.sizeField()...), `":`...), r.Bytes, 10)
			if l.Local {
				b = strconv.AppendInt(append(b, `,"`+FieldUnsized+`":`...), r.Unsized, 10)
			}
			if r.HasNewest {
				b = append(appendISO(append(b, `,"`+FieldLastModified+`":"`...), r.Newest), '"')
			}
			b = append(b, '}')
		case Table:
			b = tw.row(b, l, r, i == 0)
		}
		b = append(b, '\n')
		if _, err := bw.Write(b); err != nil {
			return err
		}
		b = b[:0]
	}
	if _, err := bw.Write(b); err != nil { // the header of an empty list
		return err
	}
	return bw.Flush()
}

// sizeField is the bytes' field name.
func (l TotalsLayout) sizeField() string {
	if l.Allocated {
		return FieldAllocatedSize
	}
	return FieldSize
}

// tableWidths are the table's right-aligned columns' widths: each its
// heading's or its widest value's.
type tableWidths struct{ size, objects, dirs int }

// tableTime is the table's time layout; timeWidth its column, as wide as
// the heading.
const (
	tableTime = "2006-01-02 15:04:05"
	timeWidth = max(len(tableTime), len(headingModified))
)

func widths(l TotalsLayout, rows []totals.Prefix) tableWidths {
	w := tableWidths{size: len(FieldSize), objects: len(headingFiles), dirs: len(FieldDirs)}
	if l.Allocated {
		w.size = len(headingAllocated)
	}
	if !l.Local {
		w.objects = len(FieldObjects)
	}
	var num [24]byte
	for i := range rows {
		r := &rows[i]
		w.size = max(w.size, len(appendIEC(num[:0], r)))
		w.objects = max(w.objects, len(strconv.AppendInt(num[:0], r.Objects, 10)))
		w.dirs = max(w.dirs, len(strconv.AppendInt(num[:0], r.Dirs, 10)))
	}
	return w
}

func (w tableWidths) header(b []byte, l TotalsLayout) []byte {
	size, objects := FieldSize, FieldObjects
	if l.Allocated {
		size = headingAllocated
	}
	if l.Local {
		objects = headingFiles
	}
	b = appendPadded(b, []byte(size), w.size)
	b = appendPadded(append(b, "  "...), []byte(objects), w.objects)
	if l.Local {
		b = appendPadded(append(b, "  "...), []byte(FieldDirs), w.dirs)
	}
	b = append(b, "  "+headingModified...)
	for range timeWidth - len(headingModified) {
		b = append(b, ' ')
	}
	return append(b, "  "+FieldPrefix+"\n"...)
}

func (w tableWidths) row(b []byte, l TotalsLayout, r *totals.Prefix, total bool) []byte {
	var num [24]byte
	b = appendPadded(b, appendIEC(num[:0], r), w.size)
	b = appendPadded(append(b, "  "...), strconv.AppendInt(num[:0], r.Objects, 10), w.objects)
	if l.Local {
		b = appendPadded(append(b, "  "...), strconv.AppendInt(num[:0], r.Dirs, 10), w.dirs)
	}
	b = append(b, "  "...)
	if r.HasNewest {
		loc := l.Loc
		if loc == nil {
			loc = time.Local
		}
		b = time.UnixMicro(r.Newest).In(loc).AppendFormat(b, tableTime)
		for range timeWidth - len(tableTime) {
			b = append(b, ' ')
		}
	} else {
		for range timeWidth {
			b = append(b, ' ')
		}
	}
	b = append(b, "  "...)
	if total {
		return append(b, headingTotal...)
	}
	return appendEscaped(b, r.Prefix, false)
}

// appendPadded appends s right-aligned in width columns.
func appendPadded(b, s []byte, width int) []byte {
	for range width - len(s) {
		b = append(b, ' ')
	}
	return append(b, s...)
}

// iecUnits are the binary units a table's sizes are given in.
var iecUnits = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// appendIEC appends a prefix's size for reading: bytes below 1 KiB, else
// at most three significant digits in binary units (5.88 MiB, 695 KiB,
// 0.98 MiB for 1000 KiB), as du -h and rclone size do; "-" when an object
// under it had no size.
func appendIEC(b []byte, r *totals.Prefix) []byte {
	if !r.Sized() {
		return append(b, '-')
	}
	if r.Bytes < 1024 {
		return append(strconv.AppendInt(b, r.Bytes, 10), " B"...)
	}
	v, u := float64(r.Bytes), 0
	for v >= 1024 && u < len(iecUnits)-1 {
		v /= 1024
		u++
	}
	if v >= 999.5 && u < len(iecUnits)-1 { // four digits: the next unit's 0.98 to 1.00
		v /= 1024
		u++
	}
	prec := 0
	switch {
	case v < 9.995:
		prec = 2
	case v < 99.95:
		prec = 1
	}
	return append(append(strconv.AppendFloat(b, v, 'f', prec, 64), ' '), iecUnits[u]...)
}
