// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"io"
	"iter"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/totals"
)

// The per-prefix totals' fields (swath du): TSV columns and JSONL members,
// the same for every source. prefix_base64 replaces prefix for a prefix
// that is not UTF-8 (a local name's bytes), which a JSON string cannot
// carry exactly; allocated_size replaces size with --allocated; a local
// tree's rows add dirs and unsized; JSONL totals read from a dataset add
// dataset (TotalsLayout.Dataset).
const (
	FieldDepth         = "depth"
	FieldPrefix        = "prefix"
	FieldPrefixBase64  = FieldPrefix + Base64Suffix
	FieldObjects       = "objects"
	FieldDirs          = "dirs"
	FieldSize          = "size"
	FieldAllocatedSize = "allocated_size"
	FieldUnsized       = "unsized"
	FieldLastModified  = "last_modified"
	FieldDataset       = "dataset"
)

// The table's headings are the fields' names, as the rows table's are the
// schema's, but for what people read differently: a local tree's objects
// are files under paths, and the times are the reader's local time
// (headingLocalTime).
const (
	headingFiles = "files"
	headingPath  = "path"
	headingTotal = "(total)" // the total row's prefix in the table
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
	// Dataset, if set, is a JSON object that every JSONL row carries as
	// its last member, FieldDataset: where totals read from a saved
	// dataset came from (swath du --from-dataset). TSV and the table have
	// a fixed shape, and the command states it on stderr instead.
	Dataset []byte
}

// WriteTotals writes per-prefix totals (swath du): TSV under a header line
// and JSONL in the order given (byte order, the total first), each row's
// depth first (0: the total), TSV's prefix last as du prints its path; or
// the table, the total last, as du's and wc's. A row includes its
// descendants, so only rows of one depth may be summed. TSV and JSONL
// carry exact bytes and the rows' times (appendTime); size is the bytes of
// the objects that had a size, and a local tree's unsized counts those
// that had none (as rclone size's sizeless); the time is null (\N in TSV)
// when no object under the prefix had one. TSV takes the rows' grammar
// (appendTSV), so any name round-trips; in JSONL a prefix that is not
// UTF-8 is prefix_base64, its exact bytes, instead of prefix. The table is
// for reading: sizes in binary units ("-" when one is unknown), local
// times to the second, the prefix (a local tree's path) last as du prints
// it.
func WriteTotals(w io.Writer, format string, l TotalsLayout, rows []totals.Prefix) error {
	bw := bufio.NewWriterSize(w, bufferBytes)
	var b []byte
	var tw tableWidths
	switch format {
	case TSV:
		b = append(b, FieldDepth+"\t"+FieldObjects...)
		if l.Local {
			b = append(b, "\t"+FieldDirs...)
		}
		b = append(append(b, '\t'), l.sizeField()...)
		if l.Local {
			b = append(b, "\t"+FieldUnsized...)
		}
		b = append(b, "\t"+FieldLastModified+"\t"+FieldPrefix+"\n"...)
	case JSONL:
	case Table:
		tw = widths(l, rows)
		b = tw.header(b, l)
	default:
		return fmt.Errorf("text output: per-prefix totals have no %q format", format)
	}
	for r := range totalsOrder(format, rows) {
		switch format {
		case TSV:
			b = strconv.AppendInt(b, int64(r.Depth), 10)
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
				b = appendTime(b, r.Newest)
			} else {
				b = append(b, nullTSV...)
			}
			b = appendTSV(append(b, '\t'), r.Prefix)
		case JSONL:
			b = strconv.AppendInt(append(b, `{"`+FieldDepth+`":`...), int64(r.Depth), 10)
			if utf8.ValidString(r.Prefix) {
				b = appendJSONString(append(b, `,"`+FieldPrefix+`":`...), r.Prefix)
			} else {
				b = append(b, `,"`+FieldPrefixBase64+`":"`...)
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
				b = append(appendTime(append(b, `,"`+FieldLastModified+`":"`...), r.Newest), '"')
			} else {
				b = append(b, `,"`+FieldLastModified+`":null`...)
			}
			if l.Dataset != nil {
				b = append(append(b, `,"`+FieldDataset+`":`...), l.Dataset...)
			}
			b = append(b, '}')
		case Table:
			b = tw.row(b, l, r)
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

// totalsOrder yields rows in format's order: as given, but for the table,
// whose total (Depth 0) comes after the other rows, as du's and wc's.
func totalsOrder(format string, rows []totals.Prefix) iter.Seq[*totals.Prefix] {
	return func(yield func(*totals.Prefix) bool) {
		emit := func(keep func(*totals.Prefix) bool) bool {
			for i := range rows {
				if r := &rows[i]; keep(r) && !yield(r) {
					return false
				}
			}
			return true
		}
		if format != Table {
			emit(func(*totals.Prefix) bool { return true })
			return
		}
		if emit(func(r *totals.Prefix) bool { return r.Depth > 0 }) {
			emit(func(r *totals.Prefix) bool { return r.Depth == 0 })
		}
	}
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

// timeWidth is the table's time column, as wide as the heading.
const timeWidth = max(len(tableTime), len(headingLocalTime))

func widths(l TotalsLayout, rows []totals.Prefix) tableWidths {
	w := tableWidths{size: len(l.sizeField()), objects: len(headingFiles), dirs: len(FieldDirs)}
	if !l.Local {
		w.objects = len(FieldObjects)
	}
	var num [24]byte
	for i := range rows {
		r := &rows[i]
		w.size = max(w.size, len(appendPrefixSize(num[:0], r)))
		w.objects = max(w.objects, len(strconv.AppendInt(num[:0], r.Objects, 10)))
		w.dirs = max(w.dirs, len(strconv.AppendInt(num[:0], r.Dirs, 10)))
	}
	return w
}

func (w tableWidths) header(b []byte, l TotalsLayout) []byte {
	size, objects, prefix := l.sizeField(), FieldObjects, FieldPrefix
	if l.Local {
		objects, prefix = headingFiles, headingPath
	}
	b = appendPadded(b, []byte(size), w.size)
	b = appendPadded(append(b, "  "...), []byte(objects), w.objects)
	if l.Local {
		b = appendPadded(append(b, "  "...), []byte(FieldDirs), w.dirs)
	}
	b = append(b, "  "+headingLocalTime...)
	for range timeWidth - len(headingLocalTime) {
		b = append(b, ' ')
	}
	return append(append(b, "  "...), prefix+"\n"...)
}

func (w tableWidths) row(b []byte, l TotalsLayout, r *totals.Prefix) []byte {
	var num [24]byte
	b = appendPadded(b, appendPrefixSize(num[:0], r), w.size)
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
		b = appendLocal(b, r.Newest, loc)
		for range timeWidth - len(tableTime) {
			b = append(b, ' ')
		}
	} else {
		for range timeWidth {
			b = append(b, ' ')
		}
	}
	b = append(b, "  "...)
	if r.Depth == 0 {
		return append(b, headingTotal...)
	}
	return appendTSV(b, r.Prefix)
}

// appendPadded appends s right-aligned in width columns.
func appendPadded(b, s []byte, width int) []byte {
	for range width - len(s) {
		b = append(b, ' ')
	}
	return append(b, s...)
}

// appendPrefixSize appends a prefix's size for the table: appendIEC, or
// "-" when an object under it had no size.
func appendPrefixSize(b []byte, r *totals.Prefix) []byte {
	if !r.Sized() {
		return append(b, '-')
	}
	return appendIEC(b, r.Bytes)
}

// iecUnits are the binary units a table's sizes are given in.
var iecUnits = [...]string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}

// Size is n bytes for people to read, appendIEC's form: the tables', and
// every size the command prints on the terminal.
func Size(n int64) string { return string(appendIEC(nil, n)) }

// appendIEC appends a size for reading, the tables' one form: bytes below
// 1 KiB, else at most three significant digits in binary units (5.88 MiB,
// 695 KiB, 0.98 MiB for 1000 KiB), as du -h and rclone size do.
func appendIEC(b []byte, n int64) []byte {
	if n < 1024 {
		return append(strconv.AppendInt(b, n, 10), " B"...)
	}
	v, u := float64(n), 0
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
