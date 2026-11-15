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
// the same for every source. The byte figures keep the dataset's names
// (size, allocated_bytes), each with the objects that had no value for it;
// the newest time under a prefix is max_last_modified, not a row's
// last_modified. prefix_base64 replaces prefix for a prefix that is not
// UTF-8 (a local name's bytes), which a JSON string cannot carry exactly; a
// local tree's rows add dirs; JSONL totals read from a dataset add dataset
// (TotalsLayout.Dataset).
const (
	FieldRowType               = "row_type"
	FieldDepth                 = "depth"
	FieldPrefix                = "prefix"
	FieldPrefixBase64          = FieldPrefix + Base64Suffix
	FieldObjects               = "objects"
	FieldDirs                  = "dirs"
	FieldSize                  = totals.Size
	FieldSizeMissing           = totals.Size + missingSuffix
	FieldAllocatedBytes        = totals.AllocatedBytes
	FieldAllocatedBytesMissing = totals.AllocatedBytes + missingSuffix
	FieldMaxLastModified       = "max_last_modified"
	FieldPrimary               = "primary"
	FieldSort                  = "sort"
	FieldLinksOnce             = "links_once"
	FieldDataset               = "dataset"

	missingSuffix = "_missing"
)

// The row_type values: the total (depth 0, whose prefix is the key prefix
// the source names, logs/2026/0 for s3://b/logs/2026/0, empty for a whole
// bucket or tree) and every other prefix.
const (
	RowTotal  = "total"
	RowPrefix = "prefix"
)

// The table's headings are the fields' names, but for what people read
// differently: the allocated bytes on disk, a local tree's objects as
// files under paths.
const (
	HeadingOnDisk = "on disk" // allocated_bytes
	headingFiles  = "files"
	headingPath   = "path"
	headingTotal  = "(total)" // the total row's prefix in the table
	// partialMark marks a figure in the table that leaves out objects
	// without a value: a lower bound.
	partialMark = ">= "
)

// TotalsLayout is what the totals' rows hold besides the prefix, the
// objects, the figures and the time.
type TotalsLayout struct {
	// Primary is the figure --sort size orders by and the table puts
	// first: totals.Size ("") or totals.AllocatedBytes. Sort is the order,
	// "name" or "size", and LinksOnce whether hard-linked files counted
	// once (totals.Tally.LinksOnce); every row carries the three.
	Primary   string
	Sort      string
	LinksOnce bool
	// Local: the source is a local tree, whose rows carry their Dirs and
	// whose table calls the objects files.
	Local bool
	// NoHeader leaves out TSV's header line.
	NoHeader bool
	// Loc is the table's time zone (nil: time.Local), as the list
	// table's; TSV and JSONL are UTC.
	Loc *time.Location
	// Dataset, if set, is a JSON object that every JSONL row carries as
	// its last member, FieldDataset: where totals read from a saved
	// dataset came from (swath du --from-dataset). TSV and the table have
	// a fixed shape, and the command states it on stderr instead.
	Dataset []byte
	// Total names what the table's total row totals, SOURCE as typed,
	// before headingTotal ("": headingTotal alone); printed as given, the
	// caller escapes it (a Windows path keeps its backslashes).
	Total string
}

func (l *TotalsLayout) primary() string {
	if l.Primary == "" {
		return totals.Size
	}
	return l.Primary
}

// WriteTotals writes per-prefix totals (swath du): TSV under a header line
// (unless NoHeader) and JSONL in the order given (byte order, the total
// first), each row's type and depth first (0: the total), TSV's prefix
// last as du prints its path; or the table, the total last, as du's and
// wc's. A row includes its descendants, so only rows of one depth may be
// summed. TSV and JSONL carry both byte figures, exact, each with the
// objects that had no value for it: a figure is the sum of the values
// there were, and null (\N in TSV) when no object under a non-empty prefix
// had one; the time is null when no object had one. Every row repeats the
// primary figure, the order and whether hard links counted once. TSV takes
// the rows' grammar (appendTSV), so any name round-trips; in JSONL a
// prefix that is not UTF-8 is prefix_base64, its exact bytes, instead of
// prefix. The table is for reading: one figure, the primary, in binary
// units (TableFigure), the counts grouped (the figure and the counts "-"
// for a directory that could not be read), the newest time as the list
// table's ("-" when unknown), and the prefix (a local tree's path) last,
// indented by its depth, the total's named by the source (Total); the
// command states the other figure's total beside it.
func WriteTotals(w io.Writer, format string, l TotalsLayout, rows []totals.Prefix) error {
	bw := bufio.NewWriterSize(w, bufferBytes)
	var b []byte
	var tw tableWidths
	switch format {
	case TSV:
		if !l.NoHeader {
			b = append(b, FieldRowType+"\t"+FieldDepth+"\t"+FieldObjects...)
			if l.Local {
				b = append(b, "\t"+FieldDirs...)
			}
			b = append(b, "\t"+FieldSize+"\t"+FieldSizeMissing+"\t"+FieldAllocatedBytes+"\t"+FieldAllocatedBytesMissing+
				"\t"+FieldMaxLastModified+"\t"+FieldPrimary+"\t"+FieldSort+"\t"+FieldLinksOnce+"\t"+FieldPrefix+"\n"...)
		}
	case JSONL:
	case Table:
		tw = widths(&l, rows)
		b = tw.header(b, &l)
	default:
		return fmt.Errorf("text output: per-prefix totals have no %q format", format)
	}
	for r := range totalsOrder(format, rows) {
		switch format {
		case TSV:
			b = append(append(b, rowType(r)...), '\t')
			b = strconv.AppendInt(b, int64(r.Depth), 10)
			b = strconv.AppendInt(append(b, '\t'), r.Objects, 10)
			if l.Local {
				b = strconv.AppendInt(append(b, '\t'), r.Dirs, 10)
			}
			b = appendTSVFigure(append(b, '\t'), r.Size, r.HasSize)
			b = strconv.AppendInt(append(b, '\t'), r.SizeMissing, 10)
			b = appendTSVFigure(append(b, '\t'), r.Allocated, r.HasAllocated)
			b = strconv.AppendInt(append(b, '\t'), r.AllocatedMissing, 10)
			b = append(b, '\t')
			if r.HasNewest {
				b = appendTime(b, r.Newest)
			} else {
				b = append(b, nullTSV...)
			}
			b = append(append(append(append(append(b, '\t'), l.primary()...), '\t'), l.Sort...), '\t')
			b = strconv.AppendBool(b, l.LinksOnce)
			b = appendTSV(append(b, '\t'), r.Prefix)
		case JSONL:
			b = append(append(append(b, `{"`+FieldRowType+`":"`...), rowType(r)...), '"')
			b = strconv.AppendInt(append(b, `,"`+FieldDepth+`":`...), int64(r.Depth), 10)
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
			b = appendJSONFigure(append(b, `,"`+FieldSize+`":`...), r.Size, r.HasSize)
			b = strconv.AppendInt(append(b, `,"`+FieldSizeMissing+`":`...), r.SizeMissing, 10)
			b = appendJSONFigure(append(b, `,"`+FieldAllocatedBytes+`":`...), r.Allocated, r.HasAllocated)
			b = strconv.AppendInt(append(b, `,"`+FieldAllocatedBytesMissing+`":`...), r.AllocatedMissing, 10)
			if r.HasNewest {
				b = append(appendTime(append(b, `,"`+FieldMaxLastModified+`":"`...), r.Newest), '"')
			} else {
				b = append(b, `,"`+FieldMaxLastModified+`":null`...)
			}
			b = append(append(b, `,"`+FieldPrimary+`":"`...), l.primary()...)
			b = append(append(b, `","`+FieldSort+`":"`...), l.Sort...)
			b = strconv.AppendBool(append(b, `","`+FieldLinksOnce+`":`...), l.LinksOnce)
			if l.Dataset != nil {
				b = append(append(b, `,"`+FieldDataset+`":`...), l.Dataset...)
			}
			b = append(b, '}')
		case Table:
			b = tw.row(b, &l, r)
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

// rowType is r's row_type: the total (depth 0) or a prefix.
func rowType(r *totals.Prefix) string {
	if r.Depth == 0 {
		return RowTotal
	}
	return RowPrefix
}

// appendTSVFigure appends a byte figure, \N when unknown.
func appendTSVFigure(b []byte, n int64, ok bool) []byte {
	if !ok {
		return append(b, nullTSV...)
	}
	return strconv.AppendInt(b, n, 10)
}

// appendJSONFigure appends a byte figure, null when unknown.
func appendJSONFigure(b []byte, n int64, ok bool) []byte {
	if !ok {
		return append(b, "null"...)
	}
	return strconv.AppendInt(b, n, 10)
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

// heading is the table's figure heading: size, or on disk for the
// allocated bytes.
func (l *TotalsLayout) heading() string {
	if l.primary() == totals.AllocatedBytes {
		return HeadingOnDisk
	}
	return FieldSize
}

// tableWidths are the table's columns' widths, each its heading's or its
// widest value's, and the time column's zone and heading.
type tableWidths struct {
	figure, objects, dirs, time int
	loc                         *time.Location
	timeHeading                 string
}

func widths(l *TotalsLayout, rows []totals.Prefix) tableWidths {
	w := tableWidths{figure: len(l.heading()), objects: len(headingFiles), dirs: len(FieldDirs)}
	if !l.Local {
		w.objects = len(FieldObjects)
	}
	w.loc, w.timeHeading = tableClock(l.Loc)
	w.time = max(len(w.timeHeading), len(tableTime))
	var num [32]byte
	for i := range rows {
		r := &rows[i]
		w.figure = max(w.figure, len(appendTableFigure(num[:0], r, l.primary())))
		w.objects = max(w.objects, len(appendGrouped(num[:0], r.Objects)))
		w.dirs = max(w.dirs, len(appendGrouped(num[:0], r.Dirs)))
	}
	return w
}

func (w tableWidths) header(b []byte, l *TotalsLayout) []byte {
	objects, prefix := FieldObjects, FieldPrefix
	if l.Local {
		objects, prefix = headingFiles, headingPath
	}
	b = appendPadded(b, []byte(l.heading()), w.figure)
	b = appendPadded(append(b, "  "...), []byte(objects), w.objects)
	if l.Local {
		b = appendPadded(append(b, "  "...), []byte(FieldDirs), w.dirs)
	}
	b = appendLeft(append(b, "  "...), w.timeHeading, w.time)
	return append(append(b, "  "...), prefix+"\n"...)
}

func (w tableWidths) row(b []byte, l *TotalsLayout, r *totals.Prefix) []byte {
	var num [32]byte
	b = appendPadded(b, appendTableFigure(num[:0], r, l.primary()), w.figure)
	b = appendPadded(append(b, "  "...), appendCount(num[:0], r, r.Objects), w.objects)
	if l.Local {
		b = appendPadded(append(b, "  "...), appendCount(num[:0], r, r.Dirs), w.dirs)
	}
	b = append(b, "  "...)
	start := len(b)
	if r.HasNewest {
		b = appendLocal(b, r.Newest, w.loc)
	} else {
		b = append(b, missingCell...)
	}
	b = append(pad(b, start, w.time), "  "...)
	if r.Depth == 0 {
		if l.Total != "" {
			b = append(append(b, l.Total...), ' ')
		}
		return append(b, headingTotal...)
	}
	for range 2 * (r.Depth - 1) { // a level's rows under its parent's
		b = append(b, ' ')
	}
	return appendTSV(b, r.Prefix)
}

// appendGrouped appends a count for the table, its digits in groups of
// three (60,010), as the command's statistics write counts.
func appendGrouped(b []byte, n int64) []byte {
	var num [24]byte
	d := strconv.AppendInt(num[:0], n, 10)
	if n < 0 {
		b, d = append(b, '-'), d[1:]
	}
	for i, c := range d {
		if i > 0 && (len(d)-i)%3 == 0 {
			b = append(b, ',')
		}
		b = append(b, c)
	}
	return b
}

// appendCount appends a count of r's for the table: grouped, or "-" for
// a directory that could not be read, whose count is unknown.
func appendCount(b []byte, r *totals.Prefix, n int64) []byte {
	if r.Unreadable {
		return append(b, missingCell...)
	}
	return appendGrouped(b, n)
}

// appendPadded appends s right-aligned in width columns.
func appendPadded(b, s []byte, width int) []byte {
	for range width - len(s) {
		b = append(b, ' ')
	}
	return append(b, s...)
}

// TableFigure is a prefix's figure (totals.Size or AllocatedBytes) as the
// table shows it: appendIEC, "-" when it is unknown, and after ">= " when
// objects without a value leave it a lower bound.
func TableFigure(r *totals.Prefix, figure string) string {
	return string(appendTableFigure(nil, r, figure))
}

func appendTableFigure(b []byte, r *totals.Prefix, figure string) []byte {
	n, ok := r.Figure(figure)
	ok = ok && !r.Unreadable
	missing := r.SizeMissing
	if figure == totals.AllocatedBytes {
		missing = r.AllocatedMissing
	}
	switch {
	case !ok:
		return append(b, missingCell...)
	case missing > 0:
		b = append(b, partialMark...)
	}
	return appendIEC(b, n)
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
