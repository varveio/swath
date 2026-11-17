// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package textout

import (
	"bufio"
	"io"
	"strconv"
	"time"
	"unicode"

	"golang.org/x/text/width"

	"github.com/varveio/swath/internal/totals"
)

// ViewLayout is how the adaptive view (swath du on a terminal) is drawn.
type ViewLayout struct {
	// Label names the total's line: SOURCE as given, written as it is.
	Label string
	// Local: the source is a local tree, whose objects are files.
	Local bool
	// Width is the terminal's columns (0: unknown, 80 assumed).
	Width int
	// Color styles the heading, the bar and the fold lines (ANSI).
	Color bool
	// Loc is the dates' zone (nil: time.Local), named once, on the total's
	// line.
	Loc *time.Location
}

// The view's columns: the bar's cells (wideBar from wideView columns,
// else from narrowView up to narrowBar while the paths keep pathRoom), and
// the gutter.
const (
	wideView   = 100
	narrowView = 72
	wideBar    = 20
	narrowBar  = 10
	minBar     = 4
	pathRoom   = 16
	viewGutter = "  "
	viewDate   = "2006-01-02"
)

var eighths = [...]string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}

// WriteView writes the adaptive view's lines (totals.View.Select) under a
// heading: per line its size, its count of objects (files on a local
// tree), its share of the total, a bar of that share, the date of the
// newest modification below it (max_last_modified) and its path, indented
// two blanks a level under its parent's, the total first under the label.
// A fold line reads "(N more)": the rest of its parent's children, their
// sums and their newest date. A "!" after the size marks a line whose sums
// leave out a directory that could not be read, at or below it. As the width shrinks the bar narrows and goes,
// then the share, then the date; the size, the count and the path stay,
// the path cut in the middle where the line would pass the width, so no
// line wraps. The sizes are the table's (TableFigure: binary units, ">= "
// for a lower bound), the counts three significant digits (12.0k), the
// dates local, the zone named on the total's line. The names are written
// as given: the caller escapes them for the terminal.
func WriteView(w io.Writer, l ViewLayout, rows []totals.ViewRow) error {
	if len(rows) == 0 {
		return nil
	}
	total := rows[0].Bytes
	loc, zone := tableZone(l.Loc)
	countHead := FieldObjects
	if l.Local {
		countHead = headingFiles
	}
	const sizeHead, shareHead, dateHead, pathHead = "size", "share", headingNewest, "path"
	names := make([]string, len(rows))
	sizes := make([]string, len(rows))
	counts := make([]string, len(rows))
	shares := make([]string, len(rows))
	dates := make([]string, len(rows))
	sizeW, countW, shareW, dateW, nameW := len(sizeHead), len(countHead), len(shareHead), len(dateHead), 0
	timed, marked := false, false
	for i := range rows {
		r := &rows[i]
		switch {
		case i == 0:
			names[i] = l.Label
		case r.Fold && r.More < 0:
			names[i] = "(more)"
		case r.Fold:
			names[i] = "(" + string(appendGrouped(nil, r.More)) + " more)"
		default:
			names[i] = r.Name
		}
		b := []byte(nil)
		if r.Missing > 0 {
			b = append(b, partialMark...)
		}
		sizes[i] = string(appendIEC(b, r.Bytes))
		counts[i] = string(appendAbbreviated(nil, r.Objects))
		shares[i] = share(r.Bytes, total)
		switch {
		case r.HasNewest:
			dates[i] = time.UnixMicro(r.Newest).In(loc).Format(viewDate)
			if i == 0 {
				dates[i] += " " + zone
			}
			timed = true
		case !r.Fold:
			dates[i] = missingCell
		}
		marked = marked || r.Unreadable
		sizeW = max(sizeW, len(sizes[i]))
		countW = max(countW, len(counts[i]))
		shareW = max(shareW, len(shares[i]))
		dateW = max(dateW, len(dates[i]))
		if i > 0 { // the total's label may run past the room: it is alone on its line
			nameW = max(nameW, 2*r.Level+cells(names[i]))
		}
	}
	width := l.Width
	if width <= 0 {
		width = 80
	}
	// The columns before the path, its gutter included; the bar's apart.
	shown := struct{ share, date bool }{true, timed}
	before := func() int {
		n := sizeW + len(viewGutter) + countW + len(viewGutter)
		if marked {
			n += 2
		}
		if shown.share {
			n += shareW + len(viewGutter)
		}
		if shown.date {
			n += dateW + len(viewGutter)
		}
		return n
	}
	need := min(nameW, pathRoom) // the room the paths keep
	bar := 0
	switch room := width - before(); {
	case width >= wideView && room-wideBar-len(viewGutter) >= need:
		bar = wideBar
	case width >= narrowView && min(narrowBar, room-len(viewGutter)-need) >= minBar:
		bar = min(narrowBar, room-len(viewGutter)-need)
	case room < need:
		shown.share = false
		if width-before() < need {
			shown.date = false
		}
	}
	cols := before()
	if bar > 0 {
		cols += bar + len(viewGutter)
	}
	bw := bufio.NewWriterSize(w, bufferBytes)
	var b []byte
	// line writes a line; dim: the heading or a fold, dimmed in color.
	line := func(size string, mark byte, count, shr string, bar func([]byte) []byte, date, name string, dim bool) error {
		b = b[:0]
		if dim && l.Color {
			b = append(b, "\x1b[2m"...)
		}
		b = appendPadded(b, []byte(size), sizeW)
		if marked {
			b = append(b, ' ', mark)
		}
		b = appendPadded(append(b, viewGutter...), []byte(count), countW)
		if shown.share {
			b = appendPadded(append(b, viewGutter...), []byte(shr), shareW)
		}
		b = append(b, viewGutter...)
		if bar != nil {
			b = append(bar(b), viewGutter...)
			if dim && l.Color {
				b = append(b, "\x1b[2m"...) // the bar's color ends with a reset
			}
		}
		if shown.date {
			b = append(b, date...)
			for range dateW - len(date) {
				b = append(b, ' ')
			}
			b = append(b, viewGutter...)
		}
		b = append(b, name...)
		if dim && l.Color {
			b = append(b, "\x1b[0m"...)
		}
		_, err := bw.Write(append(b, '\n'))
		return err
	}
	blank := func(b []byte) []byte {
		for range bar {
			b = append(b, ' ')
		}
		return b
	}
	var head func([]byte) []byte
	if bar > 0 {
		head = blank
	}
	if err := line(sizeHead, ' ', countHead, shareHead, head, dateHead, pathHead, true); err != nil {
		return err
	}
	for i := range rows {
		r := &rows[i]
		var drawn func([]byte) []byte
		if bar > 0 {
			drawn = func(b []byte) []byte { return appendBar(b, r.Bytes, total, bar, l.Color) }
		}
		indent := ""
		for range 2 * r.Level {
			indent += " "
		}
		name := indent + clip(names[i], width-cols-2*r.Level)
		mark := byte(' ')
		if r.Unreadable {
			mark = '!'
		}
		if err := line(sizes[i], mark, counts[i], shares[i], drawn, dates[i], name, r.Fold); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// appendAbbreviated appends a count for the view: up to 999 as it is, else three
// significant digits in thousands (k), millions (M), billions (G) or
// more (T, P, E): 12.0k, 37.1M.
func appendAbbreviated(b []byte, n int64) []byte {
	if n < 1000 {
		return strconv.AppendInt(b, n, 10)
	}
	const units = "kMGTPE"
	v, u := float64(n)/1000, 0
	for v >= 999.5 && u < len(units)-1 {
		v /= 1000
		u++
	}
	prec := 0
	switch {
	case v < 9.995:
		prec = 2
	case v < 99.95:
		prec = 1
	}
	return append(strconv.AppendFloat(b, v, 'f', prec, 64), units[u])
}

// clip is name cut to at most room terminal columns (at least the
// ellipsis), the middle replaced by an ellipsis so that its end, an
// extension, stays.
func clip(name string, room int) string {
	if cells(name) <= room {
		return name
	}
	r := []rune(name)
	keep := max(room, 1) - 1 // the ellipsis takes one
	used, tail := 0, len(r)  // the end first, up to half the room
	for tail > 0 && used+cell(r[tail-1]) <= (keep+1)/2 {
		tail--
		used += cell(r[tail])
	}
	head := 0 // then the start, in what is left
	for head < tail && used+cell(r[head]) <= keep {
		used += cell(r[head])
		head++
	}
	return string(r[:head]) + "…" + string(r[tail:])
}

// cells is s's width on a terminal, in columns.
func cells(s string) int {
	n := 0
	for _, r := range s {
		n += cell(r)
	}
	return n
}

// cell is r's width on a terminal: two columns for an East Asian wide or
// fullwidth character, none for a combining mark or a control, else one.
func cell(r rune) int {
	switch {
	case unicode.In(r, unicode.Mn, unicode.Me, unicode.Cf) || unicode.IsControl(r):
		return 0
	}
	switch width.LookupRune(r).Kind() {
	case width.EastAsianWide, width.EastAsianFullwidth:
		return 2
	}
	return 1
}

// share is n's share of total as a percent with one decimal ("100%" for
// the whole and only for it, "-" of nothing).
func share(n, total int64) string {
	switch {
	case total <= 0:
		return "-"
	case n >= total:
		return "100%"
	}
	p := strconv.FormatFloat(float64(n)*100/float64(total), 'f', 1, 64)
	if p == "100.0" { // a part is not the whole
		p = "99.9"
	}
	return p + "%"
}

// appendBar appends n's share of total as a bar of width cells, in eighths,
// padded with spaces.
func appendBar(b []byte, n, total int64, width int, color bool) []byte {
	cells := 0
	if total > 0 {
		cells = int(float64(n) / float64(total) * float64(width*8))
	}
	cells = min(max(cells, 0), width*8)
	if color && cells > 0 {
		b = append(b, "\x1b[36m"...)
	}
	for range cells / 8 {
		b = append(b, "█"...)
	}
	b = append(b, eighths[cells%8]...)
	if color && cells > 0 {
		b = append(b, "\x1b[0m"...)
	}
	used := cells / 8
	if cells%8 > 0 {
		used++
	}
	for range width - used {
		b = append(b, ' ')
	}
	return b
}
