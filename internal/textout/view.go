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
	// Width is the terminal's columns (0: unknown, 80 assumed). The
	// newest time comes when the lines leave room for it, the bar when
	// they leave at least minBar columns more: as the terminal narrows the
	// bar goes first, then the time.
	Width int
	// Color styles the bar and the fold lines (ANSI).
	Color bool
	// Loc is the times' zone (nil: time.Local), named once, on the total's
	// line.
	Loc *time.Location
}

// The bar's widths, and the partial blocks of its last cell in eighths.
const (
	minBar     = 10
	maxBar     = 30
	minName    = 16 // the names' room that the bar and the time leave, at least
	viewGutter = "  "
)

var eighths = [...]string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}

// WriteView writes the adaptive view's lines (totals.View.Select): per
// line its size, its share of the total, a bar of that share and the
// newest modification time below it (max_last_modified) when the width
// allows, and its name indented under its parent's, the total first under
// the label, cut in the middle where the line would pass the width. The
// names are written as given: the caller escapes them for the terminal. A fold line reads "(N more)": the rest of its parent's
// children and their sums, without a time. The sizes are the table's
// (TableFigure: binary units, ">= " for a lower bound), the times too (to
// the second, local, "-" for none), the zone named after the total's.
func WriteView(w io.Writer, l ViewLayout, rows []totals.ViewRow) error {
	if len(rows) == 0 {
		return nil
	}
	total := rows[0].Bytes
	names := make([]string, len(rows))
	sizes := make([]string, len(rows))
	shares := make([]string, len(rows))
	sizeW, shareW, nameW := 0, 0, 0
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
		shares[i] = share(r.Bytes, total)
		sizeW = max(sizeW, len(sizes[i]))
		shareW = max(shareW, len(shares[i]))
		if i > 0 { // the total's label may run past the bar's room: it is alone on its line
			nameW = max(nameW, 2*r.Level+cells(names[i]))
		}
	}
	width := l.Width
	if width <= 0 {
		width = 80
	}
	loc, zone := tableZone(l.Loc)
	timed := false
	for i := range rows {
		timed = timed || rows[i].HasNewest
	}
	// The room past the size, the share and the names: the time first,
	// then the bar, each with its gutter. A long name does not take it all:
	// past a third of the width it is cut (clip).
	room := width - sizeW - shareW - min(nameW, max(minName, width/3)) - 2*len(viewGutter)
	timeW := len(tableTime) + 1 + len(zone)
	if !timed || room < timeW+len(viewGutter) {
		timeW = 0
	} else {
		room -= timeW + len(viewGutter)
	}
	bar := min(maxBar, room-len(viewGutter))
	if bar < minBar || timed && timeW == 0 {
		bar = 0
	}
	cols := sizeW + shareW + 2*len(viewGutter) // the columns before a name's indentation
	if bar > 0 {
		cols += bar + len(viewGutter)
	}
	if timeW > 0 {
		cols += timeW + len(viewGutter)
	}
	bw := bufio.NewWriterSize(w, bufferBytes)
	var b []byte
	for i := range rows {
		r := &rows[i]
		b = appendPadded(b[:0], []byte(sizes[i]), sizeW)
		b = appendPadded(append(b, viewGutter...), []byte(shares[i]), shareW)
		b = append(b, viewGutter...)
		if bar > 0 {
			b = appendBar(b, r.Bytes, total, bar, l.Color)
			b = append(b, viewGutter...)
		}
		if timeW > 0 {
			n := len(b)
			switch {
			case r.HasNewest:
				b = appendLocal(b, r.Newest, loc)
				if i == 0 {
					b = append(append(b, ' '), zone...)
				}
			case !r.Fold:
				b = append(b, missingCell...)
			}
			for len(b)-n < timeW {
				b = append(b, ' ')
			}
			b = append(b, viewGutter...)
		}
		for range 2 * r.Level {
			b = append(b, ' ')
		}
		name := clip(names[i], width-cols-2*r.Level)
		if r.Fold && l.Color {
			b = append(append(append(b, "\x1b[2m"...), name...), "\x1b[0m"...)
		} else {
			b = append(b, name...)
		}
		if _, err := bw.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	return bw.Flush()
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
