// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"bufio"
	"cmp"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Layout is how Render draws the view.
type Layout struct {
	// Label is the total's path, on the first row, written as it is ("":
	// the first row's Name).
	Label string
	// Width is the terminal's columns (0: unknown, 80 assumed).
	Width int
	// Color styles the heading, the bar and the fold rows with ANSI escapes.
	Color bool
	// Loc is the dates' zone (nil: time.Local), named once, on the total's
	// row.
	Loc *time.Location
	// SizeHeading and CountHeading head the size and count columns ("":
	// "size" and "objects"; swath du writes "on disk" for the space on
	// disk and "files" for a local tree).
	SizeHeading, CountHeading string
}

// Align is how a Column's cells sit in its width.
type Align int

const (
	Left  Align = iota // padded on the right
	Right              // padded on the left, as the size and count columns are
)

// Column is a column of the caller's: its heading, its alignment and one
// cell per row Render takes, a fold's included, already formatted; an
// empty cell is blank. Render measures headings and cells in terminal
// columns (Cells) and writes them as they are: the caller escapes them for
// the terminal.
type Column struct {
	Heading string
	Align   Align
	Cells   []string
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
	gutter     = "  "
	dateLayout = "2006-01-02"
)

// The built-in headings, a missing date, and the mark of a size that
// leaves out objects without a value: a lower bound.
const (
	sizeHeading  = "size"
	countHeading = "objects"
	shareHeading = "share"
	dateHeading  = "newest"
	pathHeading  = "path"
	missingCell  = "-"
	partialMark  = ">= "
)

// The ANSI styles: the heading and the folds dimmed, the bar in cyan.
const (
	dim   = "\x1b[2m"
	cyan  = "\x1b[36m"
	reset = "\x1b[0m"
)

var eighths = [...]string{"", "▏", "▎", "▍", "▌", "▋", "▊", "▉"}

// Render writes rows (Select's) to w under a heading: per row its size,
// its count of objects, its share of the total, a bar of that share, the
// date of the newest modification below it, the caller's columns in the
// order given, and its path, indented two blanks a level under its
// parent's, the total first under the label. A fold row reads "(N more)":
// the rest of its parent's children, their sums and their newest date. A
// "!" after the size marks a row whose sums leave out a directory that
// could not be read, at or below it. The sizes are Size's, after ">= "
// for a row with Missing objects; the counts are Count's; the dates are
// local, the zone named on the total's row.
//
// Rows fit the width. As it shrinks, columns go in this order: the bar
// narrows and goes, then the share, then the date, then the caller's
// columns from the last to the first; the size, the count and the path
// stay, the path cut in the middle (Clip) where the row would pass the
// width. Only a width too narrow for the size, the count and the
// indented path's ellipsis is passed. The names are written as given: the caller escapes them for the
// terminal.
//
// Render writes nothing for no rows, and refuses a column whose cells are
// not one per row before writing anything.
func Render(w io.Writer, l Layout, rows []Row, extra ...Column) error {
	for _, c := range extra {
		if len(c.Cells) != len(rows) {
			return fmt.Errorf("tree: column %q has %d cells for %d rows", c.Heading, len(c.Cells), len(rows))
		}
	}
	if len(rows) == 0 {
		return nil
	}
	total := rows[0].Bytes
	loc := l.Loc
	if loc == nil {
		loc = time.Local
	}
	zone, _ := time.Now().In(loc).Zone()
	sizeHead, countHead := cmp.Or(l.SizeHeading, sizeHeading), cmp.Or(l.CountHeading, countHeading)
	label := cmp.Or(l.Label, rows[0].Name)
	names := make([]string, len(rows))
	sizes := make([]string, len(rows))
	counts := make([]string, len(rows))
	shares := make([]string, len(rows))
	dates := make([]string, len(rows))
	sizeW, countW, shareW, dateW, nameW := Cells(sizeHead), Cells(countHead), len(shareHeading), len(dateHeading), 0
	extraW := make([]int, len(extra))
	for j, c := range extra {
		extraW[j] = Cells(c.Heading)
		for _, s := range c.Cells {
			extraW[j] = max(extraW[j], Cells(s))
		}
	}
	timed, marked := false, false
	for i := range rows {
		r := &rows[i]
		switch {
		case i == 0:
			names[i] = label
		case r.Fold && r.More < 0:
			names[i] = "(more)"
		case r.Fold:
			names[i] = "(" + Grouped(r.More) + " more)"
		default:
			names[i] = r.Name
		}
		b := []byte(nil)
		if r.Missing > 0 {
			b = append(b, partialMark...)
		}
		sizes[i] = string(AppendSize(b, r.Bytes))
		counts[i] = Count(r.Objects)
		shares[i] = share(r.Bytes, total)
		switch {
		case r.Newest != nil:
			dates[i] = r.Newest.In(loc).Format(dateLayout)
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
		if i > 0 { // the total's label may run past the room: it is alone on its row
			nameW = max(nameW, 2*r.Level+Cells(names[i]))
		}
	}
	width := l.Width
	if width <= 0 {
		width = 80
	}
	// The columns before the path, its gutter included; the bar's apart.
	shown := struct {
		share, date bool
		extra       int // the caller's first columns shown
	}{true, timed, len(extra)}
	before := func() int {
		n := sizeW + len(gutter) + countW + len(gutter)
		if marked {
			n += 2
		}
		if shown.share {
			n += shareW + len(gutter)
		}
		if shown.date {
			n += dateW + len(gutter)
		}
		for _, w := range extraW[:shown.extra] {
			n += w + len(gutter)
		}
		return n
	}
	need := min(nameW, pathRoom) // the room the paths keep
	bar := 0
	switch room := width - before(); {
	case width >= wideView && room-wideBar-len(gutter) >= need:
		bar = wideBar
	case width >= narrowView && min(narrowBar, room-len(gutter)-need) >= minBar:
		bar = min(narrowBar, room-len(gutter)-need)
	case room < need:
		shown.share = false
		if width-before() < need {
			shown.date = false
		}
		for shown.extra > 0 && width-before() < need {
			shown.extra--
		}
	}
	cols := before()
	if bar > 0 {
		cols += bar + len(gutter)
	}
	bw := bufio.NewWriterSize(w, 256<<10)
	var b []byte
	// line writes a row of i (-1: the heading); faint: the heading or a
	// fold, dimmed in color.
	line := func(i int, size string, mark byte, count, shr string, bar func([]byte) []byte, date, name string, faint bool) error {
		b = b[:0]
		if faint && l.Color {
			b = append(b, dim...)
		}
		b = pad(b, size, sizeW, Right)
		if marked {
			b = append(b, ' ', mark)
		}
		b = pad(append(b, gutter...), count, countW, Right)
		if shown.share {
			b = pad(append(b, gutter...), shr, shareW, Right)
		}
		b = append(b, gutter...)
		if bar != nil {
			b = append(bar(b), gutter...)
			if faint && l.Color {
				b = append(b, dim...) // the bar's color ends with a reset
			}
		}
		if shown.date {
			b = append(pad(b, date, dateW, Left), gutter...)
		}
		for j, c := range extra[:shown.extra] {
			cell := c.Heading
			if i >= 0 {
				cell = c.Cells[i]
			}
			b = append(pad(b, cell, extraW[j], c.Align), gutter...)
		}
		b = append(b, name...)
		if faint && l.Color {
			b = append(b, reset...)
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
	if err := line(-1, sizeHead, ' ', countHead, shareHeading, head, dateHeading, pathHeading, true); err != nil {
		return err
	}
	for i := range rows {
		r := &rows[i]
		var drawn func([]byte) []byte
		if bar > 0 {
			drawn = func(b []byte) []byte { return appendBar(b, r.Bytes, total, bar, l.Color) }
		}
		indent := strings.Repeat(" ", 2*r.Level)
		name := indent + Clip(names[i], width-cols-2*r.Level)
		mark := byte(' ')
		if r.Unreadable {
			mark = '!'
		}
		if err := line(i, sizes[i], mark, counts[i], shares[i], drawn, dates[i], name, r.Fold); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// pad appends s in width terminal columns, aligned as a.
func pad(b []byte, s string, width int, a Align) []byte {
	n := width - Cells(s)
	if a == Left {
		b = append(b, s...)
	}
	for range n {
		b = append(b, ' ')
	}
	if a == Right {
		b = append(b, s...)
	}
	return b
}

// share is n's share of total as a percent with one decimal ("100%" for
// the whole and only for it, "-" of nothing).
func share(n, total int64) string {
	switch {
	case total <= 0:
		return missingCell
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
		b = append(b, cyan...)
	}
	for range cells / 8 {
		b = append(b, "█"...)
	}
	b = append(b, eighths[cells%8]...)
	if color && cells > 0 {
		b = append(b, reset...)
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
