// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"io"
	"os"
	"unicode/utf8"

	"golang.org/x/term"

	"github.com/varveio/swath/internal/textout"
	"github.com/varveio/swath/internal/totals"
)

// viewOut is how swath du draws its adaptive view: the total's label
// (SOURCE as given, or --from-dataset's SOURCE or DIR) and color.
type viewOut struct {
	label string
	color bool
	// statsOff: --no-stats, so no closing line follows the view.
	statsOff bool
	// drawn is set once the view is written, with the directories it
	// marks as unreadable (reportUnreadable then says so in one line).
	drawn      bool
	unreadable int
}

// duView is swath du's adaptive view when it is shown: the table on a
// terminal (--format auto) that neither --depth nor --sort asked for (the
// view always goes by size); nil otherwise. An explicit --format, -d N and
// --sort keep the table, a pipe its rows.
func duView(o *options, format, label string, stdoutTTY bool) *viewOut {
	if format != textout.Table || o.depthSet || o.sortSet || o.format != "" && o.format != "auto" {
		return nil
	}
	return &viewOut{label: escapeControl(label), color: colorEnabled(o.term.color, os.LookupEnv, stdoutTTY), statsOff: o.term.stats.set && !o.term.stats.on}
}

// stdoutSize is the terminal's columns and lines on stdout, 0 when
// unknown.
var stdoutSize = func() (width, height int) {
	w, h, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 0, 0
	}
	return w, h
}

// The view's lines when the terminal's height is unknown, and at least.
const (
	viewLines    = 20
	viewMinLines = 8
)

// termRows is the terminal rows text takes at width, wrapped.
func termRows(text string, width int) int {
	if width <= 0 {
		width = 80
	}
	return max(1, (utf8.RuneCountInString(text)+width-1)/width)
}

// closingRows is the terminal rows of what is printed after the view, so
// that the view leaves room for it and its top does not scroll off: the
// figures line, the levels left out, the unreadable line, the closing line
// (the statistics' result line, estimated), the hint after a large
// listing, the error line under -q, and the shell's prompt.
func (t *totalsOut) closingRows(v *totals.View, figures string, width int) int {
	lg := elog()
	rows := 1 // the prompt
	unreadable := len(v.Unreadable) > 0
	switch {
	case lg.json:
		return rows + 3 // the outcome and warning events
	case lg.quiet > 0:
		if unreadable {
			rows += termRows("swath: "+unreadableMessage(int64(len(v.Unreadable))), width)
		}
		return rows
	}
	rows += termRows("  "+figures, width)
	if v.Lowered {
		rows += termRows(loweredLine(v, t.tally), width)
	}
	if unreadable {
		if lg.shows(lvInfo) {
			rows += 3 + min(len(v.Unreadable), unreadableShown) // the heading, the paths, the rest, the outcome
		} else {
			rows += termRows(unreadableLine(len(v.Unreadable), len(v.Unreadable)), width)
		}
	}
	if !t.view.statsOff {
		// The closing line: its verdict, the counts, the bytes, the time
		// and the rate; at most about this long.
		rows += termRows("  partial: "+count(int64(len(v.Unreadable)))+" unreadable entries; 999,999,999 files, 999,999,999 directories, 999 GiB in 59m59s · 999,999 entries/s", width)
	}
	if total := v.Total.Objects; !unreadable && total >= hintRows {
		rows += termRows("hint: this run listed ~999.9M objects; to explore them again without relisting, save an inventory once and total that:", width) +
			2*termRows("hint:   swath du --from-dataset DIR -d 9 --sort size "+t.view.label, width)
	}
	return rows
}

// loweredLine says the levels the view left out.
func loweredLine(v *totals.View, t *totals.Tally) string {
	return "  levels below depth " + count(int64(v.MaxDepth)) + " are left out: more prefixes than --max-prefixes " + count(int64(t.Options().MaxPrefixes))
}

// writeView draws v as the terminal allows, leaving room for what follows
// it (closingRows), then on stderr (unless -q or --log-format json) the
// total once with both figures where the source has them, and the levels
// left out if the view had to drop some.
func (t *totalsOut) writeView(w io.Writer, v *totals.View) error {
	width, height := stdoutSize()
	total := t.tally.Result()[0] // the view's tally sums the total alone
	figures := figuresLine(&total, v.Primary)
	if !t.tally.Allocated() {
		figures = textout.TableFigure(&total, v.Primary) + " in " + count(total.Objects) + " " + plural(total.Objects, "object", "objects")
	}
	lines := viewLines
	if height > 0 {
		lines = max(viewMinLines, height-t.closingRows(v, figures, width))
	}
	rows := v.Select(lines - 1) // under the heading
	for i := range rows {
		rows[i].Name = escapeControl(rows[i].Name) // a name is the listing's: it may hold controls
	}
	if err := textout.WriteView(w, textout.ViewLayout{Label: t.view.label, Local: t.tally.Dirs(), Width: width, Color: t.view.color}, rows); err != nil {
		return err
	}
	t.view.drawn, t.view.unreadable = true, len(v.Unreadable)
	lg := elog()
	if lg.quiet > 0 || lg.json {
		return nil
	}
	noteKept(errOut, "  "+figures+"\n")
	if v.Lowered {
		noteKept(errOut, loweredLine(v, t.tally)+"\n")
	}
	return nil
}

// unreadableLine is the one line that says, under the view, that entries
// could not be read: n of them, dirs of them directories the view marks.
func unreadableLine(n, dirs int) string {
	what := plural(n, "1 entry", count(int64(n))+" entries")
	if dirs == n {
		what = plural(n, "1 directory", count(int64(n))+" directories")
	}
	return "! " + what + " could not be read (marked ! above); -v lists them"
}
