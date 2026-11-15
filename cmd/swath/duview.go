// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"io"
	"os"

	"golang.org/x/term"

	"github.com/varveio/swath/internal/textout"
	"github.com/varveio/swath/internal/totals"
)

// viewOut is how swath du draws its adaptive view: the total's label
// (SOURCE as given, or --from-dataset's SOURCE or DIR) and color.
type viewOut struct {
	label string
	color bool
}

// duView is swath du's adaptive view when it is shown: the table on a
// terminal (--format auto) that neither --depth nor --sort asked for (the
// view always goes by size); nil otherwise. An explicit --format, -d N and
// --sort keep the table, a pipe its rows.
func duView(o *options, format, label string, stdoutTTY bool) *viewOut {
	if format != textout.Table || o.depthSet || o.sortSet || o.format != "" && o.format != "auto" {
		return nil
	}
	return &viewOut{label: escapeControl(label), color: colorEnabled(o.term.color, os.LookupEnv, stdoutTTY)}
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

// The view's lines: the terminal's height less viewMargin (the closing
// line, the statistics' first lines and the prompt stay on screen), at
// least viewMinLines; viewLines when the height is unknown.
const (
	viewMargin   = 5
	viewMinLines = 8
	viewLines    = 20
)

// writeView draws v as the terminal allows, then on stderr (unless -q or
// --log-format json) the total once with both figures where the source
// has them, and the levels left out if the view had to drop some.
func (t *totalsOut) writeView(w io.Writer, v *totals.View) error {
	width, height := stdoutSize()
	lines := viewLines
	if height > 0 {
		lines = max(viewMinLines, height-viewMargin)
	}
	rows := v.Select(lines - 1) // under the heading
	for i := range rows {
		rows[i].Name = escapeControl(rows[i].Name) // a name is the listing's: it may hold controls
	}
	if err := textout.WriteView(w, textout.ViewLayout{Label: t.view.label, Local: t.tally.Dirs(), Width: width, Color: t.view.color}, rows); err != nil {
		return err
	}
	lg := elog()
	if lg.quiet > 0 || lg.json {
		return nil
	}
	total := t.tally.Result()[0] // the view's tally sums the total alone
	line := figuresLine(&total, v.Primary)
	if !t.tally.Allocated() {
		line = textout.TableFigure(&total, v.Primary) + " in " + count(total.Objects) + " " + plural(total.Objects, "object", "objects")
	}
	noteKept(errOut, "  "+line+"\n")
	if v.Lowered {
		noteKept(errOut, "  levels below depth "+count(int64(v.MaxDepth))+" are left out: more prefixes than --max-prefixes "+count(int64(t.tally.Options().MaxPrefixes))+"\n")
	}
	return nil
}
